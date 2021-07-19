package net.corda.node.services.statemachine

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.concurrent.CordaFuture
import net.corda.core.flows.Destination
import net.corda.core.flows.FlowInfo
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.FlowSession
import net.corda.core.flows.InitiatedBy
import net.corda.core.flows.InitiatingFlow
import net.corda.core.flows.KilledFlowException
import net.corda.core.flows.UnexpectedFlowEndException
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.concurrent.flatMap
import net.corda.core.messaging.MessageRecipients
import net.corda.core.utilities.UntrustworthyData
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.seconds
import net.corda.core.utilities.unwrap
import net.corda.node.services.FinalityHandler
import net.corda.node.services.messaging.Message
import net.corda.node.services.persistence.DBTransactionStorage
import net.corda.node.services.statemachine.hospital.external.DatabaseEndocrinologist
import net.corda.nodeapi.internal.persistence.contextTransaction
import net.corda.testing.core.TestIdentity
import net.corda.testing.internal.IS_OPENJ9
import net.corda.testing.node.internal.InternalMockNetwork
import net.corda.testing.node.internal.MessagingServiceSpy
import net.corda.testing.node.internal.TestStartedNode
import net.corda.testing.node.internal.enclosedCordapp
import net.corda.testing.node.internal.newContext
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.h2.util.Utils
import org.junit.After
import org.junit.Assert.assertTrue
import org.junit.Assume
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import java.sql.SQLException
import java.time.Duration
import java.time.Instant
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class RetryFlowMockTest {
    private lateinit var mockNet: InternalMockNetwork
    private lateinit var nodeA: TestStartedNode
    private lateinit var nodeB: TestStartedNode

    @Before
    fun start() {
        mockNet = InternalMockNetwork(threadPerNode = true, cordappsForAllNodes = listOf(enclosedCordapp()))
        nodeA = mockNet.createNode()
        nodeB = mockNet.createNode()
        mockNet.startNodes()
        RetryFlow.count = 0
        SendAndRetryFlow.count = 0
        RetryInsertFlow.count = 0
        DatabaseEndocrinologist.customConditions.add { t -> t is LimitedRetryCausingError }
        DatabaseEndocrinologist.customConditions.add { t -> t is RetryCausingError }
    }

    private fun <T> TestStartedNode.startFlow(logic: FlowLogic<T>): CordaFuture<T> {
        return this.services.startFlow(logic, this.services.newContext()).flatMap { it.resultFuture }
    }

    @After
    fun cleanUp() {
        mockNet.stopNodes()
        DatabaseEndocrinologist.customConditions.clear()
    }

    @Test(timeout=300_000)
	fun `Single retry`() {
        assertEquals(Unit, nodeA.startFlow(RetryFlow(1)).get())
        assertEquals(2, RetryFlow.count)
    }

    @Test(timeout=300_000)
	fun `Retry does not set senderUUID`() {
        val messagesSent = Collections.synchronizedList(mutableListOf<Message>())
        val partyB = nodeB.info.legalIdentities.first()
        nodeA.setMessagingServiceSpy(object : MessagingServiceSpy() {
            override fun send(message: Message, target: MessageRecipients, sequenceKey: Any) {
                messagesSent.add(message)
                messagingService.send(message, target)
            }
        })
        nodeA.startFlow(SendAndRetryFlow(1, partyB)).get()
        assertNotNull(messagesSent.first().senderUUID)
        assertNull(messagesSent.last().senderUUID)
        assertEquals(2, SendAndRetryFlow.count)
    }

    @Ignore("CORDA-4045: Disable flaky test")
    @Test(timeout=300_000)
	fun `Restart does not set senderUUID`() {
        val messagesSent = Collections.synchronizedList(mutableListOf<Message>())
        val partyB = nodeB.info.legalIdentities.first()
        val expectedMessagesSent = CountDownLatch(3)
        nodeA.setMessagingServiceSpy(object : MessagingServiceSpy() {
            override fun send(message: Message, target: MessageRecipients, sequenceKey: Any) {
                messagesSent.add(message)
                expectedMessagesSent.countDown()
                messagingService.send(message, target)
            }
        })
        nodeA.startFlow(KeepSendingFlow(partyB))
        KeepSendingFlow.lock.acquire()
        assertTrue(messagesSent.isNotEmpty())
        assertNotNull(messagesSent.first().senderUUID)
        nodeA = mockNet.restartNode(nodeA)
        nodeA.setMessagingServiceSpy(object : MessagingServiceSpy() {
            override fun send(message: Message, target: MessageRecipients, sequenceKey: Any) {
                messagesSent.add(message)
                expectedMessagesSent.countDown()
                messagingService.send(message, target)
            }
        })
        ReceiveFlow3.lock.release()
        assertTrue(expectedMessagesSent.await(20, TimeUnit.SECONDS))
        assertEquals(3, messagesSent.size)
        // CORDA-4045: We can't be sure that the last message sent will be the last we record, so
        // instead check we have exactly one message (the first) with sender UUID
        assertNotNull(messagesSent.singleOrNull { it.senderUUID != null })
    }

    @Test(timeout=300_000)
    fun `Early end session message does not hang receiving flow`() {
        Assume.assumeTrue(!IS_OPENJ9)
        val partyB = nodeB.info.legalIdentities.first()
        assertThatExceptionOfType(UnexpectedFlowEndException::class.java).isThrownBy {
            nodeA.startFlow(UnbalancedSendAndReceiveFlow(partyB)).getOrThrow(60.seconds)
        }
    }

    @Test(timeout=300_000)
	fun `Retry duplicate insert`() {
        assertEquals(Unit, nodeA.startFlow(RetryInsertFlow(1)).get())
        assertEquals(2, RetryInsertFlow.count)
    }

    @Test(timeout=300_000)
	fun `Patient records do not leak in hospital`() {
        assertEquals(Unit, nodeA.startFlow(RetryFlow(1)).get())
        // Need to make sure the state machine has finished.  Otherwise this test is flakey.
        mockNet.waitQuiescent()
        assertThat(nodeA.smm.flowHospital.track().snapshot).isEmpty()
        assertEquals(2, RetryFlow.count)
    }

    @Test(timeout=300_000)
	fun `Patient records do not leak in hospital when using killFlow`() {
        // Make sure we have seen an update from the hospital, and thus the flow went there.
        val alice = TestIdentity(CordaX500Name.parse("L=London,O=Alice Ltd,OU=Trade,C=GB")).party
        val records = nodeA.smm.flowHospital.track().updates.toBlocking().toIterable().iterator()
        val flow = nodeA.services.startFlow(FinalityHandler(object : FlowSession() {
            override val destination: Destination get() = alice
            override val counterparty: Party get() = alice

            override fun getCounterpartyFlowInfo(maySkipCheckpoint: Boolean): FlowInfo {
                TODO("not implemented")
            }

            override fun getCounterpartyFlowInfo(): FlowInfo {
                TODO("not implemented")
            }

            override fun <R : Any> sendAndReceive(receiveType: Class<R>, payload: Any, maySkipCheckpoint: Boolean): UntrustworthyData<R> {
                TODO("not implemented")
            }

            override fun <R : Any> sendAndReceive(receiveType: Class<R>, payload: Any): UntrustworthyData<R> {
                TODO("not implemented")
            }

            override fun <R : Any> receive(receiveType: Class<R>, maySkipCheckpoint: Boolean): UntrustworthyData<R> {
                TODO("not implemented")
            }

            override fun <R : Any> receive(receiveType: Class<R>): UntrustworthyData<R> {
                TODO("not implemented")
            }

            override fun send(payload: Any, maySkipCheckpoint: Boolean) {
                TODO("not implemented")
            }

            override fun send(payload: Any) {
                TODO("not implemented")
            }

            override fun close() {
                TODO("Not yet implemented")
            }

        }), nodeA.services.newContext()).get()
        records.next()
        // Killing it should remove it.
        nodeA.smm.killFlow(flow.id)
        assertFailsWith<KilledFlowException> {
            flow.resultFuture.getOrThrow(20.seconds)
        }
        // Sleep added because the flow leaves the hospital after the future has returned
        // This means that the removal code has not run by the time the snapshot is taken
        Thread.sleep(2000)
        assertThat(nodeA.smm.flowHospital.track().snapshot).isEmpty()
    }

    class LimitedRetryCausingError : IllegalStateException("I am going to live forever")

    class RetryCausingError : SQLException("deadlock")

    class RetryFlow(private val i: Int) : FlowLogic<Unit>() {
        companion object {
            @Volatile
            var count = 0
        }

        @Suspendable
        override fun call() {
            logger.info("Hello $count")
            if (count++ < i) {
                if (i == Int.MAX_VALUE) {
                    throw LimitedRetryCausingError()
                } else {
                    throw RetryCausingError()
                }
            }
        }
    }

    @InitiatingFlow
    class SendAndRetryFlow(private val i: Int, private val other: Party) : FlowLogic<Unit>() {
        companion object {
            @Volatile
            var count = 0
        }

        @Suspendable
        override fun call() {
            logger.info("Sending...")
            val session = initiateFlow(other)
            session.send("Boo")
            if (count++ < i) {
                throw RetryCausingError()
            }
        }
    }

    @Suppress("unused")
    @InitiatedBy(SendAndRetryFlow::class)
    class ReceiveFlow2(private val other: FlowSession) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            val received = other.receive<String>().unwrap { it }
            logger.info("Received... $received")
        }
    }

    @InitiatingFlow
    class KeepSendingFlow(private val other: Party) : FlowLogic<Unit>() {

        companion object {
            val lock = Semaphore(0)
        }

        @Suspendable
        override fun call() {
            val session = initiateFlow(other)
            session.send("boo")
            lock.release()
            session.receive<String>()
            session.send("boo")
        }
    }

    @Suppress("unused")
    @InitiatedBy(KeepSendingFlow::class)
    class ReceiveFlow3(private val other: FlowSession) : FlowLogic<Unit>() {

        companion object {
            val lock = Semaphore(0)
        }

        @Suspendable
        override fun call() {
            other.receive<String>()
            lock.acquire()
            other.send("hoo")
            other.receive<String>()
        }
    }

    class RetryInsertFlow(private val i: Int) : FlowLogic<Unit>() {
        companion object {
            @Volatile
            var count = 0
        }

        @Suspendable
        override fun call() {
            logger.info("Hello")
            doInsert()
            // Checkpoint so we roll back to here
            FlowLogic.sleep(Duration.ofSeconds(0))
            if (count++ < i) {
                doInsert()
            }
        }

        private fun doInsert() {
            val tx = DBTransactionStorage.DBTransaction("Foo", null, Utils.EMPTY_BYTES,
                    DBTransactionStorage.TransactionStatus.VERIFIED, Instant.now())
            contextTransaction.session.save(tx)
        }
    }

    @InitiatingFlow
    class UnbalancedSendAndReceiveFlow(private val other: Party) : FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            val session = initiateFlow(other)
            session.send("boo")
            session.receive<String>()
            session.receive<String>()
        }
    }

    @Suppress("unused")
    @InitiatedBy(UnbalancedSendAndReceiveFlow::class)
    class UnbalancedSendAndReceiveResponder(private val other: FlowSession) : FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            other.receive<String>()
            other.send("hoo")
        }
    }
}
