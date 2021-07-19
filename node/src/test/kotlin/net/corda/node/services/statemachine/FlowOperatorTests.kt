package net.corda.node.services.statemachine

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.FlowExternalAsyncOperation
import net.corda.core.flows.FlowExternalOperation
import net.corda.core.flows.FlowInfo
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.FlowSession
import net.corda.core.flows.InitiatingFlow
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.messaging.MessageRecipients
import net.corda.core.serialization.deserialize
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.seconds
import net.corda.node.services.messaging.Message
import net.corda.testing.core.ALICE_NAME
import net.corda.testing.core.BOB_NAME
import net.corda.testing.core.CHARLIE_NAME
import net.corda.testing.core.DAVE_NAME
import net.corda.testing.core.executeTest
import net.corda.testing.flows.registerCordappFlowFactory
import net.corda.testing.node.internal.InternalMockNetwork
import net.corda.testing.node.internal.InternalMockNodeParameters
import net.corda.testing.node.internal.MessagingServiceSpy
import net.corda.testing.node.internal.TestStartedNode
import net.corda.testing.node.internal.enclosedCordapp
import net.corda.testing.node.internal.startFlow
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.util.concurrent.CompletableFuture
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class FlowOperatorTests {

    companion object {
        val log = contextLogger()
        val EUGENE_NAME = CordaX500Name("Eugene", "EugeneCorp", "GB")
    }

    lateinit var mockNet: InternalMockNetwork
    lateinit var aliceNode: TestStartedNode
    private lateinit var aliceParty: Party
    lateinit var bobNode: TestStartedNode
    private lateinit var bobParty: Party
    lateinit var charlieNode: TestStartedNode
    private lateinit var charlieParty: Party
    lateinit var daveNode: TestStartedNode
    lateinit var daveParty: Party
    private lateinit var eugeneNode: TestStartedNode
    private lateinit var eugeneParty: Party
    private lateinit var notaryParty: Party

    @Before
    fun setup() {
        mockNet = InternalMockNetwork(
                threadPerNode = true,
                cordappsForAllNodes = listOf(enclosedCordapp())
        )
        aliceNode = mockNet.createNode(InternalMockNodeParameters(
                legalName = ALICE_NAME
        ))
        bobNode = mockNet.createNode(InternalMockNodeParameters(
                legalName = BOB_NAME
        ))
        charlieNode = mockNet.createNode(InternalMockNodeParameters(
                legalName = CHARLIE_NAME
        ))
        daveNode = mockNet.createNode(InternalMockNodeParameters(
                legalName = DAVE_NAME
        ))
        eugeneNode = mockNet.createNode(InternalMockNodeParameters(
                legalName = EUGENE_NAME
        ))
        mockNet.startNodes()
        aliceParty = aliceNode.info.legalIdentities.first()
        bobParty = bobNode.info.legalIdentities.first()
        charlieParty = charlieNode.info.legalIdentities.first()
        daveParty = daveNode.info.legalIdentities.first()
        eugeneParty = eugeneNode.info.legalIdentities.first()
        notaryParty = mockNet.defaultNotaryIdentity

        // put nodes offline, alice and charlie are staying online
        bobNode.dispose()
        daveNode.dispose()
        eugeneNode.dispose()
    }

    @After
    fun cleanUp() {
        mockNet.stopNodes()
    }

    @Test(timeout = 300_000)
    fun `query should return all flows which are waiting for counter party to process`() {
        charlieNode.registerCordappFlowFactory(ReceiveFlow::class) { AcceptingFlow("Hello", it) }

        val bobStart = aliceNode.services.startFlow(ReceiveFlow("Hello", listOf(bobParty)))
        val daveStart = aliceNode.services.startFlow(ReceiveFlow("Hello", listOf(daveParty)))
        charlieNode.services.startFlow(ReceiveFlow("Hello", listOf(charlieParty)))

        val cut = FlowOperator(aliceNode.smm, aliceNode.services.clock)

        executeTest(5.seconds) {
            val result = cut.queryWaitingFlows(
                    WaitingFlowQuery(counterParties = mutableListOf(ALICE_NAME, BOB_NAME, CHARLIE_NAME, DAVE_NAME, EUGENE_NAME)
                    ))

            assertEquals(2, result.size)

            val bob = result.first { it.waitingForParties.first().name == BOB_NAME }
            assertNull(bob.externalOperationImplName)
            assertEquals(WaitingSource.RECEIVE, bob.source)
            assertEquals(1, bob.waitingForParties.size)
            assertEquals(bobStart.id, bob.id)

            val dave = result.first { it.waitingForParties.first().name == DAVE_NAME }
            assertNull(dave.externalOperationImplName)
            assertEquals(WaitingSource.RECEIVE, dave.source)
            assertEquals(daveStart.id, dave.id)
            assertEquals(1, dave.waitingForParties.size)
        }
    }

    @Test(timeout = 300_000)
    fun `query should return only requested party flows which are waiting for counter party to process`() {
        aliceNode.services.startFlow(ReceiveFlow("Hello", listOf(bobParty)))
        val daveStart = aliceNode.services.startFlow(ReceiveFlow("Hello", listOf(daveParty)))

        val cut = FlowOperator(aliceNode.smm, aliceNode.services.clock)

        executeTest(5.seconds) {
            val result = cut.queryWaitingFlows(
                    WaitingFlowQuery(counterParties = mutableListOf(DAVE_NAME)
                    ))

            assertEquals(1, result.size)
            assertEquals(daveStart.id, result.first().id)
            assertNull(result.first().externalOperationImplName)
            assertEquals(WaitingSource.RECEIVE, result.first().source)
            assertEquals(1, result.first().waitingForParties.size)
            assertEquals(DAVE_NAME, result.first().waitingForParties.first().name)
        }
    }

    @Test(timeout = 300_000)
    fun `query should return all parties in a single flow which are waiting for counter party to process`() {
        val start = aliceNode.services.startFlow(ReceiveFlow("Hello", listOf(bobParty, daveParty)))

        val cut = FlowOperator(aliceNode.smm, aliceNode.services.clock)

        executeTest(5.seconds) {
            val result = cut.queryWaitingFlows(
                    WaitingFlowQuery(counterParties = mutableListOf(ALICE_NAME, BOB_NAME, CHARLIE_NAME, DAVE_NAME, EUGENE_NAME)
                    ))

            assertEquals(1, result.size)
            assertEquals(start.id, result.first().id)
            assertNull(result.first().externalOperationImplName)
            assertEquals(WaitingSource.RECEIVE, result.first().source)
            assertEquals(2, result.first().waitingForParties.size)
            assertTrue(result.first().waitingForParties.any { it.name == BOB_NAME })
            assertTrue(result.first().waitingForParties.any { it.name == DAVE_NAME })
        }
    }

    @Test(timeout = 300_000)
    fun `query should return only flows which are waiting for counter party to process and not in the hospital`() {
        charlieNode.registerCordappFlowFactory(ReceiveFlow::class) { AcceptingFlow("Fail", it) }

        aliceNode.services.startFlow(ReceiveFlow("Hello", listOf(charlieParty)))
        val daveStart = aliceNode.services.startFlow(ReceiveFlow("Hello", listOf(daveParty)))

        val cut = FlowOperator(aliceNode.smm, aliceNode.services.clock)

        executeTest(5.seconds) {
            val result = cut.queryWaitingFlows(
                    WaitingFlowQuery(counterParties = mutableListOf(BOB_NAME, DAVE_NAME)
                    ))

            assertEquals(1, result.size)
            assertEquals(daveStart.id, result.first().id)
            assertNull(result.first().externalOperationImplName)
            assertEquals(WaitingSource.RECEIVE, result.first().source)
            assertEquals(1, result.first().waitingForParties.size)
            assertEquals(DAVE_NAME, result.first().waitingForParties.first().name)
        }
    }

    @Test(timeout = 300_000)
    fun `query should return only flows which are waiting more than 4 seconds for counter party to process`() {
        val bobStart = aliceNode.services.startFlow(ReceiveFlow("Hello", listOf(bobParty)))
        Thread.sleep(4500)
        aliceNode.services.startFlow(ReceiveFlow("Hello", listOf(daveParty)))

        val cut = FlowOperator(aliceNode.smm, aliceNode.services.clock)

        executeTest(5.seconds) {
            val result = cut.queryWaitingFlows(
                    WaitingFlowQuery(
                            counterParties = mutableListOf(ALICE_NAME, BOB_NAME, CHARLIE_NAME, DAVE_NAME, EUGENE_NAME),
                            onlyIfSuspendedLongerThan = 4.seconds
                    ))
            assertEquals(1, result.size)
            assertEquals(1, result.size)
            assertEquals(bobStart.id, result.first().id)
            assertNull(result.first().externalOperationImplName)
            assertEquals(WaitingSource.RECEIVE, result.first().source)
            assertEquals(1, result.first().waitingForParties.size)
            assertEquals(BOB_NAME, result.first().waitingForParties.first().name)
        }
    }

    @Test(timeout = 300_000)
    fun `mixed query should return all flows which are waiting for counter party to process`() {
        charlieNode.registerCordappFlowFactory(ReceiveFlow::class) { AcceptingFlow("Hello", it) }

        val future = CompletableFuture<String>()
        aliceNode.services.startFlow(ExternalAsyncOperationFlow(future))
        val bobStart = aliceNode.services.startFlow(ReceiveFlow("Hello", listOf(bobParty)))
        val daveStart = aliceNode.services.startFlow(GetFlowInfoFlow(listOf(daveParty)))
        charlieNode.services.startFlow(ReceiveFlow("Hello", listOf(charlieParty)))

        val cut = FlowOperator(aliceNode.smm, aliceNode.services.clock)

        executeTest(5.seconds) {
            val result = cut.queryWaitingFlows(
                    WaitingFlowQuery(
                            counterParties = mutableListOf(ALICE_NAME, BOB_NAME, CHARLIE_NAME, DAVE_NAME, EUGENE_NAME),
                            waitingSources = mutableListOf(WaitingSource.EXTERNAL_OPERATION, WaitingSource.RECEIVE, WaitingSource.GET_FLOW_INFO)
                    ))

            assertEquals(2, result.size)

            val receive = result.first { it.source == WaitingSource.RECEIVE }
            assertNull(receive.externalOperationImplName)
            assertEquals(1, receive.waitingForParties.size)
            assertEquals(bobStart.id, receive.id)
            assertEquals(BOB_NAME, receive.waitingForParties.first().name)

            val getFlowInfo = result.first { it.source == WaitingSource.GET_FLOW_INFO }
            assertNull(getFlowInfo.externalOperationImplName)
            assertEquals(1, getFlowInfo.waitingForParties.size)
            assertEquals(daveStart.id, getFlowInfo.id)
            assertEquals(DAVE_NAME, getFlowInfo.waitingForParties.first().name)
        }
    }

    @Test(timeout = 300_000)
    fun `query should return all flows which are waiting for counter party (the flow must have counter party) to process grouped by party`() {
        val future = CompletableFuture<String>()
        aliceNode.services.startFlow(ExternalAsyncOperationFlow(future))
        val bobStart = aliceNode.services.startFlow(ReceiveFlow("Hello", listOf(bobParty)))
        val daveStart = aliceNode.services.startFlow(ReceiveFlow("Hello", listOf(daveParty)))

        val cut = FlowOperator(aliceNode.smm, aliceNode.services.clock)

        executeTest(5.seconds) {
            val result = cut.queryFlowsCurrentlyWaitingForPartiesGrouped(WaitingFlowQuery(
                    waitingSources = mutableListOf(WaitingSource.EXTERNAL_OPERATION, WaitingSource.RECEIVE)
            ))

            assertEquals(2, result.size)
            assertEquals(1, result.getValue(bobParty).size)
            assertNull(result.getValue(bobParty).first().externalOperationImplName)
            assertEquals(bobStart.id, result.getValue(bobParty).first().id)
            assertEquals(WaitingSource.RECEIVE, result.getValue(bobParty).first().source)
            assertEquals(1, result.getValue(bobParty).first().waitingForParties.size)
            assertEquals(BOB_NAME, result.getValue(bobParty).first().waitingForParties.first().name)
            assertEquals(1, result.getValue(daveParty).size)
            assertEquals(daveStart.id, result.getValue(daveParty).first().id)
            assertNull(result.getValue(daveParty).first().externalOperationImplName)
            assertEquals(WaitingSource.RECEIVE, result.getValue(daveParty).first().source)
            assertEquals(1, result.getValue(daveParty).first().waitingForParties.size)
            assertEquals(DAVE_NAME, result.getValue(daveParty).first().waitingForParties.first().name)
        }
    }

    @Test(timeout = 300_000)
    fun `get should return all flow state machines which are waiting for other party to process`() {
        aliceNode.services.startFlow(ReceiveFlow("Hello", listOf(bobParty)))
        aliceNode.services.startFlow(ReceiveFlow("Hello", listOf(daveParty)))

        val cut = FlowOperator(aliceNode.smm, aliceNode.services.clock)

        executeTest(5.seconds) {
            val result = cut.getAllWaitingFlows().toList()

            assertEquals(2, result.size)
        }
    }

    @Test(timeout = 300_000)
    fun `query should return only requested by id flows which are waiting for counter party to process`() {
        charlieNode.registerCordappFlowFactory(ReceiveFlow::class) { AcceptingFlow("Fail", it) }

        val charlieStart = aliceNode.services.startFlow(ReceiveFlow("Hello", listOf(charlieParty)))
        aliceNode.services.startFlow(ReceiveFlow("Hello", listOf(daveParty)))
        val eugeneStart = aliceNode.services.startFlow(ReceiveFlow("Hello", listOf(eugeneParty)))

        val cut = FlowOperator(aliceNode.smm, aliceNode.services.clock)

        executeTest(5.seconds) {
            val result = cut.queryWaitingFlows(
                    WaitingFlowQuery(flowIds = mutableListOf(charlieStart.id, eugeneStart.id)
                    ))

            assertEquals(1, result.size)
            assertEquals(eugeneStart.id, result.first().id)
            assertNull(result.first().externalOperationImplName)
            assertEquals(WaitingSource.RECEIVE, result.first().source)
            assertEquals(1, result.first().waitingForParties.size)
            assertEquals(EUGENE_NAME, result.first().waitingForParties.first().name)
        }
    }

    @Test(timeout = 300_000)
    fun `query should return all flows which are waiting for getting info about counter party`() {
        val start = aliceNode.services.startFlow(GetFlowInfoFlow(listOf(eugeneParty)))

        val cut = FlowOperator(aliceNode.smm, aliceNode.services.clock)

        executeTest(5.seconds) {
            val result = cut.queryWaitingFlows(
                    WaitingFlowQuery(counterParties = mutableListOf(ALICE_NAME, BOB_NAME, CHARLIE_NAME, DAVE_NAME, EUGENE_NAME)
                    ))

            assertEquals(1, result.size)
            assertEquals(start.id, result.first().id)
            assertNull(result.first().externalOperationImplName)
            assertEquals(WaitingSource.GET_FLOW_INFO, result.first().source)
            assertEquals(1, result.first().waitingForParties.size)
            assertEquals(EUGENE_NAME, result.first().waitingForParties.first().name)
        }
    }

    @Test(timeout = 300_000)
    fun `query should return all flows which are waiting for sending and receiving from counter party when stuck in remote party`() {
        val start = aliceNode.services.startFlow(SendAndReceiveFlow("Hello", listOf(eugeneParty)))

        val cut = FlowOperator(aliceNode.smm, aliceNode.services.clock)

        executeTest(5.seconds) {
            val result = cut.queryWaitingFlows(
                    WaitingFlowQuery(counterParties = mutableListOf(ALICE_NAME, BOB_NAME, CHARLIE_NAME, DAVE_NAME, EUGENE_NAME)
                    ))

            assertEquals(1, result.size)
            assertEquals(start.id, result.first().id)
            assertNull(result.first().externalOperationImplName)
            assertEquals(WaitingSource.RECEIVE, result.first().source) // yep, it's receive
            assertEquals(1, result.first().waitingForParties.size)
            assertEquals(EUGENE_NAME, result.first().waitingForParties.first().name)
        }
    }

    @Test(timeout = 300_000)
    fun `query should return all flows which are waiting for sending and receiving from counter party when stuck in sending`() {
        val future = CompletableFuture<Unit>()
        aliceNode.setMessagingServiceSpy(BlockingMessageSpy("PauseSend", future))

        val start = aliceNode.services.startFlow(SendAndReceiveFlow("PauseSend", listOf(eugeneParty)))

        val cut = FlowOperator(aliceNode.smm, aliceNode.services.clock)

        executeTest(5.seconds, { future.complete(Unit) }) {
            val result = cut.queryWaitingFlows(
                    WaitingFlowQuery(counterParties = mutableListOf(ALICE_NAME, BOB_NAME, CHARLIE_NAME, DAVE_NAME, EUGENE_NAME)
                    ))

            assertEquals(1, result.size)
            assertEquals(start.id, result.first().id)
            assertNull(result.first().externalOperationImplName)
            assertEquals(WaitingSource.SEND_AND_RECEIVE, result.first().source)
            assertEquals(1, result.first().waitingForParties.size)
            assertEquals(EUGENE_NAME, result.first().waitingForParties.first().name)
        }
    }

    @Test(timeout = 300_000)
    fun `query should return all flows which are waiting for async external operations`() {
        val future = CompletableFuture<String>()
        val start = aliceNode.services.startFlow(ExternalAsyncOperationFlow(future))

        val cut = FlowOperator(aliceNode.smm, aliceNode.services.clock)

        executeTest(5.seconds, { future.complete("Hello") }) {
            val result = cut.queryWaitingFlows(WaitingFlowQuery(
                    waitingSources = mutableListOf(WaitingSource.EXTERNAL_OPERATION)
            )) // the list of counter parties must be empty to get any external operation

            assertEquals(1, result.size)
            assertEquals(start.id, result.first().id)
            assertEquals(ExternalAsyncOperationFlow.ExternalOperation::class.java.canonicalName, result.first().externalOperationImplName)
            assertEquals(WaitingSource.EXTERNAL_OPERATION, result.first().source)
            assertEquals(0, result.first().waitingForParties.size)
        }
    }

    @Test(timeout = 300_000)
    fun `query should return all flows which are waiting for external operations`() {
        val future = CompletableFuture<String>()
        val start = aliceNode.services.startFlow(ExternalOperationFlow(future))

        val cut = FlowOperator(aliceNode.smm, aliceNode.services.clock)

        executeTest(5.seconds, { future.complete("Hello") }) {
            val result = cut.queryWaitingFlows(WaitingFlowQuery())

            assertEquals(1, result.size)
            assertEquals(start.id, result.first().id)
            assertEquals(ExternalOperationFlow.ExternalOperation::class.java.canonicalName, result.first().externalOperationImplName)
            assertEquals(WaitingSource.EXTERNAL_OPERATION, result.first().source)
            assertEquals(0, result.first().waitingForParties.size)
        }
    }

    @Test(timeout = 300_000)
    fun `query should return all flows which are sleeping`() {
        val start = aliceNode.services.startFlow(SleepFlow())

        val cut = FlowOperator(aliceNode.smm, aliceNode.services.clock)

        executeTest(5.seconds) {
            val result = cut.queryWaitingFlows(WaitingFlowQuery())

            assertEquals(1, result.size)
            assertEquals(start.id, result.first().id)
            assertNull(result.first().externalOperationImplName)
            assertEquals(WaitingSource.SLEEP, result.first().source)
            assertEquals(0, result.first().waitingForParties.size)
        }
    }

    @Test(timeout = 300_000)
    fun `query should return all flows which are waiting for sending from counter party`() {
        val future = CompletableFuture<Unit>()
        aliceNode.setMessagingServiceSpy(BlockingMessageSpy("PauseSend", future))

        val start = aliceNode.services.startFlow(SendFlow("PauseSend", listOf(eugeneParty)))

        val cut = FlowOperator(aliceNode.smm, aliceNode.services.clock)

        executeTest(5.seconds, { future.complete(Unit) }) {
            val result = cut.queryWaitingFlows(
                    WaitingFlowQuery(counterParties = mutableListOf(ALICE_NAME, BOB_NAME, CHARLIE_NAME, DAVE_NAME, EUGENE_NAME)
                    ))

            assertEquals(1, result.size)
            assertEquals(start.id, result.first().id)
            assertNull(result.first().externalOperationImplName)
            assertEquals(WaitingSource.SEND, result.first().source)
            assertEquals(1, result.first().waitingForParties.size)
            assertEquals(EUGENE_NAME, result.first().waitingForParties.first().name)
        }
    }

    @Test(timeout = 300_000)
    fun `query should return flow which is waiting for ledger commit as an async external operation` () {
        val txId = SecureHash.randomSHA256()
        aliceNode.services.startFlow(WaitForLedgerCommitFlow(txId))

        val flowOperator = FlowOperator(aliceNode.internals.smm, aliceNode.services.clock)

        executeTest(5.seconds) {
            val suspendedFlows = flowOperator.getAllWaitingFlows().toList()
            assertEquals(1, suspendedFlows.size)
            val waitingFlowInfo = flowOperator.queryWaitingFlows(WaitingFlowQuery()).single()
            assertEquals(WaitingSource.EXTERNAL_OPERATION, waitingFlowInfo.source)
            assertEquals("net.corda.core.internal.WaitForLedgerCommit", waitingFlowInfo.externalOperationImplName)
        }
    }

    @InitiatingFlow
    class ReceiveFlow(private val payload: String, private val otherParties: List<Party>) : FlowLogic<Unit>() {
        init {
            require(otherParties.isNotEmpty())
        }

        @Suspendable
        override fun call() {
            if (payload == "Fail") {
                error(payload)
            }

            val sessions = mutableMapOf<FlowSession, Class<out Any>>()
            otherParties.forEach {
                sessions[initiateFlow(it)] = String::class.java
            }

            receiveAllMap(sessions)
        }
    }

    @InitiatingFlow
    class SendAndReceiveFlow(private val payload: String, private val otherParties: List<Party>) : FlowLogic<Unit>() {
        init {
            require(otherParties.isNotEmpty())
        }

        @Suspendable
        override fun call() {
            if (payload == "Fail") {
                error(payload)
            }
            otherParties.forEach {
                val session = initiateFlow(it)
                session.sendAndReceive<String>(payload)
            }
        }
    }

    @InitiatingFlow
    class GetFlowInfoFlow(private val otherParties: List<Party>) : FlowLogic<FlowInfo>() {
        init {
            require(otherParties.isNotEmpty())
        }

        @Suspendable
        override fun call(): FlowInfo {
            val flowInfo = otherParties.map {
                val session = initiateFlow(it)
                session.getCounterpartyFlowInfo()
            }.toList()
            return flowInfo.first()
        }
    }

    @InitiatingFlow
    class ExternalAsyncOperationFlow(private val future: CompletableFuture<String>) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            await(ExternalOperation(future))
        }

        class ExternalOperation(private val future: CompletableFuture<String>) : FlowExternalAsyncOperation<String> {
            override fun execute(deduplicationId: String): CompletableFuture<String> {
                return future
            }
        }
    }

    @InitiatingFlow
    class ExternalOperationFlow(private val future: CompletableFuture<String>) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            await(ExternalOperation(future))
        }

        class ExternalOperation(private val future: CompletableFuture<String>) : FlowExternalOperation<String> {
            override fun execute(deduplicationId: String): String {
                return future.get()
            }
        }
    }

    @InitiatingFlow
    class SleepFlow : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            sleep(15.seconds)
        }
    }

    @InitiatingFlow
    class SendFlow(private val payload: String, private val otherParties: List<Party>) : FlowLogic<Unit>() {
        init {
            require(otherParties.isNotEmpty())
        }

        @Suspendable
        override fun call() {
            if (payload == "Fail") {
                error(payload)
            }
            otherParties.forEach {
                initiateFlow(it).send(payload)
            }
        }
    }

    class AcceptingFlow(private val payload: Any, private val otherPartySession: FlowSession) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            if (payload == "Fail") {
                error(payload)
            }

            otherPartySession.send(payload)
        }
    }

    @InitiatingFlow
    class WaitForLedgerCommitFlow(private val txId: SecureHash) : FlowLogic<SignedTransaction>() {
        @Suspendable
        override fun call(): SignedTransaction {
            return waitForLedgerCommit(txId)
        }
    }

    class BlockingMessageSpy(
            private val expectedPayload: String,
            private val future: CompletableFuture<Unit>
    ) : MessagingServiceSpy() {
        @Suppress("TooGenericExceptionCaught")
        override fun send(message: Message, target: MessageRecipients, sequenceKey: Any) {
            try {
                val sessionMessage = message.data.bytes.deserialize<InitialSessionMessage>()
                if (sessionMessage.firstPayload?.deserialize<String>() == expectedPayload) {
                    future.get()
                }
            } catch (e: Throwable) {
                log.error("Expected '${InitialSessionMessage::class.qualifiedName}'", e)
            }
            messagingService.send(message, target)
        }
    }
}