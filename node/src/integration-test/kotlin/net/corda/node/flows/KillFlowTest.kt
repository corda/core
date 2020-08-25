package net.corda.node.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.client.rpc.ext.MultiRPCClient
import net.corda.core.contracts.StateRef
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.FlowExternalOperation
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.FlowSession
import net.corda.core.flows.InitiatedBy
import net.corda.core.flows.InitiatingFlow
import net.corda.core.flows.KilledFlowException
import net.corda.core.flows.StartableByRPC
import net.corda.core.flows.StateMachineRunId
import net.corda.core.flows.UnexpectedFlowEndException
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.concurrent.transpose
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.startFlow
import net.corda.core.node.services.StatesNotAvailableException
import net.corda.core.utilities.NonEmptySet
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.minutes
import net.corda.core.utilities.seconds
import net.corda.finance.DOLLARS
import net.corda.finance.contracts.asset.Cash
import net.corda.finance.flows.CashIssueFlow
import net.corda.node.services.Permissions
import net.corda.node.services.statemachine.Checkpoint
import net.corda.node.services.statemachine.LockingRpc
import net.corda.testing.core.ALICE_NAME
import net.corda.testing.core.BOB_NAME
import net.corda.testing.core.CHARLIE_NAME
import net.corda.testing.core.singleIdentity
import net.corda.testing.driver.DriverParameters
import net.corda.testing.driver.NodeParameters
import net.corda.testing.driver.OutOfProcess
import net.corda.testing.driver.driver
import net.corda.testing.driver.internal.incrementalPortAllocation
import net.corda.testing.node.User
import net.corda.testing.node.internal.FINANCE_CORDAPPS
import org.assertj.core.api.Assertions
import org.junit.Test
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.system.measureTimeMillis
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class KillFlowTest {

    @Test(timeout = 300_000)
    fun `a killed flow will end when it reaches the next suspension point`() {

        // Allocate named port to be used for RPC interaction
        val rpcAddress = incrementalPortAllocation().nextHostAndPort()

        // Create a specific RPC user
        val rpcUser = User("MultiRpcClientTest", "MultiRpcClientTestPwd", setOf(Permissions.all()))

        driver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {
            val alice = startNode(providedName = ALICE_NAME, defaultParameters = NodeParameters(rpcAddress = rpcAddress, rpcUsers = listOf(rpcUser))).getOrThrow()
            alice.rpc.let { rpc ->
                val handle = rpc.startFlow(::AFlowThatGetsMurderedWhenItTriesToSuspend)
                AFlowThatGetsMurderedWhenItTriesToSuspend.lockA.acquire()
                rpc.killFlow(handle.id)
                AFlowThatGetsMurderedWhenItTriesToSuspend.lockB.release()
                assertFailsWith<KilledFlowException> {
                    handle.returnValue.getOrThrow(1.minutes)
                }
                assertEquals(1, rpc.startFlow(::GetNumberOfCheckpointsFlow).returnValue.getOrThrow(20.seconds))
            }
        }
    }

    @Test(timeout = 300_000)
    fun `a killed flow will propagate the killed error to counter parties when it reaches the next suspension point`() {
        driver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {
            val (alice, bob, charlie) = listOf(ALICE_NAME, BOB_NAME, CHARLIE_NAME)
                .map { startNode(providedName = it) }
                .transpose()
                .getOrThrow()
            alice.rpc.let { rpc ->

                AFlowThatGetsMurderedWhenItTriesToSuspendAndSomehowKillsItsFriendsResponder.locks[BOB_NAME] = CountDownLatch(1)
                AFlowThatGetsMurderedWhenItTriesToSuspendAndSomehowKillsItsFriendsResponder.locks[CHARLIE_NAME] = CountDownLatch(1)

                val handle = rpc.startFlow(
                    ::AFlowThatGetsMurderedWhenItTriesToSuspendAndSomehowKillsItsFriends,
                    listOf(bob.nodeInfo.singleIdentity(), charlie.nodeInfo.singleIdentity())
                )

                AFlowThatGetsMurderedWhenItTriesToSuspendAndSomehowKillsItsFriends.lockA.acquire()

                AFlowThatGetsMurderedWhenItTriesToSuspendAndSomehowKillsItsFriendsResponder.locks.forEach { (_, lock) ->
                    lock.await(30, TimeUnit.SECONDS)
                }

                AFlowThatGetsMurderedWhenItTriesToSuspendAndSomehowKillsItsFriendsResponder.locks[BOB_NAME] = CountDownLatch(1)
                AFlowThatGetsMurderedWhenItTriesToSuspendAndSomehowKillsItsFriendsResponder.locks[CHARLIE_NAME] = CountDownLatch(1)

                rpc.killFlow(handle.id)

                AFlowThatGetsMurderedWhenItTriesToSuspendAndSomehowKillsItsFriends.lockB.release()

                assertFailsWith<KilledFlowException> {
                    handle.returnValue.getOrThrow(1.minutes)
                }

                AFlowThatGetsMurderedWhenItTriesToSuspendAndSomehowKillsItsFriendsResponder.locks.forEach { (_, lock) ->
                    lock.await(30, TimeUnit.SECONDS)
                }

                assertTrue(AFlowThatGetsMurderedWhenItTriesToSuspendAndSomehowKillsItsFriendsResponder.receivedKilledExceptions[BOB_NAME]!!)
                assertTrue(AFlowThatGetsMurderedWhenItTriesToSuspendAndSomehowKillsItsFriendsResponder.receivedKilledExceptions[CHARLIE_NAME]!!)
                assertEquals(1, rpc.startFlow(::GetNumberOfCheckpointsFlow).returnValue.getOrThrow(20.seconds))
                assertEquals(1, bob.rpc.startFlow(::GetNumberOfCheckpointsFlow).returnValue.getOrThrow(20.seconds))
                assertEquals(1, charlie.rpc.startFlow(::GetNumberOfCheckpointsFlow).returnValue.getOrThrow(20.seconds))
            }
        }
    }

    @Test(timeout = 300_000)
    fun `killing a flow that is sleeping ends the flow immediately`() {
        driver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {
            val alice = startNode(providedName = ALICE_NAME).getOrThrow()
            alice.rpc.let { rpc ->
                val handle = rpc.startFlow(::AFlowThatGetsMurdered)
                Thread.sleep(5000)
                val time = measureTimeMillis {
                    rpc.killFlow(handle.id)
                    assertFailsWith<KilledFlowException> {
                        handle.returnValue.getOrThrow(1.minutes)
                    }
                }
                assertTrue(time < 1.minutes.toMillis(), "It should at a minimum, take less than a minute to kill this flow")
                assertTrue(time < 5.seconds.toMillis(), "Really, it should take less than a few seconds to kill a flow")
                assertEquals(1, rpc.startFlow(::GetNumberOfCheckpointsFlow).returnValue.getOrThrow(20.seconds))
            }
        }
    }

    @Test(timeout = 300_000)
    fun `killing a flow suspended in send + receive + sendAndReceive ends the flow immediately`() {
        driver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = false)) {
            val (alice, bob) = listOf(ALICE_NAME, BOB_NAME)
                .map { startNode(providedName = it) }
                .transpose()
                .getOrThrow()
            val bobParty = bob.nodeInfo.singleIdentity()
            bob.stop()
            val terminated = (bob as OutOfProcess).process.waitFor(30, TimeUnit.SECONDS)
            if (terminated) {
                alice.rpc.run {
                    killFlowAndAssert(::AFlowThatGetsMurderedTryingToSendAMessage, bobParty)
                    killFlowAndAssert(::AFlowThatGetsMurderedTryingToReceiveAMessage, bobParty)
                    killFlowAndAssert(::AFlowThatGetsMurderedTryingToSendAndReceiveAMessage, bobParty)
                }
            } else {
                throw IllegalStateException("The node should have terminated!")
            }
        }
    }

    private inline fun <reified T : FlowLogic<Unit>> CordaRPCOps.killFlowAndAssert(flow: (Party) -> T, party: Party) {
        val handle = startFlow(flow, party)
        Thread.sleep(5000)
        val time = measureTimeMillis {
            killFlow(handle.id)
            assertFailsWith<KilledFlowException> {
                handle.returnValue.getOrThrow(1.minutes)
            }
        }
        assertTrue(time < 1.minutes.toMillis(), "It should at a minimum, take less than a minute to kill this flow")
        assertTrue(time < 5.seconds.toMillis(), "Really, it should take less than a few seconds to kill a flow")
        assertEquals(1, startFlow(::GetNumberOfCheckpointsFlow).returnValue.getOrThrow(20.seconds))
    }

    @Test(timeout = 300_000)
    fun `killing a flow suspended in waitForLedgerCommit ends the flow immediately`() {
        driver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {
            val alice = startNode(providedName = ALICE_NAME).getOrThrow()
            alice.rpc.let { rpc ->
                val handle = rpc.startFlow(::AFlowThatGetsMurderedTryingToWaitForATransaction)
                Thread.sleep(5000)
                val time = measureTimeMillis {
                    rpc.killFlow(handle.id)
                    assertFailsWith<KilledFlowException> {
                        handle.returnValue.getOrThrow(1.minutes)
                    }
                }
                assertTrue(time < 1.minutes.toMillis(), "It should at a minimum, take less than a minute to kill this flow")
                assertTrue(time < 5.seconds.toMillis(), "Really, it should take less than a few seconds to kill a flow")
                assertEquals(1, rpc.startFlow(::GetNumberOfCheckpointsFlow).returnValue.getOrThrow(20.seconds))
            }
        }
    }

    @Test(timeout = 300_000)
    fun `killing a flow suspended in await ends the flow immediately`() {
        driver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {
            val alice = startNode(providedName = ALICE_NAME).getOrThrow()
            alice.rpc.let { rpc ->
                val handle = rpc.startFlow(::AFlowThatGetsMurderedTryingToAwaitAFuture)
                Thread.sleep(5000)
                val time = measureTimeMillis {
                    rpc.killFlow(handle.id)
                    assertFailsWith<KilledFlowException> {
                        handle.returnValue.getOrThrow(1.minutes)
                    }
                }
                assertTrue(time < 1.minutes.toMillis(), "It should at a minimum, take less than a minute to kill this flow")
                assertTrue(time < 5.seconds.toMillis(), "Really, it should take less than a few seconds to kill a flow")
                assertEquals(1, rpc.startFlow(::GetNumberOfCheckpointsFlow).returnValue.getOrThrow(20.seconds))
            }
        }
    }

    @Test(timeout = 300_000)
    fun `a killed flow will propagate the killed error to counter parties if it was suspended`() {
        driver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {
            val (alice, bob, charlie) = listOf(ALICE_NAME, BOB_NAME, CHARLIE_NAME)
                .map { startNode(providedName = it) }
                .transpose()
                .getOrThrow()
            alice.rpc.let { rpc ->

                AFlowThatGetsMurderedAndSomehowKillsItsFriendsResponder.locks[BOB_NAME] = CountDownLatch(1)
                AFlowThatGetsMurderedAndSomehowKillsItsFriendsResponder.locks[CHARLIE_NAME] = CountDownLatch(1)

                val handle = rpc.startFlow(
                    ::AFlowThatGetsMurderedAndSomehowKillsItsFriends,
                    listOf(bob.nodeInfo.singleIdentity(), charlie.nodeInfo.singleIdentity())
                )

                AFlowThatGetsMurderedAndSomehowKillsItsFriendsResponder.locks.forEach { (_, lock) -> lock.await(30, TimeUnit.SECONDS) }

                AFlowThatGetsMurderedAndSomehowKillsItsFriendsResponder.locks[BOB_NAME] = CountDownLatch(1)
                AFlowThatGetsMurderedAndSomehowKillsItsFriendsResponder.locks[CHARLIE_NAME] = CountDownLatch(1)

                rpc.killFlow(handle.id)
                assertFailsWith<KilledFlowException> {
                    handle.returnValue.getOrThrow(20.seconds)
                }

                AFlowThatGetsMurderedAndSomehowKillsItsFriendsResponder.locks.forEach { (_, lock) -> lock.await(30, TimeUnit.SECONDS) }

                assertTrue(AFlowThatGetsMurderedAndSomehowKillsItsFriendsResponder.receivedKilledExceptions[BOB_NAME]!!)
                assertTrue(AFlowThatGetsMurderedAndSomehowKillsItsFriendsResponder.receivedKilledExceptions[CHARLIE_NAME]!!)
                assertEquals(1, rpc.startFlow(::GetNumberOfCheckpointsFlow).returnValue.getOrThrow(20.seconds))
                assertEquals(1, bob.rpc.startFlow(::GetNumberOfCheckpointsFlow).returnValue.getOrThrow(20.seconds))
                assertEquals(1, charlie.rpc.startFlow(::GetNumberOfCheckpointsFlow).returnValue.getOrThrow(20.seconds))
            }
        }
    }

    @Test(timeout = 300_000)
    fun `a killed initiated flow will propagate the killed error to the initiator and its counter parties`() {
        driver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {
            val (alice, bob, charlie) = listOf(ALICE_NAME, BOB_NAME, CHARLIE_NAME)
                .map { startNode(providedName = it) }
                .transpose()
                .getOrThrow()

            AFlowThatGetsMurderedByItsFriendResponder.locks[BOB_NAME] = CountDownLatch(1)
            AFlowThatGetsMurderedByItsFriendResponder.locks[CHARLIE_NAME] = CountDownLatch(1)

            val handle = alice.rpc.startFlow(
                ::AFlowThatGetsMurderedByItsFriend,
                listOf(bob.nodeInfo.singleIdentity(), charlie.nodeInfo.singleIdentity())
            )

            AFlowThatGetsMurderedByItsFriendResponder.locks.forEach { (_, lock) -> lock.await(30, TimeUnit.SECONDS) }

            AFlowThatGetsMurderedByItsFriendResponder.locks[CHARLIE_NAME] = CountDownLatch(1)

            val initiatedFlowId = AFlowThatGetsMurderedByItsFriendResponder.flowIds[BOB_NAME]!!

            bob.rpc.killFlow(initiatedFlowId)

            assertFailsWith<UnexpectedFlowEndException> {
                handle.returnValue.getOrThrow(1.minutes)
            }

            AFlowThatGetsMurderedByItsFriendResponder.locks[CHARLIE_NAME]!!.await(30, TimeUnit.SECONDS)

            assertTrue(AFlowThatGetsMurderedByItsFriend.receivedKilledException)
            assertFalse(AFlowThatGetsMurderedByItsFriendResponder.receivedKilledExceptions[BOB_NAME]!!)
            assertTrue(AFlowThatGetsMurderedByItsFriendResponder.receivedKilledExceptions[CHARLIE_NAME]!!)
            assertEquals(1, alice.rpc.startFlow(::GetNumberOfCheckpointsFlow).returnValue.getOrThrow(20.seconds))
            assertEquals(1, bob.rpc.startFlow(::GetNumberOfCheckpointsFlow).returnValue.getOrThrow(20.seconds))
            assertEquals(1, charlie.rpc.startFlow(::GetNumberOfCheckpointsFlow).returnValue.getOrThrow(20.seconds))
        }
    }

    @Test(timeout = 300_000)
    fun `killing a flow releases soft lock`() {
        driver(DriverParameters(startNodesInProcess = true)) {
            val alice = startNode(
                providedName = ALICE_NAME,
                defaultParameters = NodeParameters(additionalCordapps = FINANCE_CORDAPPS)
            ).getOrThrow()
            alice.rpc.let { rpc ->
                val issuerRef = OpaqueBytes("BankOfMars".toByteArray())
                val cash = rpc.startFlow(
                    ::CashIssueFlow,
                    10.DOLLARS,
                    issuerRef,
                    defaultNotaryIdentity
                ).returnValue.getOrThrow().stx.tx.outRefsOfType<Cash.State>().single()
                val flow = rpc.startFlow(::SoftLock, cash.ref, Duration.ofMinutes(5))

                var locked = false
                while (!locked) {
                    try {
                        rpc.startFlow(::SoftLock, cash.ref, Duration.ofSeconds(1)).returnValue.getOrThrow()
                    } catch (e: StatesNotAvailableException) {
                        locked = true
                    }
                }

                val killed = rpc.killFlow(flow.id)
                assertTrue(killed)
                Assertions.assertThatCode {
                    rpc.startFlow(
                        ::SoftLock,
                        cash.ref,
                        Duration.ofSeconds(1)
                    ).returnValue.getOrThrow()
                }.doesNotThrowAnyException()
            }
        }
    }

    @StartableByRPC
    class AFlowThatGetsMurderedWhenItTriesToSuspend : FlowLogic<Unit>() {

        companion object {
            val lockA = Semaphore(0)
            val lockB = Semaphore(0)
        }

        @Suspendable
        override fun call() {
            lockA.release()
            sleep(1.seconds)
            lockB.acquire()
            sleep(1.seconds)
        }
    }

    @StartableByRPC
    @InitiatingFlow
    class AFlowThatGetsMurderedWhenItTriesToSuspendAndSomehowKillsItsFriends(private val parties: List<Party>) : FlowLogic<Unit>() {

        companion object {
            val lockA = Semaphore(0)
            val lockB = Semaphore(0)
        }

        @Suspendable
        override fun call() {
            val sessionOne = initiateFlow(parties[0])
            val sessionTwo = initiateFlow(parties[1])
            // trigger sessions with 2 counter parties
            sessionOne.sendAndReceive<String>("what is up")
            sessionOne.send("what is up 2")
            sessionTwo.sendAndReceive<String>("what is up")
            sessionTwo.send("what is up 2")
            lockA.release()
            lockB.acquire()
            sleep(1.seconds)
        }
    }

    @InitiatedBy(AFlowThatGetsMurderedWhenItTriesToSuspendAndSomehowKillsItsFriends::class)
    class AFlowThatGetsMurderedWhenItTriesToSuspendAndSomehowKillsItsFriendsResponder(private val session: FlowSession) :
        FlowLogic<Unit>() {

        companion object {
            val locks = ConcurrentHashMap<CordaX500Name, CountDownLatch>()
            var receivedKilledExceptions = ConcurrentHashMap<CordaX500Name, Boolean>().apply {
                this[BOB_NAME] = false
                this[CHARLIE_NAME] = false
            }
        }

        @Suspendable
        override fun call() {
            session.receive<String>()
            session.send("hi")
            session.receive<String>()
            locks[ourIdentity.name]!!.countDown()
            try {
                session.receive<String>()
            } catch (e: UnexpectedFlowEndException) {
                receivedKilledExceptions[ourIdentity.name] = true
                locks[ourIdentity.name]!!.countDown()
                throw e
            }
        }
    }

    @StartableByRPC
    @InitiatingFlow
    class AFlowThatGetsMurdered : FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            sleep(1.minutes)
        }
    }

    @StartableByRPC
    @InitiatingFlow
    class AFlowThatGetsMurderedTryingToSendAMessage(private val party: Party) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            val session = initiateFlow(party)
            session.send("hi")
        }
    }

    @InitiatedBy(AFlowThatGetsMurderedTryingToSendAMessage::class)
    class AFlowThatGetsMurderedTryingToSendAMessageResponder(private val session: FlowSession) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            session.send("haha")
        }
    }

    @StartableByRPC
    @InitiatingFlow
    class AFlowThatGetsMurderedTryingToReceiveAMessage(private val party: Party) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            val session = initiateFlow(party)
            session.receive<String>()
        }
    }

    @InitiatedBy(AFlowThatGetsMurderedTryingToReceiveAMessage::class)
    class AFlowThatGetsMurderedTryingToReceiveAMessageResponder(private val session: FlowSession) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            session.receive<String>()
        }
    }

    @StartableByRPC
    @InitiatingFlow
    class AFlowThatGetsMurderedTryingToSendAndReceiveAMessage(private val party: Party) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            val session = initiateFlow(party)
            session.sendAndReceive<String>("hi")
        }
    }

    @InitiatedBy(AFlowThatGetsMurderedTryingToSendAndReceiveAMessage::class)
    class AFlowThatGetsMurderedTryingToSendAndReceiveAMessageResponder(private val session: FlowSession) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            session.receive<String>()
        }
    }

    @StartableByRPC
    @InitiatingFlow
    class AFlowThatGetsMurderedTryingToWaitForATransaction : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            waitForLedgerCommit(SecureHash.randomSHA256())
        }
    }

    @StartableByRPC
    @InitiatingFlow
    class AFlowThatGetsMurderedTryingToAwaitAFuture : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            await(MyFuture())
        }

        class MyFuture : FlowExternalOperation<Unit> {
            override fun execute(deduplicationId: String) {
                Thread.sleep(3.minutes.toMillis())
            }
        }
    }

    @StartableByRPC
    @InitiatingFlow
    class AFlowThatGetsMurderedAndSomehowKillsItsFriends(private val parties: List<Party>) : FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            val sessionOne = initiateFlow(parties[0])
            val sessionTwo = initiateFlow(parties[1])
            // trigger sessions with 2 counter parties
            sessionOne.sendAndReceive<String>("what is up")
            sessionOne.send("what is up 2")
            // why is this second send needed to cause the kill command to propagate to the other side
            // will be exactly the same for normal error propagation
            sessionTwo.sendAndReceive<String>("what is up")
            sessionTwo.send("what is up 2")
            sleep(3.minutes)
        }
    }

    @InitiatedBy(AFlowThatGetsMurderedAndSomehowKillsItsFriends::class)
    class AFlowThatGetsMurderedAndSomehowKillsItsFriendsResponder(private val session: FlowSession) : FlowLogic<Unit>() {

        companion object {
            val locks = ConcurrentHashMap<CordaX500Name, CountDownLatch>()
            var receivedKilledExceptions = ConcurrentHashMap<CordaX500Name, Boolean>().apply {
                this[BOB_NAME] = false
                this[CHARLIE_NAME] = false
            }
        }

        @Suspendable
        override fun call() {
            session.receive<String>()
            session.send("hi")
            session.receive<String>()
            locks[ourIdentity.name]!!.countDown()
            try {
                session.receive<String>()
            } catch (e: UnexpectedFlowEndException) {
                receivedKilledExceptions[ourIdentity.name] = true
                locks[ourIdentity.name]!!.countDown()
                throw e
            }
        }
    }

    @StartableByRPC
    @InitiatingFlow
    class AFlowThatGetsMurderedByItsFriend(private val parties: List<Party>) : FlowLogic<Unit>() {

        companion object {
            var receivedKilledException = false
        }

        @Suspendable
        override fun call() {
            val sessionOne = initiateFlow(parties[0])
            val sessionTwo = initiateFlow(parties[1])
            // trigger sessions with 2 counter parties
            sessionOne.sendAndReceive<String>("what is up")
            sessionOne.send("what is up 2")
            // why is this second send needed to cause the kill command to propagate to the other side
            // will be exactly the same for normal error propagation
            sessionTwo.sendAndReceive<String>("what is up")
            sessionTwo.send("what is up 2")
            try {
                sessionOne.receive<String>()
            } catch (e: UnexpectedFlowEndException) {
                logger.info("Received exception in initiating flow")
                receivedKilledException = true
                throw e
            }
        }
    }

    @InitiatedBy(AFlowThatGetsMurderedByItsFriend::class)
    class AFlowThatGetsMurderedByItsFriendResponder(private val session: FlowSession) : FlowLogic<Unit>() {

        companion object {
            val locks = ConcurrentHashMap<CordaX500Name, CountDownLatch>()
            var receivedKilledExceptions = ConcurrentHashMap<CordaX500Name, Boolean>().apply {
                this[BOB_NAME] = false
                this[CHARLIE_NAME] = false
            }
            var flowIds = ConcurrentHashMap<CordaX500Name, StateMachineRunId>()
        }

        @Suspendable
        override fun call() {
            flowIds[ourIdentity.name] = runId
            session.receive<String>()
            session.send("hi")
            session.receive<String>()
            locks[ourIdentity.name]!!.countDown()
            try {
                session.receive<String>()
            } catch (e: UnexpectedFlowEndException) {
                receivedKilledExceptions[ourIdentity.name] = true
                locks[ourIdentity.name]!!.countDown()
                throw e
            }
        }
    }

    @StartableByRPC
    class SoftLock(private val stateRef: StateRef, private val duration: Duration) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            logger.info("Soft locking state with hash $stateRef...")
            serviceHub.vaultService.softLockReserve(runId.uuid, NonEmptySet.of(stateRef))
            sleep(duration)
        }
    }

    @StartableByRPC
    class GetNumberOfCheckpointsFlow : FlowLogic<Long>() {
        override fun call(): Long {
            return serviceHub.jdbcSession().prepareStatement("select count(*) from node_checkpoints").use { ps ->
                ps.executeQuery().use { rs ->
                    rs.next()
                    rs.getLong(1)
                }
            }
        }
    }
}