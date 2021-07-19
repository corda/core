package net.corda.node.services.statemachine

import co.paralleluniverse.fibers.Fiber
import co.paralleluniverse.fibers.Suspendable
import co.paralleluniverse.strands.Strand
import co.paralleluniverse.strands.concurrent.Semaphore
import net.corda.client.rpc.notUsed
import net.corda.core.concurrent.CordaFuture
import net.corda.core.contracts.ContractState
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.random63BitValue
import net.corda.core.flows.Destination
import net.corda.core.flows.FinalityFlow
import net.corda.core.flows.FlowException
import net.corda.core.flows.FlowExternalAsyncOperation
import net.corda.core.flows.FlowExternalOperation
import net.corda.core.flows.FlowInfo
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.FlowSession
import net.corda.core.flows.HospitalizeFlowException
import net.corda.core.flows.InitiatingFlow
import net.corda.core.flows.ReceiveFinalityFlow
import net.corda.core.flows.StateMachineRunId
import net.corda.core.flows.UnexpectedFlowEndException
import net.corda.core.identity.Party
import net.corda.core.internal.DeclaredField
import net.corda.core.internal.FlowIORequest
import net.corda.core.internal.FlowStateMachine
import net.corda.core.internal.concurrent.flatMap
import net.corda.core.internal.concurrent.openFuture
import net.corda.core.internal.declaredField
import net.corda.core.messaging.MessageRecipients
import net.corda.core.node.services.PartyInfo
import net.corda.core.node.services.queryBy
import net.corda.core.serialization.SerializationDefaults
import net.corda.core.serialization.SerializedBytes
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.toFuture
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.ProgressTracker
import net.corda.core.utilities.ProgressTracker.Change
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.seconds
import net.corda.core.utilities.unwrap
import net.corda.node.services.persistence.CheckpointPerformanceRecorder
import net.corda.node.services.persistence.DBCheckpointStorage
import net.corda.node.services.persistence.checkpoints
import net.corda.nodeapi.internal.persistence.DatabaseTransaction
import net.corda.nodeapi.internal.persistence.currentDBSession
import net.corda.testing.contracts.DummyContract
import net.corda.testing.contracts.DummyState
import net.corda.testing.core.ALICE_NAME
import net.corda.testing.core.BOB_NAME
import net.corda.testing.core.dummyCommand
import net.corda.testing.core.singleIdentity
import net.corda.testing.flows.registerCordappFlowFactory
import net.corda.testing.internal.IS_OPENJ9
import net.corda.testing.internal.LogHelper
import net.corda.testing.node.InMemoryMessagingNetwork.MessageTransfer
import net.corda.testing.node.InMemoryMessagingNetwork.ServicePeerAllocationStrategy.RoundRobin
import net.corda.testing.node.internal.DUMMY_CONTRACTS_CORDAPP
import net.corda.testing.node.internal.FINANCE_CONTRACTS_CORDAPP
import net.corda.testing.node.internal.InternalMockNetwork
import net.corda.testing.node.internal.InternalMockNodeParameters
import net.corda.testing.node.internal.TestStartedNode
import net.corda.testing.node.internal.getMessage
import net.corda.testing.node.internal.startFlow
import net.corda.testing.node.internal.startFlowWithClientId
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatIllegalArgumentException
import org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType
import org.assertj.core.api.Condition
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assume
import org.junit.Before
import org.junit.Test
import rx.Notification
import rx.Observable
import java.sql.SQLTransientConnectionException
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeoutException
import java.util.function.Predicate
import kotlin.concurrent.thread
import kotlin.reflect.KClass
import kotlin.streams.toList
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class FlowFrameworkTests {
    companion object {
        init {
            LogHelper.setLevel("+net.corda.flow")
        }
    }

    private lateinit var mockNet: InternalMockNetwork
    private lateinit var aliceNode: TestStartedNode
    private lateinit var bobNode: TestStartedNode
    private lateinit var alice: Party
    private lateinit var bob: Party
    private lateinit var notaryIdentity: Party
    private val receivedSessionMessages = ArrayList<SessionTransfer>()

    private val dbCheckpointStorage = DBCheckpointStorage(
        object : CheckpointPerformanceRecorder {
            override fun record(
                serializedCheckpointState: SerializedBytes<CheckpointState>,
                serializedFlowState: SerializedBytes<FlowState>?
            ) {
                // do nothing
            }
        },
        Clock.systemUTC()
    )

    @Before
    fun setUpMockNet() {
        mockNet = InternalMockNetwork(
            cordappsForAllNodes = listOf(DUMMY_CONTRACTS_CORDAPP, FINANCE_CONTRACTS_CORDAPP),
            servicePeerAllocationStrategy = RoundRobin()
        )

        aliceNode = mockNet.createNode(InternalMockNodeParameters(legalName = ALICE_NAME))
        bobNode = mockNet.createNode(InternalMockNodeParameters(legalName = BOB_NAME))

        // Extract identities
        alice = aliceNode.info.singleIdentity()
        bob = bobNode.info.singleIdentity()
        notaryIdentity = mockNet.defaultNotaryIdentity

        receivedSessionMessagesObservable().forEach { receivedSessionMessages += it }
    }

    private fun receivedSessionMessagesObservable(): Observable<SessionTransfer> {
        return mockNet.messagingNetwork.receivedMessages.toSessionTransfers()
    }

    @After
    fun cleanUp() {
        mockNet.stopNodes()
        receivedSessionMessages.clear()

        SuspendingFlow.hookBeforeCheckpoint = {}
        SuspendingFlow.hookAfterCheckpoint = {}
        StaffedFlowHospital.onFlowResuscitated.clear()
        StaffedFlowHospital.onFlowKeptForOvernightObservation.clear()
        WaitForLedgerCommitFlow.preWaitForLedgerCommitHook = null
        WaitForLedgerCommitFlow.postWaitForLedgerCommitHook = null
    }

    @Test(timeout=300_000)
	fun `flow can lazily use the serviceHub in its constructor`() {
        val flow = LazyServiceHubAccessFlow()
        aliceNode.services.startFlow(flow)
        assertThat(flow.lazyTime).isNotNull()
    }

    class SuspendThrowingActionExecutor(private val exception: Exception, private val delegate: ActionExecutor) : ActionExecutor {
        private var thrown = false
        @Suspendable
        override fun executeAction(fiber: FlowFiber, action: Action) {
            if (action is Action.CommitTransaction && !thrown) {
                thrown = true
                throw exception
            } else {
                delegate.executeAction(fiber, action)
            }
        }
    }

    @Test(timeout=300_000)
	fun `exception while fiber suspended is retried and completes successfully`() {
        bobNode.registerCordappFlowFactory(ReceiveFlow::class) { InitiatedSendFlow("Hello", it) }
        val flow = ReceiveFlow(bob)
        val fiber = aliceNode.services.startFlow(flow) as FlowStateMachineImpl
        // Before the flow runs change the suspend action to throw an exception
        val throwingActionExecutor = SuspendThrowingActionExecutor(
            Exception("Thrown during suspend"),
            fiber.transientValues.actionExecutor
        )
        fiber.declaredField<TransientReference<FlowStateMachineImpl.TransientValues>>("transientValuesReference").value =
            TransientReference(fiber.transientValues.copy(actionExecutor = throwingActionExecutor))
        mockNet.runNetwork()
        fiber.resultFuture.getOrThrow()
        assertThat(aliceNode.smm.allStateMachines).isEmpty()
        // Make sure the fiber does actually terminate
        assertThat(fiber.state).isEqualTo(Strand.State.WAITING)
    }

    @Test(timeout=300_000)
	fun `both sides do a send as their first IO request`() {
        bobNode.registerCordappFlowFactory(PingPongFlow::class) { PingPongFlow(it, 20L) }
        aliceNode.services.startFlow(PingPongFlow(bob, 10L))
        mockNet.runNetwork()

        assertSessionTransfers(
            aliceNode sent sessionInit(PingPongFlow::class, payload = 10L) to bobNode,
            bobNode sent sessionConfirm() to aliceNode,
            bobNode sent sessionData(20L) to aliceNode,
            aliceNode sent sessionData(11L) to bobNode,
            bobNode sent sessionData(21L) to aliceNode,
            aliceNode sent normalEnd to bobNode,
            bobNode sent normalEnd to aliceNode
        )
    }

    @Test(timeout=300_000)
    fun `other side ends before doing expected send`() {
        bobNode.registerCordappFlowFactory(ReceiveFlow::class) { NoOpFlow() }
        val resultFuture = aliceNode.services.startFlow(ReceiveFlow(bob)).resultFuture
        mockNet.runNetwork()
        assertThatExceptionOfType(UnexpectedFlowEndException::class.java).isThrownBy {
            resultFuture.getOrThrow()
        }
    }

    @Test(timeout=300_000)
	fun `FlowMonitor flow suspends on a FlowIORequest`() { // alice flow only, suspends on a FlowIORequest
        monitorFlows { aliceFlowMonitor, bobFlowMonitor ->
            val terminationSignal = Semaphore(0)
            // bob's flow need to wait otherwise it could end the session prematurely
            bobNode.registerCordappFlowFactory(ReceiveFlow::class) { NoOpFlow( terminateUponSignal = terminationSignal) }
            aliceNode.services.startFlow(ReceiveFlow(bob))
            mockNet.runNetwork()
            assertEquals(1, aliceFlowMonitor.waitingFlowDurations(Duration.ZERO).toSet().size)
            assertEquals(0, bobFlowMonitor.waitingFlowDurations(Duration.ZERO).toSet().size)
            // continue bob's NoOpFlow, it will send an EndSessionMessage to alice
            terminationSignal.release()
            mockNet.runNetwork()
            // alice's ReceiveFlow is not finished because bob sent an EndSessionMessage, check that flow is no longer waiting
            assertEquals(0, aliceFlowMonitor.waitingFlowDurations(Duration.ZERO).toSet().size)
        }
    }

    @Test(timeout=300_000)
	fun `FlowMonitor flows suspend on a FlowIORequest`() { // alice and bob's flows, both suspend on a FlowIORequest
        monitorFlows { aliceFlowMonitor, bobFlowMonitor ->
            bobNode.registerCordappFlowFactory(ReceiveFlow::class) { InitiatedReceiveFlow(it) }
            aliceNode.services.startFlow(ReceiveFlow(bob))
            mockNet.runNetwork()
            // both flows are suspened on a receive from the counter party
            assertEquals(1, aliceFlowMonitor.waitingFlowDurations(Duration.ZERO).toSet().size)
            assertEquals(1, bobFlowMonitor.waitingFlowDurations(Duration.ZERO).toSet().size)
        }
    }

    @Test(timeout=300_000)
	fun `FlowMonitor flow is running`() { // flow is running a "take a long time" task
        monitorFlows { aliceFlowMonitor, _ ->
            val terminationSignal = Semaphore(0)
            // "take a long time" task, implemented by a NoOpFlow stuck in call method
            aliceNode.services.startFlow(NoOpFlow( terminateUponSignal = terminationSignal))
            mockNet.waitQuiescent() // current thread needs to wait fiber running on a different thread, has reached the blocking point
            assertEquals(0, aliceFlowMonitor.waitingFlowDurations(Duration.ZERO).toSet().size)
            // "take a long time" flow continues ...
            terminationSignal.release()
            assertEquals(0, aliceFlowMonitor.waitingFlowDurations(Duration.ZERO).toSet().size)
        }
    }

    private fun monitorFlows(script: (FlowMonitor, FlowMonitor) -> Unit) {
        val clock = Clock.systemUTC()
        script(
            FlowMonitor(FlowOperator(aliceNode.smm, clock), Duration.ZERO, Duration.ZERO),
            FlowMonitor(FlowOperator(bobNode.smm, clock), Duration.ZERO, Duration.ZERO)
        )
    }

    @Test(timeout = 300_000)
    fun `flow status is updated in database when flow suspends on ioRequest`() {
        val terminationSignal = Semaphore(0)
        bobNode.registerCordappFlowFactory(ReceiveFlow::class) { NoOpFlow( terminateUponSignal = terminationSignal) }
        val flowId = aliceNode.services.startFlow(ReceiveFlow(bob)).id
        mockNet.runNetwork()
        aliceNode.database.transaction {
            val checkpoint = dbCheckpointStorage.getCheckpoint(flowId)
            assertEquals(FlowIORequest.Receive::class.java.simpleName, checkpoint?.flowIoRequest)
        }
        terminationSignal.release()
    }

    @Test(timeout=300_000)
	fun `receiving unexpected session end before entering sendAndReceive`() {
        bobNode.registerCordappFlowFactory(WaitForOtherSideEndBeforeSendAndReceive::class) { NoOpFlow() }
        val sessionEndReceived = Semaphore(0)
        receivedSessionMessagesObservable().filter {
            it.message is ExistingSessionMessage && it.message.payload === EndSessionMessage
        }.subscribe { sessionEndReceived.release() }
        val resultFuture = aliceNode.services.startFlow(
            WaitForOtherSideEndBeforeSendAndReceive(bob, sessionEndReceived)
        ).resultFuture
        mockNet.runNetwork()
        assertThatExceptionOfType(UnexpectedFlowEndException::class.java).isThrownBy {
            resultFuture.getOrThrow()
        }
    }

    @Test(timeout=300_000)
	fun `FlowException thrown on other side`() {
        val erroringFlow = bobNode.registerCordappFlowFactory(ReceiveFlow::class) {
            ExceptionFlow { MyFlowException("Nothing useful") }
        }
        val erroringFlowSteps = erroringFlow.flatMap { it.progressSteps }

        val receivingFiber = aliceNode.services.startFlow(ReceiveFlow(bob)) as FlowStateMachineImpl

        mockNet.runNetwork()

        assertThatExceptionOfType(MyFlowException::class.java)
            .isThrownBy { receivingFiber.resultFuture.getOrThrow() }
            .withMessage("Nothing useful")
            .withStackTraceContaining(ReceiveFlow::class.java.name)  // Make sure the stack trace is that of the receiving flow
            .withStackTraceContaining("Received counter-flow exception from peer ${bob.name}")
        bobNode.database.transaction {
            assertThat(bobNode.internals.checkpointStorage.checkpoints()).isEmpty()
        }

        assertThat(receivingFiber.state).isEqualTo(Strand.State.WAITING)
        assertThat((erroringFlow.get().stateMachine as FlowStateMachineImpl).state).isEqualTo(Strand.State.WAITING)
        assertThat(erroringFlowSteps.get()).containsExactly(
            Notification.createOnNext(ProgressTracker.STARTING),
            Notification.createOnNext(ExceptionFlow.START_STEP),
            Notification.createOnError(erroringFlow.get().exceptionThrown)
        )

        assertSessionTransfers(
            aliceNode sent sessionInit(ReceiveFlow::class) to bobNode,
            bobNode sent sessionConfirm() to aliceNode,
            bobNode sent errorMessage(erroringFlow.get().exceptionThrown) to aliceNode
        )
        // Make sure the original stack trace isn't sent down the wire
        val lastMessage = receivedSessionMessages.last().message as ExistingSessionMessage
        assertThat((lastMessage.payload as ErrorSessionMessage).flowException!!.stackTrace).isEmpty()
    }

    @Test(timeout=300_000)
	fun `sub-class of FlowException can have a peer field without causing serialisation problems`() {
        val exception = MyPeerFlowException("Nothing useful", alice)
        bobNode.registerCordappFlowFactory(ReceiveFlow::class) {
            ExceptionFlow { exception }
        }

        val receivingFiber = aliceNode.services.startFlow(ReceiveFlow(bob)) as FlowStateMachineImpl

        mockNet.runNetwork()

        assertThatExceptionOfType(MyPeerFlowException::class.java)
            .isThrownBy { receivingFiber.resultFuture.getOrThrow() }
            .has(Condition(Predicate<MyPeerFlowException> { it.peer == alice }, "subclassed peer field has original value"))
            .has(Condition(Predicate<MyPeerFlowException> {
                DeclaredField<Party?>(
                    FlowException::class.java,
                    "peer",
                    it
                ).value == bob
            }, "FlowException's private peer field has value set"))
    }

    //We should update this test when we do the work to persists the flow result.
    @Test(timeout = 300_000)
    fun `Checkpoint and all its related records are deleted when the flow finishes`() {
        val terminationSignal = Semaphore(0)
        val flow = aliceNode.services.startFlow(NoOpFlow( terminateUponSignal = terminationSignal))
        mockNet.waitQuiescent() // current thread needs to wait fiber running on a different thread, has reached the blocking point
        aliceNode.database.transaction {
            val checkpoint = dbCheckpointStorage.getCheckpoint(flow.id)
            assertNull(checkpoint!!.result)
            assertNotNull(checkpoint.serializedFlowState)
            assertNotEquals(Checkpoint.FlowStatus.COMPLETED, checkpoint.status)
        }
        terminationSignal.release()
        mockNet.waitQuiescent()
        aliceNode.database.transaction {
            val checkpoint = dbCheckpointStorage.getCheckpoint(flow.id)
            assertNull(checkpoint)
            assertEquals(0, findRecordsFromDatabase<DBCheckpointStorage.DBFlowMetadata>().size)
            assertEquals(0, findRecordsFromDatabase<DBCheckpointStorage.DBFlowCheckpointBlob>().size)
            assertEquals(0, findRecordsFromDatabase<DBCheckpointStorage.DBFlowCheckpoint>().size)
        }
    }

    @Test(timeout = 300_000)
    fun `Flow metadata finish time is set in database when the flow finishes`() {
        Assume.assumeTrue(!IS_OPENJ9)
        val terminationSignal = Semaphore(0)
        val clientId = UUID.randomUUID().toString()
        val flow = aliceNode.services.startFlowWithClientId(clientId, NoOpFlow(terminateUponSignal = terminationSignal))
        mockNet.waitQuiescent()
        aliceNode.database.transaction {
            val metadata = session.find(DBCheckpointStorage.DBFlowMetadata::class.java, flow.id.uuid.toString())
            assertNull(metadata.finishInstant)
        }
        terminationSignal.release()
        mockNet.waitQuiescent()
        aliceNode.database.transaction {
            val metadata = session.find(DBCheckpointStorage.DBFlowMetadata::class.java, flow.id.uuid.toString())
            assertNotNull(metadata.finishInstant)
            assertTrue(metadata.finishInstant!!.truncatedTo(ChronoUnit.MILLIS) >= metadata.startInstant.truncatedTo(ChronoUnit.MILLIS))
        }
    }

    @Test(timeout = 300_000)
    fun `Flow persists progress tracker in the database when the flow suspends`() {
        bobNode.registerCordappFlowFactory(ReceiveFlow::class) { InitiatedReceiveFlow(it) }
        val aliceFlowId = aliceNode.services.startFlow(ReceiveFlow(bob)).id
        mockNet.runNetwork()
        aliceNode.database.transaction {
            val checkpoint = aliceNode.internals.checkpointStorage.getCheckpoint(aliceFlowId)
            assertEquals(ReceiveFlow.START_STEP.label, checkpoint!!.progressStep)
        }
        bobNode.database.transaction {
            val checkpoints = bobNode.internals.checkpointStorage.checkpoints().single()
            assertEquals(InitiatedReceiveFlow.START_STEP.label, checkpoints.progressStep)
        }
    }

    private class ConditionalExceptionFlow(val otherPartySession: FlowSession, val sendPayload: Any) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            val throwException = otherPartySession.receive<Boolean>().unwrap { it }
            if (throwException) {
                throw MyFlowException("Throwing exception as requested")
            }
            otherPartySession.send(sendPayload)
        }
    }

    @Test(timeout=300_000)
	fun `retry subFlow due to receiving FlowException`() {
        @InitiatingFlow
        class AskForExceptionFlow(val otherParty: Party, val throwException: Boolean) : FlowLogic<String>() {
            @Suspendable
            override fun call(): String = initiateFlow(otherParty).sendAndReceive<String>(throwException).unwrap { it }
        }

        class RetryOnExceptionFlow(val otherParty: Party) : FlowLogic<String>() {
            @Suspendable
            override fun call(): String {
                return try {
                    subFlow(AskForExceptionFlow(otherParty, throwException = true))
                } catch (e: MyFlowException) {
                    subFlow(AskForExceptionFlow(otherParty, throwException = false))
                }
            }
        }

        bobNode.registerCordappFlowFactory(AskForExceptionFlow::class) { ConditionalExceptionFlow(it, "Hello") }
        val resultFuture = aliceNode.services.startFlow(RetryOnExceptionFlow(bob)).resultFuture
        mockNet.runNetwork()
        assertThat(resultFuture.getOrThrow()).isEqualTo("Hello")
    }

    @Test(timeout=300_000)
	fun `serialisation issue in counterparty`() {
        bobNode.registerCordappFlowFactory(ReceiveFlow::class) { InitiatedSendFlow(NonSerialisableData(1), it) }
        val result = aliceNode.services.startFlow(ReceiveFlow(bob)).resultFuture
        mockNet.runNetwork()
        assertThatExceptionOfType(UnexpectedFlowEndException::class.java).isThrownBy {
            result.getOrThrow()
        }
    }

    @Test(timeout=300_000)
	fun `FlowException has non-serialisable object`() {
        bobNode.registerCordappFlowFactory(ReceiveFlow::class) {
            ExceptionFlow { NonSerialisableFlowException(NonSerialisableData(1)) }
        }
        val result = aliceNode.services.startFlow(ReceiveFlow(bob)).resultFuture
        mockNet.runNetwork()
        assertThatExceptionOfType(FlowException::class.java).isThrownBy {
            result.getOrThrow()
        }
    }

    @Test(timeout=300_000)
	fun waitForLedgerCommit() {
        val ptx = TransactionBuilder(notary = notaryIdentity)
            .addOutputState(DummyState(), DummyContract.PROGRAM_ID)
            .addCommand(dummyCommand(alice.owningKey))
        val stx = aliceNode.services.signInitialTransaction(ptx)

        val committerStx = aliceNode.registerCordappFlowFactory(CommitterFlow::class) {
            CommitReceiverFlow(it, stx.id)
        }.flatMap { it.stateMachine.resultFuture }
        // The waitForLedgerCommit call has to occur on separate flow
        val waiterStx = bobNode.services.startFlow(WaitForLedgerCommitFlow(stx.id)).resultFuture
        val commitReceiverStx = bobNode.services.startFlow(CommitterFlow(stx, alice)).resultFuture
        mockNet.runNetwork()
        assertThat(committerStx.getOrThrow()).isEqualTo(waiterStx.getOrThrow()).isEqualTo(commitReceiverStx.getOrThrow())
    }

    @Test(timeout=300_000)
	fun `waitForLedgerCommit throws exception if any active session ends in error`() {
        val ptx = TransactionBuilder(notary = notaryIdentity)
            .addOutputState(DummyState(), DummyContract.PROGRAM_ID)
            .addCommand(dummyCommand())
        val stx = aliceNode.services.signInitialTransaction(ptx)

        aliceNode.registerCordappFlowFactory(WaitForLedgerCommitFlow::class) { ExceptionFlow { throw Exception("Error") } }
        val waiter = bobNode.services.startFlow(WaitForLedgerCommitFlow(stx.id, alice)).resultFuture
        mockNet.runNetwork()
        assertThatExceptionOfType(UnexpectedFlowEndException::class.java).isThrownBy {
            waiter.getOrThrow()
        }
    }

    @Test(timeout=300_000)
	fun `verify vault query service is tokenizable by force checkpointing within a flow`() {
        aliceNode.registerCordappFlowFactory(VaultQueryFlow::class) { InitiatedSendFlow("Hello", it) }
        val result = bobNode.services.startFlow(VaultQueryFlow(alice)).resultFuture
        mockNet.runNetwork()
        result.getOrThrow()
    }

    @Test(timeout=300_000)
	fun `customised client flow`() {
        val receiveFlowFuture = bobNode.registerCordappFlowFactory(SendFlow::class) { InitiatedReceiveFlow(it) }
        aliceNode.services.startFlow(CustomSendFlow("Hello", bob)).resultFuture
        mockNet.runNetwork()
        assertThat(receiveFlowFuture.getOrThrow().receivedPayloads).containsOnly("Hello")
    }

    @Test(timeout=300_000)
	fun `customised client flow which has annotated @InitiatingFlow again`() {
        assertThatExceptionOfType(IllegalArgumentException::class.java).isThrownBy {
            aliceNode.services.startFlow(IncorrectCustomSendFlow("Hello", bob)).resultFuture
        }.withMessageContaining(InitiatingFlow::class.java.simpleName)
    }

    @Test(timeout=300_000)
	fun `upgraded initiating flow`() {
        bobNode.registerCordappFlowFactory(UpgradedFlow::class, initiatedFlowVersion = 1) { InitiatedSendFlow("Old initiated", it) }
        val result = aliceNode.services.startFlow(UpgradedFlow(bob)).resultFuture
        mockNet.runNetwork()
        assertThat(receivedSessionMessages).startsWith(
            aliceNode sent sessionInit(UpgradedFlow::class, flowVersion = 2) to bobNode,
            bobNode sent sessionConfirm(flowVersion = 1) to aliceNode
        )
        val (receivedPayload, node2FlowVersion) = result.getOrThrow()
        assertThat(receivedPayload).isEqualTo("Old initiated")
        assertThat(node2FlowVersion).isEqualTo(1)
    }

    @Test(timeout=300_000)
	fun `upgraded initiated flow`() {
        bobNode.registerCordappFlowFactory(SendFlow::class, initiatedFlowVersion = 2) { UpgradedFlow(it) }
        val initiatingFlow = SendFlow("Old initiating", bob)
        val flowInfo = aliceNode.services.startFlow(initiatingFlow).resultFuture
        mockNet.runNetwork()
        assertThat(receivedSessionMessages).startsWith(
            aliceNode sent sessionInit(SendFlow::class, flowVersion = 1, payload = "Old initiating") to bobNode,
            bobNode sent sessionConfirm(flowVersion = 2) to aliceNode
        )
        assertThat(flowInfo.get().flowVersion).isEqualTo(2)
    }

    @Test(timeout=300_000)
	fun `unregistered flow`() {
        val future = aliceNode.services.startFlow(NeverRegisteredFlow("Hello", bob)).resultFuture
        mockNet.runNetwork()
        assertThatExceptionOfType(UnexpectedFlowEndException::class.java)
            .isThrownBy { future.getOrThrow() }
            .withMessageEndingWith("${NeverRegisteredFlow::class.java.name} is not registered")
    }

    @Test(timeout=300_000)
	fun `session init with unknown class is sent to the flow hospital, from where we then drop it`() {
        aliceNode.sendSessionMessage(InitialSessionMessage(SessionId(random63BitValue()), 0, "not.a.real.Class", 1, "", null), bob)
        mockNet.runNetwork()
        assertThat(receivedSessionMessages).hasSize(1) // Only the session-init is expected as the session-reject is blocked by the flow hospital
        val medicalRecords = bobNode.smm.flowHospital.track().apply { updates.notUsed() }.snapshot
        assertThat(medicalRecords).hasSize(1)
        val sessionInitRecord = medicalRecords[0] as StaffedFlowHospital.MedicalRecord.SessionInit
        assertThat(sessionInitRecord.initiatorFlowClassName).isEqualTo("not.a.real.Class")
        bobNode.smm.flowHospital.dropSessionInit(sessionInitRecord.id)  // Drop the message which is processed as an error back to sender
        mockNet.runNetwork()
        assertThat(receivedSessionMessages).hasSize(2) // Now the session-reject is expected
        val lastMessage = receivedSessionMessages.last().message as ExistingSessionMessage
        assertThat((lastMessage.payload as RejectSessionMessage).message).isEqualTo("Don't know not.a.real.Class")
    }

    @Test(timeout=300_000)
	fun `non-flow class in session init`() {
        aliceNode.sendSessionMessage(InitialSessionMessage(SessionId(random63BitValue()), 0, String::class.java.name, 1, "", null), bob)
        mockNet.runNetwork()
        assertThat(receivedSessionMessages).hasSize(2) // Only the session-init and session-reject are expected
        val lastMessage = receivedSessionMessages.last().message as ExistingSessionMessage
        assertThat((lastMessage.payload as RejectSessionMessage).message).isEqualTo("${String::class.java.name} is not a flow")
    }

    @Test(timeout=300_000)
	fun `single inlined sub-flow`() {
        bobNode.registerCordappFlowFactory(SendAndReceiveFlow::class) { SingleInlinedSubFlow(it) }
        val result = aliceNode.services.startFlow(SendAndReceiveFlow(bob, "Hello")).resultFuture
        mockNet.runNetwork()
        assertThat(result.getOrThrow()).isEqualTo("HelloHello")
    }

    @Test(timeout=300_000)
	fun `double inlined sub-flow`() {
        bobNode.registerCordappFlowFactory(SendAndReceiveFlow::class) { DoubleInlinedSubFlow(it) }
        val result = aliceNode.services.startFlow(SendAndReceiveFlow(bob, "Hello")).resultFuture
        mockNet.runNetwork()
        assertThat(result.getOrThrow()).isEqualTo("HelloHello")
    }

    @Test(timeout=300_000)
	fun `non-FlowException thrown on other side`() {
        val erroringFlowFuture = bobNode.registerCordappFlowFactory(ReceiveFlow::class) {
            ExceptionFlow { Exception("evil bug!") }
        }
        val erroringFlowSteps = erroringFlowFuture.flatMap { it.progressSteps }

        val receiveFlow = ReceiveFlow(bob)
        val receiveFlowSteps = receiveFlow.progressSteps
        val receiveFlowResult = aliceNode.services.startFlow(receiveFlow).resultFuture

        mockNet.runNetwork()

        erroringFlowFuture.getOrThrow()
        val flowSteps = erroringFlowSteps.get()
        assertThat(flowSteps).containsExactly(
            Notification.createOnNext(ProgressTracker.STARTING),
            Notification.createOnNext(ExceptionFlow.START_STEP),
            Notification.createOnError(erroringFlowFuture.get().exceptionThrown)
        )

        val receiveFlowException = assertFailsWith(UnexpectedFlowEndException::class) {
            receiveFlowResult.getOrThrow()
        }
        assertThat(receiveFlowException.message).doesNotContain("evil bug!")
        assertThat(receiveFlowSteps.get()).containsExactly(
            Notification.createOnNext(ProgressTracker.STARTING),
            Notification.createOnNext(ReceiveFlow.START_STEP),
            Notification.createOnError(receiveFlowException)
        )
        assertThat(receiveFlowException).hasStackTraceContaining("Received unexpected counter-flow exception from peer ${bob.name}")

        assertSessionTransfers(
            aliceNode sent sessionInit(ReceiveFlow::class) to bobNode,
            bobNode sent sessionConfirm() to aliceNode,
            bobNode sent errorMessage() to aliceNode
        )
    }

    @Test(timeout=300_000)
	fun `initiating flow using unknown AnonymousParty`() {
        val anonymousBob = bobNode.services.keyManagementService.freshKeyAndCert(bobNode.info.legalIdentitiesAndCerts.single(), false)
            .party.anonymise()
        bobNode.registerCordappFlowFactory(SendAndReceiveFlow::class) { SingleInlinedSubFlow(it) }
        val result = aliceNode.services.startFlow(SendAndReceiveFlow(anonymousBob, "Hello")).resultFuture
        mockNet.runNetwork()
        assertThatIllegalArgumentException()
            .isThrownBy { result.getOrThrow() }
            .withMessage("Could not resolve destination: $anonymousBob")
    }

    @Test(timeout=300_000)
	fun `initiating flow using known AnonymousParty`() {
        val anonymousBob = bobNode.services.keyManagementService.freshKeyAndCert(bobNode.info.legalIdentitiesAndCerts.single(), false)
        aliceNode.services.identityService.verifyAndRegisterIdentity(anonymousBob)
        val bobResponderFlow = bobNode.registerCordappFlowFactory(SendAndReceiveFlow::class) { SingleInlinedSubFlow(it) }
        val result = aliceNode.services.startFlow(SendAndReceiveFlow(anonymousBob.party.anonymise(), "Hello")).resultFuture
        mockNet.runNetwork()
        bobResponderFlow.getOrThrow()
        assertThat(result.getOrThrow()).isEqualTo("HelloHello")
    }

    @Test(timeout=300_000)
    fun `initiating flow with anonymous party at the same node`() {
        val anonymousBob = bobNode.services.keyManagementService.freshKeyAndCert(bobNode.info.legalIdentitiesAndCerts.single(), false)
        val bobResponderFlow = bobNode.registerCordappFlowFactory(SendAndReceiveFlow::class) { SingleInlinedSubFlow(it) }
        val result = bobNode.services.startFlow(SendAndReceiveFlow(anonymousBob.party.anonymise(), "Hello")).resultFuture
        mockNet.runNetwork()
        bobResponderFlow.getOrThrow()
        assertThat(result.getOrThrow()).isEqualTo("HelloHello")
    }

    @Test(timeout=300_000)
    fun `Checkpoint status changes to RUNNABLE when flow is loaded from checkpoint - FlowState Unstarted`() {
        var firstExecution = true
        var flowState: FlowState? = null
        var dbCheckpointStatusBeforeSuspension: Checkpoint.FlowStatus? = null
        var dbCheckpointStatusAfterSuspension: Checkpoint.FlowStatus? = null
        var inMemoryCheckpointStatusBeforeSuspension: Checkpoint.FlowStatus? = null
        val futureFiber = openFuture<FlowStateMachineImpl<*>>().toCompletableFuture()

        SuspendingFlow.hookBeforeCheckpoint = {
            val flowFiber = this as? FlowStateMachineImpl<*>
            flowState = flowFiber!!.transientState.checkpoint.flowState

            if (firstExecution) {
                firstExecution = false
                throw HospitalizeFlowException()
            } else {
                dbCheckpointStatusBeforeSuspension = aliceNode.internals.checkpointStorage.getCheckpoints().toList().single().second.status
                currentDBSession().clear() // clear session as Hibernate with fails with 'org.hibernate.NonUniqueObjectException' once it tries to save a DBFlowCheckpoint upon checkpoint
                inMemoryCheckpointStatusBeforeSuspension = flowFiber.transientState.checkpoint.status

                futureFiber.complete(flowFiber)
            }
        }
        SuspendingFlow.hookAfterCheckpoint = {
            dbCheckpointStatusAfterSuspension = aliceNode.internals.checkpointStorage.getCheckpointsToRun().toList().single()
                    .second.status
        }

        assertFailsWith<TimeoutException> {
            aliceNode.services.startFlow(SuspendingFlow()).resultFuture.getOrThrow(10.seconds) // wait till flow gets hospitalized
        }
        // flow is in hospital
        assertTrue(flowState is FlowState.Unstarted)
        val inMemoryHospitalizedCheckpointStatus = aliceNode.internals.smm.snapshot().first().transientState.checkpoint.status
        assertEquals(Checkpoint.FlowStatus.HOSPITALIZED, inMemoryHospitalizedCheckpointStatus)
        aliceNode.database.transaction {
            val checkpoint = aliceNode.internals.checkpointStorage.getCheckpoints().toList().single().second
            assertEquals(Checkpoint.FlowStatus.HOSPITALIZED, checkpoint.status)
        }
        // restart Node - flow will be loaded from checkpoint
        aliceNode = mockNet.restartNode(aliceNode)
        futureFiber.get().resultFuture.getOrThrow() // wait until the flow has completed
        // checkpoint states ,after flow retried, before and after suspension
        assertEquals(Checkpoint.FlowStatus.RUNNABLE, dbCheckpointStatusBeforeSuspension)
        assertEquals(Checkpoint.FlowStatus.RUNNABLE, inMemoryCheckpointStatusBeforeSuspension)
        assertEquals(Checkpoint.FlowStatus.RUNNABLE, dbCheckpointStatusAfterSuspension)
    }

    @Test(timeout=300_000)
    fun `Checkpoint status changes to RUNNABLE when flow is loaded from checkpoint - FlowState Started`() {
        var firstExecution = true
        var flowState: FlowState? = null
        var dbCheckpointStatus: Checkpoint.FlowStatus? = null
        var inMemoryCheckpointStatus: Checkpoint.FlowStatus? = null
        val futureFiber = openFuture<FlowStateMachineImpl<*>>().toCompletableFuture()

        SuspendingFlow.hookAfterCheckpoint = {
            val flowFiber = this as? FlowStateMachineImpl<*>
            flowState = flowFiber!!.transientState.checkpoint.flowState

            if (firstExecution) {
                firstExecution = false
                throw HospitalizeFlowException()
            } else {
                dbCheckpointStatus = aliceNode.internals.checkpointStorage.getCheckpoints().toList().single().second.status
                inMemoryCheckpointStatus = flowFiber.transientState.checkpoint.status

                futureFiber.complete(flowFiber)
            }
        }

        assertFailsWith<TimeoutException> {
            aliceNode.services.startFlow(SuspendingFlow()).resultFuture.getOrThrow(10.seconds) // wait till flow gets hospitalized
        }
        // flow is in hospital
        assertTrue(flowState is FlowState.Started)
        aliceNode.database.transaction {
            val checkpoint = aliceNode.internals.checkpointStorage.getCheckpoints().toList().single().second
            assertEquals(Checkpoint.FlowStatus.HOSPITALIZED, checkpoint.status)
        }
        // restart Node - flow will be loaded from checkpoint
        aliceNode = mockNet.restartNode(aliceNode)
        futureFiber.get().resultFuture.getOrThrow() // wait until the flow has completed
        // checkpoint states ,after flow retried, after suspension
        assertEquals(Checkpoint.FlowStatus.RUNNABLE, dbCheckpointStatus)
        assertEquals(Checkpoint.FlowStatus.RUNNABLE, inMemoryCheckpointStatus)
    }

    @Test(timeout=300_000)
    fun `Checkpoint is updated in DB with HOSPITALIZED status and the error when flow is kept for overnight observation` () {
        var flowId: StateMachineRunId? = null

        assertFailsWith<TimeoutException> {
            val fiber = aliceNode.services.startFlow(ExceptionFlow { HospitalizeFlowException("Overnight observation") })
            flowId = fiber.id
            fiber.resultFuture.getOrThrow(10.seconds)
        }

        aliceNode.database.transaction {
            val checkpoint = aliceNode.internals.checkpointStorage.checkpoints().single()
            assertEquals(Checkpoint.FlowStatus.HOSPITALIZED, checkpoint.status)

            // assert all fields of DBFlowException
            val exceptionDetails = aliceNode.internals.checkpointStorage.getDBCheckpoint(flowId!!)!!.exceptionDetails
            assertEquals(HospitalizeFlowException::class.java.name, exceptionDetails!!.type)
            assertEquals("Overnight observation", exceptionDetails.message)
            val deserializedException = exceptionDetails.value?.let { SerializedBytes<Any>(it) }?.deserialize(context = SerializationDefaults.STORAGE_CONTEXT)
            assertNotNull(deserializedException)
            val hospitalizeFlowException = deserializedException as HospitalizeFlowException
            assertEquals("Overnight observation", hospitalizeFlowException.message)
        }
    }

    @Test(timeout=300_000)
    fun `Checkpoint status and error in memory and in DB are not dirtied upon flow retry`() {
        var firstExecution = true
        var dbCheckpointStatus: Checkpoint.FlowStatus? = null
        var inMemoryCheckpointStatus: Checkpoint.FlowStatus? = null
        var persistedException: DBCheckpointStorage.DBFlowException? = null

        SuspendingFlow.hookAfterCheckpoint = {
            if (firstExecution) {
                firstExecution = false
                throw SQLTransientConnectionException("connection is not available")
            } else {
                val flowFiber = this as? FlowStateMachineImpl<*>
                dbCheckpointStatus = aliceNode.internals.checkpointStorage.getCheckpoints().toList().single().second.status
                inMemoryCheckpointStatus = flowFiber!!.transientState.checkpoint.status
                persistedException = aliceNode.internals.checkpointStorage.getDBCheckpoint(flowFiber.id)!!.exceptionDetails
            }
        }

        aliceNode.services.startFlow(SuspendingFlow()).resultFuture.getOrThrow()
        // checkpoint states ,after flow retried, after suspension
        assertEquals(Checkpoint.FlowStatus.RUNNABLE, dbCheckpointStatus)
        assertEquals(Checkpoint.FlowStatus.RUNNABLE, inMemoryCheckpointStatus)
        assertEquals(null, persistedException)
    }

    // When ported to ENT use the existing API there to properly retry the flow
    @Test(timeout=300_000)
    fun `Hospitalized flow, resets to 'RUNNABLE' and clears exception when retried`() {
        var firstRun = true
        var counter = 0
        val waitUntilHospitalizedTwice = Semaphore(-1)

        StaffedFlowHospital.onFlowKeptForOvernightObservation.add { _, _ ->
            ++counter
            if (firstRun) {
                firstRun = false
                val fiber = FlowStateMachineImpl.currentStateMachine()!!
                thread {
                    // schedule a [RetryFlowFromSafePoint] after the [OvernightObservation] gets scheduled by the hospital
                    Thread.sleep(2000)
                    fiber.scheduleEvent(Event.RetryFlowFromSafePoint)
                }
            }
            waitUntilHospitalizedTwice.release()
        }

        var counterRes = 0
        StaffedFlowHospital.onFlowResuscitated.add { _, _, _ -> ++counterRes }

        aliceNode.services.startFlow(ExceptionFlow { HospitalizeFlowException("hospitalizing") })

        waitUntilHospitalizedTwice.acquire()
        assertEquals(2, counter)
        assertEquals(0, counterRes)
    }

    @Test(timeout=300_000)
    fun `Hospitalized flow, resets to 'RUNNABLE' and clears database exception on node start`() {
        var checkpointStatusAfterRestart: Checkpoint.FlowStatus? = null
        var dbExceptionAfterRestart: List<DBCheckpointStorage.DBFlowException>? = null

        var secondRun = false
        SuspendingFlow.hookBeforeCheckpoint = {
            if(secondRun) {
                aliceNode.database.transaction {
                    checkpointStatusAfterRestart = findRecordsFromDatabase<DBCheckpointStorage.DBFlowCheckpoint>().single().status
                    dbExceptionAfterRestart = findRecordsFromDatabase()
                }
            } else {
                secondRun = true
            }

            throw HospitalizeFlowException("hospitalizing")
        }

        var counter = 0
        val waitUntilHospitalized = Semaphore(0)
        StaffedFlowHospital.onFlowKeptForOvernightObservation.add { _, _ ->
            ++counter
            waitUntilHospitalized.release()
        }

        var counterRes = 0
        StaffedFlowHospital.onFlowResuscitated.add { _, _, _ -> ++counterRes }

        aliceNode.services.startFlow(SuspendingFlow())

        waitUntilHospitalized.acquire()
        Thread.sleep(3000) // wait until flow saves overnight observation state in database
        aliceNode = mockNet.restartNode(aliceNode)

        waitUntilHospitalized.acquire()
        Thread.sleep(3000) // wait until flow saves overnight observation state in database
        assertEquals(2, counter)
        assertEquals(0, counterRes)
        assertEquals(Checkpoint.FlowStatus.RUNNABLE, checkpointStatusAfterRestart)
        assertEquals(0, dbExceptionAfterRestart!!.size)
    }

    @Test(timeout=300_000)
    fun `flows waiting on ExecuteAsyncOperation persist flow async operations additional info`() {
        val txHash = SecureHash.randomSHA256()
        val flowHandle1 = aliceNode.services.startFlow(WaitForLedgerCommitFlow(txHash))
        val flowHandle2 = aliceNode.services.startFlow(ExternalOperationFlow(async = false))
        val flowHandle3 = aliceNode.services.startFlow(ExternalOperationFlow(async = true))

        Thread.sleep(2000) // wait until all flows have suspended on their [FlowIORequest.ExecuteAsyncOperation]s

        aliceNode.database.transaction {
            val checkpoint1 = aliceNode.internals.checkpointStorage.getCheckpoint(flowHandle1.id)
            val checkpoint2 = aliceNode.internals.checkpointStorage.getCheckpoint(flowHandle2.id)
            val checkpoint3 = aliceNode.internals.checkpointStorage.getCheckpoint(flowHandle3.id)
            assertEquals(
                "ExecuteAsyncOperation(WaitForLedgerCommit(hash=$txHash))",
                checkpoint1!!.flowIoRequest
            )
            assertEquals("ExecuteAsyncOperation(external operation)", checkpoint2!!.flowIoRequest)
            assertEquals("ExecuteAsyncOperation(external async operation)", checkpoint3!!.flowIoRequest)
        }
    }

    @Test(timeout=300_000)
    fun `event AsyncOperationCompletion after event Error will not resume the flow`() {
        val ptx = TransactionBuilder(notary = notaryIdentity)
                .addOutputState(DummyState(), DummyContract.PROGRAM_ID)
                .addCommand(dummyCommand(alice.owningKey))
        val stx = aliceNode.services.signInitialTransaction(ptx)

        val fiberFuture = CompletableFuture<FlowStateMachineImpl<*>>()
        WaitForLedgerCommitFlow.preWaitForLedgerCommitHook = {
            fiberFuture.complete(it)
        }

        var codeRun = false
        WaitForLedgerCommitFlow.postWaitForLedgerCommitHook = {
            // the code in this lambda should never be executed as the flow suspended on a [WaitForLedgerCommit]
            // will identify that has previously errored and will keep processing events instead of executing
            // a [TopLevelTransition.asyncOperationCompletionTransition].
            codeRun = true
        }

        val flowHandle = aliceNode.services.startFlow(WaitForLedgerCommitFlow(stx.id))

        val fiber = fiberFuture.get()
        Thread.sleep(2000) // wait until fiber suspends on 'WaitForLedgerCommit'
        // manually inject a [Event.Error] and then a [Event.AsyncOperationCompletion] to impersonate the case in which
        // a flow is waiting for ledger commit for transaction X, and then the following series of events occurs:
        // 1. an error message arrives from a counter party session
        // 2. the statemachine is waiting on a [FlowIORequest.ExecuteAsyncOperation.operation] and operation is [WaitForLedgerCommit].
        // 3. the statemachine collect and throws the error, then an [Event.Error] is created and pushed into the fiber's queue, so at this time the queue only has the [Event.Error].
        // 4. transaction X gets commited and the [DBTransactionStorage.updatesPublisher] pushes the event, which calls the callback registered by the flow, which in turn then registers a [Event.AsyncOperationCompletion],
        //      now the fibers queue may be in either of the following states: [[Event.Error], [Event.AsyncOperationCompletion]] or [[Event.AsyncOperationCompletion]].
        // 5. in this case the flow should error and NOT resume with the result contained in [Event.AsyncOperationCompletion.returnValue]
        fiber.scheduleEvent(Event.Error(IllegalStateException("counter party exception")))
        fiber.scheduleEvent(Event.AsyncOperationCompletion(stx))

        assertFailsWith<IllegalStateException> {
            flowHandle.resultFuture.getOrThrow()
        }
        assertFalse(codeRun)
    }
        //region Helpers

    private val normalEnd = ExistingSessionMessage(SessionId(0), EndSessionMessage) // NormalSessionEnd(0)

    private fun assertSessionTransfers(vararg expected: SessionTransfer) {
        assertThat(receivedSessionMessages).containsExactly(*expected)
    }

    private val FlowLogic<*>.progressSteps: CordaFuture<List<Notification<ProgressTracker.Step>>>
        get() {
            return progressTracker!!.changes
                .ofType(Change.Position::class.java)
                .map { it.newStep }
                .materialize()
                .toList()
                .toFuture()
        }

    @InitiatingFlow
    private class WaitForOtherSideEndBeforeSendAndReceive(
        val otherParty: Party,
        @Transient val receivedOtherFlowEnd: Semaphore
    ) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            // Kick off the flow on the other side ...
            val session = initiateFlow(otherParty)
            session.send(1)
            // ... then pause this one until it's received the session-end message from the other side
            receivedOtherFlowEnd.acquire()

            session.sendAndReceive<Int>(2)
        }
    }

    // we need brand new class for a flow to fail, so here it is
    @InitiatingFlow
    private open class NeverRegisteredFlow(val payload: Any, vararg val otherParties: Party) : FlowLogic<FlowInfo>() {
        init {
            require(otherParties.isNotEmpty())
        }

        @Suspendable
        override fun call(): FlowInfo {
            val flowInfos = otherParties.map {
                val session = initiateFlow(it)
                session.send(payload)
                session.getCounterpartyFlowInfo()
            }.toList()
            return flowInfos.first()
        }
    }

    @InitiatingFlow
    class WaitForLedgerCommitFlow(private val txId: SecureHash, private val party: Party? = null) : FlowLogic<SignedTransaction>() {

        companion object {
            var preWaitForLedgerCommitHook: ((FlowStateMachineImpl<*>) -> Unit)? = null
            var postWaitForLedgerCommitHook: (() -> Unit)? = null
        }

        @Suspendable
        override fun call(): SignedTransaction {
            if (party != null) {
                initiateFlow(party).send(Unit)
            }

            preWaitForLedgerCommitHook?.invoke(this.stateMachine as FlowStateMachineImpl<*>)
            val stx = waitForLedgerCommit(txId)
            postWaitForLedgerCommitHook?.invoke()
            return stx
        }
    }

    @InitiatingFlow
    class CommitterFlow(private val stx: SignedTransaction, private val otherParty: Party) : FlowLogic<SignedTransaction>() {
        @Suspendable
        override fun call(): SignedTransaction {
            val session = initiateFlow(otherParty)
            return subFlow(FinalityFlow(stx, session))
        }
    }

    class CommitReceiverFlow(private val otherSide: FlowSession, private val txId: SecureHash) : FlowLogic<SignedTransaction>() {
        @Suspendable
        override fun call(): SignedTransaction = subFlow(ReceiveFinalityFlow(otherSide, expectedTxId = txId))
    }

    private class LazyServiceHubAccessFlow : FlowLogic<Unit>() {
        val lazyTime: Instant by lazy { serviceHub.clock.instant() }
        @Suspendable
        override fun call() = Unit
    }

    private interface CustomInterface

    private class CustomSendFlow(payload: String, otherParty: Party) : CustomInterface, SendFlow(payload, otherParty)

    @InitiatingFlow
    private class IncorrectCustomSendFlow(payload: String, otherParty: Party) : CustomInterface, SendFlow(payload, otherParty)

    @InitiatingFlow
    private class VaultQueryFlow(val otherParty: Party) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            val otherPartySession = initiateFlow(otherParty)
            // Hold onto reference here to force checkpoint of vaultService and thus
            // prove it is registered as a tokenizableService in the node
            val vaultQuerySvc = serviceHub.vaultService
            otherPartySession.receive<Any>()
            vaultQuerySvc.queryBy<ContractState>().states
        }
    }

    @InitiatingFlow(version = 2)
    private class UpgradedFlow(val otherParty: Party, val otherPartySession: FlowSession? = null) : FlowLogic<Pair<Any, Int>>() {
        constructor(otherPartySession: FlowSession) : this(otherPartySession.counterparty, otherPartySession)

        @Suspendable
        override fun call(): Pair<Any, Int> {
            val otherPartySession = this.otherPartySession ?: initiateFlow(otherParty)
            val received = otherPartySession.receive<Any>().unwrap { it }
            val otherFlowVersion = otherPartySession.getCounterpartyFlowInfo().flowVersion
            return Pair(received, otherFlowVersion)
        }
    }

    private class SingleInlinedSubFlow(val otherPartySession: FlowSession) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            val payload = otherPartySession.receive<String>().unwrap { it }
            subFlow(InlinedSendFlow(payload + payload, otherPartySession))
        }
    }

    private class DoubleInlinedSubFlow(val otherPartySession: FlowSession) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            subFlow(SingleInlinedSubFlow(otherPartySession))
        }
    }

    private data class NonSerialisableData(val a: Int)
    private class NonSerialisableFlowException(@Suppress("unused") val data: NonSerialisableData) : FlowException()

    private class InlinedSendFlow(val payload: String, val otherPartySession: FlowSession) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() = otherPartySession.send(payload)
    }

    //endregion Helpers
}

internal fun sessionConfirm(flowVersion: Int = 1) =
    ExistingSessionMessage(SessionId(0), ConfirmSessionMessage(SessionId(0), FlowInfo(flowVersion, "")))

internal inline fun <reified P : FlowLogic<*>> TestStartedNode.getSingleFlow(): Pair<P, CordaFuture<*>> {
    return smm.findStateMachines(P::class.java).single()
}

private fun sanitise(message: SessionMessage) = when (message) {
    is InitialSessionMessage -> message.copy(initiatorSessionId = SessionId(0), initiationEntropy = 0, appName = "")
    is ExistingSessionMessage -> {
        val payload = message.payload
        message.copy(
            recipientSessionId = SessionId(0),
            payload = when (payload) {
                is ConfirmSessionMessage -> payload.copy(
                    initiatedSessionId = SessionId(0),
                    initiatedFlowInfo = payload.initiatedFlowInfo.copy(appName = "")
                )
                is ErrorSessionMessage -> payload.copy(
                    errorId = 0
                )
                else -> payload
            }
        )
    }
}

internal fun Observable<MessageTransfer>.toSessionTransfers(): Observable<SessionTransfer> {
    return filter { it.getMessage().topic == FlowMessagingImpl.sessionTopic }.map {
        val from = it.sender.id
        val message = it.messageData.deserialize<SessionMessage>()
        SessionTransfer(from, sanitise(message), it.recipients)
    }
}

internal fun TestStartedNode.sendSessionMessage(message: SessionMessage, destination: Party) {
    services.networkService.apply {
        val address = getAddressOfParty(PartyInfo.SingleNode(destination, emptyList()))
        send(createMessage(FlowMessagingImpl.sessionTopic, message.serialize().bytes), address)
    }
}

inline fun <reified T> DatabaseTransaction.findRecordsFromDatabase(): List<T> {
    val criteria = session.criteriaBuilder.createQuery(T::class.java)
    criteria.select(criteria.from(T::class.java))
    return session.createQuery(criteria).resultList
}

internal fun errorMessage(errorResponse: FlowException? = null) =
    ExistingSessionMessage(SessionId(0), ErrorSessionMessage(errorResponse, 0))

internal infix fun TestStartedNode.sent(message: SessionMessage): Pair<Int, SessionMessage> = Pair(internals.id, message)
internal infix fun Pair<Int, SessionMessage>.to(node: TestStartedNode): SessionTransfer =
    SessionTransfer(first, second, node.network.myAddress)

internal data class SessionTransfer(val from: Int, val message: SessionMessage, val to: MessageRecipients) {
    val isPayloadTransfer: Boolean
        get() =
            message is ExistingSessionMessage && message.payload is DataSessionMessage ||
                    message is InitialSessionMessage && message.firstPayload != null

    override fun toString(): String = "$from sent $message to $to"
}

internal fun sessionInit(clientFlowClass: KClass<out FlowLogic<*>>, flowVersion: Int = 1, payload: Any? = null): InitialSessionMessage {
    return InitialSessionMessage(SessionId(0), 0, clientFlowClass.java.name, flowVersion, "", payload?.serialize())
}

internal fun sessionData(payload: Any) = ExistingSessionMessage(SessionId(0), DataSessionMessage(payload.serialize()))

@InitiatingFlow
internal open class SendFlow(private val payload: Any, private vararg val otherParties: Party) : FlowLogic<FlowInfo>() {
    init {
        require(otherParties.isNotEmpty())
    }

    @Suspendable
    override fun call(): FlowInfo {
        val flowInfos = otherParties.map {
            val session = initiateFlow(it)
            session.send(payload)
            session.getCounterpartyFlowInfo()
        }.toList()
        return flowInfos.first()
    }
}

internal class NoOpFlow(
        val nonTerminating: Boolean = false,
        @Transient val terminateUponSignal: Semaphore? = null
) : FlowLogic<Unit>() {
    @Transient
    var flowStarted = false

    @Suspendable
    override fun call() {
        flowStarted = true
        if (nonTerminating) {
            Fiber.park()
        }

        terminateUponSignal?.acquire() // block at Semaphore and resume upon external signaling
    }
}

internal class InitiatedReceiveFlow(private val otherPartySession: FlowSession) : FlowLogic<Unit>() {
    object START_STEP : ProgressTracker.Step("Starting")
    object RECEIVED_STEP : ProgressTracker.Step("Received")

    override val progressTracker: ProgressTracker = ProgressTracker(START_STEP, RECEIVED_STEP)
    private var nonTerminating: Boolean = false
    @Transient
    var receivedPayloads: List<String> = emptyList()

    @Suspendable
    override fun call() {
        progressTracker.currentStep = START_STEP
        receivedPayloads = listOf(otherPartySession.receive<String>().unwrap { it })
        progressTracker.currentStep = RECEIVED_STEP
        if (nonTerminating) {
            Fiber.park()
        }
    }

    fun nonTerminating(): InitiatedReceiveFlow {
        nonTerminating = true
        return this
    }
}

internal open class InitiatedSendFlow(private val payload: Any, private val otherPartySession: FlowSession) : FlowLogic<Unit>() {
    @Suspendable
    override fun call() = otherPartySession.send(payload)
}

@InitiatingFlow
internal class ReceiveFlow(private vararg val otherParties: Party) : FlowLogic<Unit>() {
    object START_STEP : ProgressTracker.Step("Starting")
    object RECEIVED_STEP : ProgressTracker.Step("Received")

    init {
        require(otherParties.isNotEmpty())
    }

    override val progressTracker: ProgressTracker = ProgressTracker(START_STEP, RECEIVED_STEP)
    private var nonTerminating: Boolean = false
    @Transient
    var receivedPayloads: List<String> = emptyList()

    @Suspendable
    override fun call() {
        progressTracker.currentStep = START_STEP
        receivedPayloads = otherParties.map { initiateFlow(it).receive<String>().unwrap { it } }
        progressTracker.currentStep = RECEIVED_STEP
        if (nonTerminating) {
            Fiber.park()
        }
    }

    fun nonTerminating(): ReceiveFlow {
        nonTerminating = true
        return this
    }
}

internal class MyFlowException(override val message: String) : FlowException() {
    override fun equals(other: Any?): Boolean = other is MyFlowException && other.message == this.message
    override fun hashCode(): Int = message.hashCode()
}

internal class MyPeerFlowException(override val message: String, val peer: Party) : FlowException()

@InitiatingFlow
internal class SendAndReceiveFlow(
    private val destination: Destination,
    private val payload: Any,
    private val otherPartySession: FlowSession? = null
) : FlowLogic<Any>() {
    constructor(otherPartySession: FlowSession, payload: Any) : this(otherPartySession.counterparty, payload, otherPartySession)

    @Suspendable
    override fun call(): Any {
        return (otherPartySession ?: initiateFlow(destination)).sendAndReceive<Any>(payload).unwrap { it }
    }
}

@InitiatingFlow
internal class PingPongFlow(private val otherParty: Party, private val payload: Long, private val otherPartySession: FlowSession? = null) :
    FlowLogic<Unit>() {
    constructor(otherPartySession: FlowSession, payload: Long) : this(otherPartySession.counterparty, payload, otherPartySession)

    @Transient
    var receivedPayload: Long? = null
    @Transient
    var receivedPayload2: Long? = null

    @Suspendable
    override fun call() {
        val session = otherPartySession ?: initiateFlow(otherParty)
        receivedPayload = session.sendAndReceive<Long>(payload).unwrap { it }
        receivedPayload2 = session.sendAndReceive<Long>(payload + 1).unwrap { it }
    }
}

internal class ExceptionFlow<E : Exception>(val exception: () -> E) : FlowLogic<Nothing>() {
    object START_STEP : ProgressTracker.Step("Starting")

    override val progressTracker: ProgressTracker = ProgressTracker(START_STEP)
    lateinit var exceptionThrown: E

    @Suspendable
    override fun call(): Nothing {
        progressTracker.currentStep = START_STEP
        exceptionThrown = exception()
        throw exceptionThrown
    }
}

internal class SuspendingFlow : FlowLogic<Unit>() {

    companion object {
        var hookBeforeCheckpoint: FlowStateMachine<*>.() -> Unit = {}
        var hookAfterCheckpoint: FlowStateMachine<*>.() -> Unit = {}
    }

    @Suspendable
    override fun call() {
        stateMachine.hookBeforeCheckpoint()
        stateMachine.suspend(FlowIORequest.ForceCheckpoint, maySkipCheckpoint = false) // flow checkpoints => checkpoint is in DB
        stateMachine.hookAfterCheckpoint()
    }
}

internal class ExternalOperation: FlowExternalOperation<Unit> {
    override fun execute(deduplicationId: String) {
        Thread.sleep(10000)
    }
    override fun toString() = "external operation"
}

internal class ExternalAsyncOperation: FlowExternalAsyncOperation<Unit> {
    override fun execute(deduplicationId: String): CompletableFuture<Unit> {
        Thread.sleep(10000)
        return openFuture<Unit>().toCompletableFuture()
    }
    override fun toString() = "external async operation"
}

internal class ExternalOperationFlow(private val async: Boolean) : FlowLogic<Unit>() {

    @Suspendable
    override fun call() {
        if (async) {
            await(ExternalAsyncOperation())
        } else {
            await(ExternalOperation())
        }
    }
}
