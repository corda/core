package net.corda.node.services

import co.paralleluniverse.fibers.Suspendable
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.whenever
import net.corda.core.concurrent.CordaFuture
import net.corda.core.contracts.AlwaysAcceptAttachmentConstraint
import net.corda.core.contracts.StateRef
import net.corda.core.contracts.TimeWindow
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.*
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.FlowIORequest
import net.corda.core.internal.WaitForLedgerCommit
import net.corda.core.internal.bufferUntilSubscribed
import net.corda.core.internal.concurrent.openFuture
import net.corda.core.internal.notary.NotaryServiceFlow
import net.corda.core.internal.notary.SinglePartyNotaryService
import net.corda.core.internal.notary.UniquenessProvider
import net.corda.core.node.NotaryInfo
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.ProgressTracker
import net.corda.core.utilities.minutes
import net.corda.core.utilities.seconds
import net.corda.node.services.api.ServiceHubInternal
import net.corda.node.services.config.FlowTimeoutConfiguration
import net.corda.node.services.config.NodeConfiguration
import net.corda.node.services.config.NotaryConfig
import net.corda.node.services.transactions.NonValidatingNotaryFlow
import net.corda.nodeapi.internal.DevIdentityGenerator
import net.corda.nodeapi.internal.network.NetworkParametersCopier
import net.corda.testing.common.internal.testNetworkParameters
import net.corda.testing.contracts.DummyContract
import net.corda.testing.core.dummyCommand
import net.corda.testing.core.singleIdentity
import net.corda.testing.internal.LogHelper
import net.corda.testing.node.InMemoryMessagingNetwork
import net.corda.testing.node.MockNetworkParameters
import net.corda.testing.node.internal.*
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import java.security.PublicKey
import java.time.Duration
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class TimedFlowTests {
    companion object {
        /** The notary nodes don't run any consensus protocol, so 2 nodes are sufficient for the purpose of this test. */
        private const val CLUSTER_SIZE = 2
        /** A shared counter across all notary service nodes. */
        var requestsReceived: AtomicInteger = AtomicInteger(0)

        private lateinit var mockNet: InternalMockNetwork
        private lateinit var notary: Party
        private lateinit var node: TestStartedNode
        private lateinit var patientNode: TestStartedNode

        private val waitEtaThreshold: Duration = NotaryServiceFlow.defaultEstimatedWaitTime
        private var waitETA: Duration = waitEtaThreshold

        init {
            LogHelper.setLevel("+net.corda.flow", "+net.corda.testing.node", "+net.corda.node.services.messaging")
        }

        @BeforeClass
        @JvmStatic
        fun setup() {
            mockNet = InternalMockNetwork(
                    cordappsForAllNodes = listOf(DUMMY_CONTRACTS_CORDAPP, enclosedCordapp()),
                    defaultParameters = MockNetworkParameters().withServicePeerAllocationStrategy(InMemoryMessagingNetwork.ServicePeerAllocationStrategy.RoundRobin()),
                    threadPerNode = true
            )
            val started = startClusterAndNode(mockNet)
            notary = started.first
            node = started.second
            patientNode = started.third
        }

        @AfterClass
        @JvmStatic
        fun stopNodes() {
            mockNet.stopNodes()
        }

        private fun startClusterAndNode(mockNet: InternalMockNetwork): Triple<Party, TestStartedNode, TestStartedNode> {
            val replicaIds = (0 until CLUSTER_SIZE)
            val serviceLegalName = CordaX500Name("Custom Notary", "Zurich", "CH")
            val notaryIdentity = DevIdentityGenerator.generateDistributedNotaryCompositeIdentity(
                    replicaIds.map { mockNet.baseDirectory(mockNet.nextNodeId + it) },
                    serviceLegalName)

            val networkParameters = NetworkParametersCopier(testNetworkParameters(listOf(NotaryInfo(notaryIdentity, false))))
            val notaryConfig = mock<NotaryConfig> {
                whenever(it.serviceLegalName).thenReturn(serviceLegalName)
                whenever(it.validating).thenReturn(false)
                whenever(it.className).thenReturn(TestNotaryService::class.java.name)
            }

            val notaryNodes = (0 until CLUSTER_SIZE).map {
                mockNet.createUnstartedNode(InternalMockNodeParameters(configOverrides = {
                    doReturn(notaryConfig).whenever(it).notary
                }))
            }

            val aliceNode = mockNet.createUnstartedNode(
                    InternalMockNodeParameters(
                            legalName = CordaX500Name("Alice", "AliceCorp", "GB"),
                            configOverrides = { conf: NodeConfiguration ->
                                val retryConfig = FlowTimeoutConfiguration(1.seconds, 3, 1.0)
                                doReturn(retryConfig).whenever(conf).flowTimeout
                            }
                    )
            )

            val patientNode = mockNet.createUnstartedNode(
                    InternalMockNodeParameters(
                            legalName = CordaX500Name("Bob", "BobCorp", "GB"),
                            configOverrides = { conf: NodeConfiguration ->
                                val retryConfig = FlowTimeoutConfiguration(10.seconds, 3, 1.0)
                                doReturn(retryConfig).whenever(conf).flowTimeout
                            }
                    )
            )

            // MockNetwork doesn't support notary clusters, so we create all the nodes we need unstarted, and then install the
            // network-parameters in their directories before they're started.
            val nodes = (notaryNodes + aliceNode + patientNode).map { node ->
                networkParameters.install(mockNet.baseDirectory(node.id))
                node.start()
            }

            return Triple(notaryIdentity, nodes[nodes.lastIndex - 1], nodes.last())
        }
    }

    @Before
    fun resetCounter() {
        requestsReceived = AtomicInteger(0)
    }

    @Test(timeout=300_000)
	fun `timed flows are restarted`() {
        node.run {
            val issueTx = signInitialTransaction(notary) {
                setTimeWindow(services.clock.instant(), 30.seconds)
                addOutputState(DummyContract.SingleOwnerState(owner = info.singleIdentity()), DummyContract.PROGRAM_ID, AlwaysAcceptAttachmentConstraint)
            }
            val flow = NotaryFlow.Client(issueTx)
            val progressTracker = flow.progressTracker
            assertNotEquals(ProgressTracker.DONE, progressTracker.currentStep)
            val progressTrackerDone = getDoneFuture(progressTracker)

            val notarySignatures = services.startFlow(flow).resultFuture.get()
            (issueTx + notarySignatures).verifyRequiredSignatures()
            progressTrackerDone.get()
        }
    }

    @Test(timeout=300_000)
	fun `timed sub-flows are restarted`() {
        node.run {
            val issueTx = signInitialTransaction(notary) {
                setTimeWindow(services.clock.instant(), 30.seconds)
                addOutputState(DummyContract.SingleOwnerState(owner = info.singleIdentity()), DummyContract.PROGRAM_ID, AlwaysAcceptAttachmentConstraint)
            }
            val flow = FinalityFlow(issueTx, emptyList())
            val progressTracker = flow.progressTracker
            assertNotEquals(ProgressTracker.DONE, progressTracker.currentStep)
            val progressTrackerDone = getDoneFuture(flow.progressTracker)

            val stx = services.startFlow(flow).resultFuture.get()
            stx.verifyRequiredSignatures()
            progressTrackerDone.get()
        }
    }

    @Test(timeout=300_000)
	fun `timed flow can update its ETA`() {
        try {
            waitETA = 10.minutes
            node.run {
                val issueTx = signInitialTransaction(notary) {
                    setTimeWindow(services.clock.instant(), 30.seconds)
                    addOutputState(DummyContract.SingleOwnerState(owner = info.singleIdentity()), DummyContract.PROGRAM_ID, AlwaysAcceptAttachmentConstraint)
                }
                val flow = NotaryFlow.Client(issueTx)
                val progressTracker = flow.progressTracker
                assertNotEquals(ProgressTracker.DONE, progressTracker.currentStep)
                val progressTrackerDone = getDoneFuture(progressTracker)

                val resultFuture = services.startFlow(flow).resultFuture
                var exceptionThrown = false
                try {
                    resultFuture.get(3, TimeUnit.SECONDS)
                } catch (e: TimeoutException) {
                    exceptionThrown = true
                }
                assertTrue(exceptionThrown)
                flow.stateMachine.updateTimedFlowTimeout(2)
                val notarySignatures = resultFuture.get(10, TimeUnit.SECONDS)
                (issueTx + notarySignatures).verifyRequiredSignatures()
                progressTrackerDone.get()
            }
        } finally {
            waitETA = waitEtaThreshold
        }
    }

    @Test(timeout=300_000)
	fun `timed flow cannot update its ETA to less than default`() {
        try {
            waitETA = 1.seconds
            patientNode.run {
                val issueTx = signInitialTransaction(notary) {
                    setTimeWindow(services.clock.instant(), 30.seconds)
                    addOutputState(DummyContract.SingleOwnerState(owner = info.singleIdentity()), DummyContract.PROGRAM_ID, AlwaysAcceptAttachmentConstraint)
                }
                val flow = NotaryFlow.Client(issueTx)
                val progressTracker = flow.progressTracker
                assertNotEquals(ProgressTracker.DONE, progressTracker.currentStep)
                val progressTrackerDone = getDoneFuture(progressTracker)

                val resultFuture = services.startFlow(flow).resultFuture
                flow.stateMachine.updateTimedFlowTimeout(1)
                var exceptionThrown = false
                try {
                    resultFuture.get(3, TimeUnit.SECONDS)
                } catch (e: TimeoutException) {
                    exceptionThrown = true
                }
                assertTrue(exceptionThrown)
                val notarySignatures = resultFuture.get(15, TimeUnit.SECONDS)
                (issueTx + notarySignatures).verifyRequiredSignatures()
                progressTrackerDone.get()
            }
        } finally {
            waitETA = waitEtaThreshold
        }
    }

    private fun TestStartedNode.signInitialTransaction(notary: Party, block: TransactionBuilder.() -> Any?): SignedTransaction {
        return services.signInitialTransaction(
                TransactionBuilder(notary).apply {
                    addCommand(dummyCommand(services.myInfo.singleIdentity().owningKey))
                    block()
                }
        )
    }

    /** Returns a future that completes when the [progressTracker] reaches the [ProgressTracker.DONE] step. */
    private fun getDoneFuture(progressTracker: ProgressTracker): Future<ProgressTracker.Change> {
        return progressTracker.changes.takeFirst {
            it.progressTracker.currentStep == ProgressTracker.DONE
        }.bufferUntilSubscribed().toBlocking().toFuture()
    }

    /**
     * A test notary service that will just stop forever the first time you invoke its commitInputStates method and will succeed the
     * second time around.
     */
    private class TestNotaryService(override val services: ServiceHubInternal, override val notaryIdentityKey: PublicKey) : SinglePartyNotaryService() {
        override val uniquenessProvider = object : UniquenessProvider {
            /** A dummy commit method that immediately returns a success message. */
            override fun commit(states: List<StateRef>, txId: SecureHash, callerIdentity: Party, requestSignature: NotarisationRequestSignature, timeWindow: TimeWindow?, references: List<StateRef>): CordaFuture<UniquenessProvider.Result> {
                return openFuture<UniquenessProvider.Result>().apply {
                    val signature = services.database.transaction {
                        signTransaction(txId)
                    }
                    set(UniquenessProvider.Result.Success(signature))
                }
            }

            override fun getEta(numStates: Int): Duration = waitETA
        }

        @Suspendable
        override fun commitInputStates(
                inputs: List<StateRef>,
                txId: SecureHash,
                caller: Party,
                requestSignature: NotarisationRequestSignature,
                timeWindow: TimeWindow?,
                references: List<StateRef>
        ) : UniquenessProvider.Result {
            val callingFlow = FlowLogic.currentTopLevel
                    ?: throw IllegalStateException("This method should be invoked in a flow context.")

            if (requestsReceived.getAndIncrement() == 0) {
                log.info("Ignoring")
                // Waiting forever
                callingFlow.stateMachine.suspend(FlowIORequest.ExecuteAsyncOperation(WaitForLedgerCommit(SecureHash.randomSHA256(), services)), false)
            } else {
                log.info("Processing")
                return super.commitInputStates(inputs, txId, caller, requestSignature, timeWindow, references)
            }
            return UniquenessProvider.Result.Failure(NotaryError.General(Throwable("leave me alone")))
        }

        override fun createServiceFlow(otherPartySession: FlowSession): FlowLogic<Void?> = NonValidatingNotaryFlow(otherPartySession, this, waitEtaThreshold)
        override fun start() {}
        override fun stop() {}
    }
}
