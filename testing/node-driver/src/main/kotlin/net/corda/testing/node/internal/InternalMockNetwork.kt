package net.corda.testing.node.internal

import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.whenever
import net.corda.common.configuration.parsing.internal.ConfigurationWithOptions
import net.corda.core.DoNotImplement
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.random63BitValue
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.InitiatedBy
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.internal.FlowIORequest
import net.corda.core.internal.NetworkParametersStorage
import net.corda.core.internal.PLATFORM_VERSION
import net.corda.core.internal.VisibleForTesting
import net.corda.core.internal.createDirectories
import net.corda.core.internal.deleteIfExists
import net.corda.core.internal.div
import net.corda.core.internal.notary.NotaryService
import net.corda.core.internal.uncheckedCast
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.MessageRecipients
import net.corda.core.messaging.RPCOps
import net.corda.core.messaging.SingleMessageRecipient
import net.corda.core.node.NetworkParameters
import net.corda.core.node.NodeInfo
import net.corda.core.node.NotaryInfo
import net.corda.core.serialization.SerializationWhitelist
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.hours
import net.corda.core.utilities.seconds
import net.corda.coretesting.internal.rigorousMock
import net.corda.coretesting.internal.stubs.CertificateStoreStubs
import net.corda.coretesting.internal.testThreadFactory
import net.corda.node.VersionInfo
import net.corda.node.internal.AbstractNode
import net.corda.node.internal.InitiatedFlowFactory
import net.corda.node.internal.NodeFlowManager
import net.corda.node.services.api.FlowStarter
import net.corda.node.services.api.ServiceHubInternal
import net.corda.node.services.api.StartedNodeServices
import net.corda.node.services.config.FlowTimeoutConfiguration
import net.corda.node.services.config.NetworkParameterAcceptanceSettings
import net.corda.node.services.config.NodeConfiguration
import net.corda.node.services.config.NotaryConfig
import net.corda.node.services.config.VerifierType
import net.corda.node.services.identity.PersistentIdentityService
import net.corda.node.services.keys.BasicHSMKeyManagementService
import net.corda.node.services.keys.KeyManagementServiceInternal
import net.corda.node.services.messaging.Message
import net.corda.node.services.messaging.MessagingService
import net.corda.node.services.persistence.NodeAttachmentService
import net.corda.node.services.statemachine.FlowState
import net.corda.node.services.statemachine.StateMachineManager
import net.corda.node.utilities.AffinityExecutor.ServiceAffinityExecutor
import net.corda.node.utilities.DefaultNamedCacheFactory
import net.corda.nodeapi.internal.DevIdentityGenerator
import net.corda.nodeapi.internal.config.User
import net.corda.nodeapi.internal.network.NetworkParametersCopier
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.nodeapi.internal.persistence.DatabaseConfig
import net.corda.testing.common.internal.testNetworkParameters
import net.corda.testing.node.DatabaseSnapshot
import net.corda.testing.node.InMemoryMessagingNetwork
import net.corda.testing.node.MockNetworkNotarySpec
import net.corda.testing.node.MockNetworkParameters
import net.corda.testing.node.MockNodeParameters
import net.corda.testing.node.MockServices.Companion.makeTestDataSourceProperties
import net.corda.testing.node.TestClock
import org.apache.activemq.artemis.utils.ReusableLatch
import org.apache.sshd.common.util.security.SecurityUtils
import rx.Observable
import rx.Scheduler
import rx.internal.schedulers.CachedThreadScheduler
import java.math.BigInteger
import java.net.URLClassLoader
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Clock
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

val MOCK_VERSION_INFO = VersionInfo(PLATFORM_VERSION, "Mock release", "Mock revision", "Mock Vendor")

data class MockNodeArgs(
        val config: NodeConfiguration,
        val network: InternalMockNetwork,
        val id: Int,
        val entropyRoot: BigInteger,
        val version: VersionInfo = MOCK_VERSION_INFO,
        val flowManager: MockNodeFlowManager = MockNodeFlowManager()
)

// TODO We don't need a parameters object as this is internal only
data class InternalMockNodeParameters(
        val forcedID: Int? = null,
        val legalName: CordaX500Name? = null,
        val entropyRoot: BigInteger = BigInteger.valueOf(random63BitValue()),
        val configOverrides: (NodeConfiguration) -> Any? = {},
        val version: VersionInfo = MOCK_VERSION_INFO,
        val additionalCordapps: Collection<TestCordappInternal> = emptyList(),
        val flowManager: MockNodeFlowManager = MockNodeFlowManager()) {
    constructor(mockNodeParameters: MockNodeParameters) : this(
            mockNodeParameters.forcedID,
            mockNodeParameters.legalName,
            mockNodeParameters.entropyRoot,
            { mockNodeParameters.configOverrides?.applyMockNodeOverrides(it) },
            MOCK_VERSION_INFO,
            uncheckedCast(mockNodeParameters.additionalCordapps)
    )
}

/**
 * A [StartedNode] which exposes its internal [InternalMockNetwork.MockNode] for testing.
 */
interface TestStartedNode {
    val internals: InternalMockNetwork.MockNode
    val info: NodeInfo
    val services: StartedNodeServices
    val smm: StateMachineManager
    val attachments: NodeAttachmentService
    val rpcOpsList: List<RPCOps>
    val network: MockNodeMessagingService
    val database: CordaPersistence
    val notaryService: NotaryService?

    fun dispose() = internals.stop()

    fun pumpReceive(block: Boolean = false): InMemoryMessagingNetwork.MessageTransfer? {
        return network.pumpReceive(block)
    }

    /**
     * Attach a [MessagingServiceSpy] to the [InternalMockNetwork.MockNode] allowing interception and modification of messages.
     */
    fun setMessagingServiceSpy(spy: MessagingServiceSpy) {
        internals.setMessagingServiceSpy(spy)
    }

    /**
     * Use this method to register your initiated flows in your tests. This is automatically done by the node when it
     * starts up for all [FlowLogic] classes it finds which are annotated with [InitiatedBy].
     * @return An [Observable] of the initiated flows started by counterparties.
     */
    fun <T : FlowLogic<*>> registerInitiatedFlow(initiatedFlowClass: Class<T>, track: Boolean = false): Observable<T>

    fun <T : FlowLogic<*>> registerInitiatedFlow(initiatingFlowClass: Class<out FlowLogic<*>>, initiatedFlowClass: Class<T>, track: Boolean = false): Observable<T>

    val cordaRPCOps: CordaRPCOps
        get() = rpcOpsList.mapNotNull { it as? CordaRPCOps }.single()
}

open class InternalMockNetwork(cordappPackages: List<String> = emptyList(),
                               // TODO InternalMockNetwork does not need MockNetworkParameters
                               defaultParameters: MockNetworkParameters = MockNetworkParameters(),
                               val networkSendManuallyPumped: Boolean = defaultParameters.networkSendManuallyPumped,
                               val threadPerNode: Boolean = defaultParameters.threadPerNode,
                               servicePeerAllocationStrategy: InMemoryMessagingNetwork.ServicePeerAllocationStrategy = defaultParameters.servicePeerAllocationStrategy,
                               val notarySpecs: List<MockNetworkNotarySpec> = defaultParameters.notarySpecs,
                               val testDirectory: Path = Paths.get("build") / "mock-network" /  getTimestampAsDirectoryName(),
                               initialNetworkParameters: NetworkParameters = testNetworkParameters(),
                               val defaultFactory: (MockNodeArgs) -> MockNode = { args -> MockNode(args) },
                               cordappsForAllNodes: Collection<TestCordappInternal> = emptySet(),
                               val autoVisibleNodes: Boolean = true) : AutoCloseable {
    companion object {
        fun createCordappClassLoader(cordapps: Collection<TestCordappInternal>?): URLClassLoader? {
            if (cordapps == null || cordapps.isEmpty()) {
                return null
            }
            return URLClassLoader(cordapps.map { it.jarFile.toUri().toURL() }.toTypedArray())
        }
    }

    var networkParameters: NetworkParameters = initialNetworkParameters
        private set

    init {
        // Apache SSHD for whatever reason registers a SFTP FileSystemProvider - which gets loaded by JimFS.
        // This SFTP support loads BouncyCastle, which we want to avoid.
        // Please see https://issues.apache.org/jira/browse/SSHD-736 - it's easier then to create our own fork of SSHD
        SecurityUtils.setAPrioriDisabledProvider("BC", true) // XXX: Why isn't this static?
        require(initialNetworkParameters.notaries.isEmpty()) { "Define notaries using notarySpecs" }
    }

    var nextNodeId = 0
        private set
    private val busyLatch = ReusableLatch()
    val messagingNetwork = InMemoryMessagingNetwork.create(networkSendManuallyPumped, servicePeerAllocationStrategy, busyLatch)
    // A unique identifier for this network to segregate databases with the same nodeID but different networks.
    private val networkId = random63BitValue()
    private val networkParametersCopier: NetworkParametersCopier
    private val _nodes = mutableListOf<MockNode>()
    private val sharedUserCount = AtomicInteger(0)
    private val combinedCordappsForAllNodes = cordappsForPackages(cordappPackages) + cordappsForAllNodes
    private val cordappClassLoader = createCordappClassLoader(combinedCordappsForAllNodes)
    private val serializationEnv = checkNotNull(setDriverSerialization(cordappClassLoader)) {
        "Using more than one mock network simultaneously is not supported."
    }

    /** A read only view of the current set of nodes. */
    val nodes: List<MockNode> get() = _nodes

    /**
     * Returns the list of nodes started by the network. Each notary specified when the network is constructed ([notarySpecs]
     * parameter) maps 1:1 to the notaries returned by this list.
     */
    val notaryNodes: List<TestStartedNode>

    /**
     * Returns the single notary node on the network. Throws if there are none or more than one.
     * @see notaryNodes
     */
    val defaultNotaryNode: TestStartedNode
        get() {
            return when (notaryNodes.size) {
                0 -> throw IllegalStateException("There are no notaries defined on the network")
                1 -> notaryNodes[0]
                else -> throw IllegalStateException("There is more than one notary defined on the network")
            }
        }

    /**
     * Return the identity of the default notary node.
     * @see defaultNotaryNode
     */
    val defaultNotaryIdentity: Party
        get() {
            return defaultNotaryNode.info.legalIdentities.singleOrNull()
                    ?: throw IllegalStateException("Default notary has multiple identities")
        }

    /**
     * Because this executor is shared, we need to be careful about nodes shutting it down.
     */
    private val sharedServerThread = object : ServiceAffinityExecutor("Mock network", 1) {
        override fun shutdown() {
            // We don't actually allow the shutdown of the network-wide shared thread pool until all references to
            // it have been shutdown.
            if (sharedUserCount.decrementAndGet() == 0) {
                super.shutdown()
            }
        }

        override fun awaitTermination(timeout: Long, unit: TimeUnit): Boolean {
            return if (!isShutdown) {
                flush()
                true
            } else {
                super.awaitTermination(timeout, unit)
            }
        }
    }

    init {
        try {
            val notaryInfos = generateNotaryIdentities()
            networkParameters = initialNetworkParameters.copy(notaries = notaryInfos)
            // The network parameters must be serialised before starting any of the nodes
            networkParametersCopier = NetworkParametersCopier(networkParameters)
            @Suppress("LeakingThis")
            // Notary nodes need a platform version >= network min platform version.
            notaryNodes = createNotaries()
        } catch (t: Throwable) {
            stopNodes()
            throw t
        }
    }

    private fun generateNotaryIdentities(): List<NotaryInfo> {
        return notarySpecs.mapIndexed { index, (name, validating) ->
            val identity = DevIdentityGenerator.installKeyStoreWithNodeIdentity(baseDirectory(nextNodeId + index), name)
            NotaryInfo(identity, validating)
        }
    }

    @VisibleForTesting
    internal open fun createNotaries(): List<TestStartedNode> {
        return notarySpecs.map { spec ->
            createNode(InternalMockNodeParameters(
                    legalName = spec.name,
                    configOverrides = { doReturn(NotaryConfig(spec.validating, className = spec.className)).whenever(it).notary }
            ))
        }
    }

    private fun getServerThread(id: Int): ServiceAffinityExecutor {
        return if (threadPerNode) {
            ServiceAffinityExecutor("Mock node $id thread", 1)
        } else {
            sharedUserCount.incrementAndGet()
            sharedServerThread
        }
    }

    open class MockNode(
            args: MockNodeArgs,
            private val mockFlowManager: MockNodeFlowManager = args.flowManager,
            allowAppSchemaUpgradeWithCheckpoints: Boolean = false) : AbstractNode<TestStartedNode>(
            args.config,
            TestClock(Clock.systemUTC()),
            DefaultNamedCacheFactory(),
            args.version,
            mockFlowManager,
            args.network.getServerThread(args.id),
            args.network.busyLatch,
            allowHibernateToManageAppSchema = true,
            allowAppSchemaUpgradeWithCheckpoints = allowAppSchemaUpgradeWithCheckpoints
    ) {
        companion object {
            private val staticLog = contextLogger()
        }

        /** The actual [TestStartedNode] implementation created by this node */
        private class TestStartedNodeImpl(
                override val internals: MockNode,
                override val attachments: NodeAttachmentService,
                override val network: MockNodeMessagingService,
                override val services: StartedNodeServices,
                override val info: NodeInfo,
                override val smm: StateMachineManager,
                override val database: CordaPersistence,
                override val rpcOpsList: List<RPCOps>,
                override val notaryService: NotaryService?) : TestStartedNode {

            override fun dispose() = internals.stop()

            override fun <T : FlowLogic<*>> registerInitiatedFlow(initiatedFlowClass: Class<T>, track: Boolean): Observable<T> {
                internals.flowManager.registerInitiatedFlow(initiatedFlowClass)
                return smm.changes.filter { it is StateMachineManager.Change.Add }.map { it.logic }.ofType(initiatedFlowClass)
            }

            override fun <T : FlowLogic<*>> registerInitiatedFlow(initiatingFlowClass: Class<out FlowLogic<*>>, initiatedFlowClass: Class<T>, track: Boolean): Observable<T> {
                internals.flowManager.registerInitiatedFlow(initiatingFlowClass, initiatedFlowClass)
                return smm.changes.filter { it is StateMachineManager.Change.Add }.map { it.logic }.ofType(initiatedFlowClass)
            }
        }

        override val runMigrationScripts: Boolean = true

        val mockNet = args.network
        val id = args.id

        init {
            require(id >= 0) { "Node ID must be zero or positive, was passed: $id" }
        }

        private val entropyCounter = AtomicReference(args.entropyRoot)
        override val log get() = staticLog
        override val transactionVerifierWorkerCount: Int get() = 1

        private var _rxIoScheduler: Scheduler? = null
        override val rxIoScheduler: Scheduler
            get() {
                return _rxIoScheduler ?: CachedThreadScheduler(testThreadFactory()).also {
                    runOnStop += it::shutdown
                    _rxIoScheduler = it
                }
            }

        override val started: TestStartedNode? get() = super.started

        override fun createStartedNode(nodeInfo: NodeInfo, rpcOps: List<RPCOps>, notaryService: NotaryService?): TestStartedNode {
            return TestStartedNodeImpl(
                    this,
                    attachments,
                    network as MockNodeMessagingService,
                    object : StartedNodeServices, ServiceHubInternal by services, FlowStarter by flowStarter {},
                    nodeInfo,
                    smm,
                    database,
                    rpcOps,
                    notaryService
            )
        }

        override fun start(): TestStartedNode {
            mockNet.networkParametersCopier.install(configuration.baseDirectory)
            return super.start().also(::advertiseNodeToNetwork)
        }

        private fun advertiseNodeToNetwork(newNode: TestStartedNode) {
            if (!mockNet.autoVisibleNodes) return
            mockNet.nodes
                    .mapNotNull { it.started }
                    .forEach { existingNode ->
                        newNode.services.networkMapCache.addOrUpdateNode(existingNode.info)
                        existingNode.services.networkMapCache.addOrUpdateNode(newNode.info)
                    }
        }

        override fun makeMessagingService(): MockNodeMessagingService {
            return MockNodeMessagingService(configuration, serverThread).closeOnStop(usesDatabase = false)
        }

        override fun startMessagingService(rpcOps: List<RPCOps>,
                                           nodeInfo: NodeInfo,
                                           myNotaryIdentity: PartyAndCertificate?,
                                           networkParameters: NetworkParameters) {
            (network as MockNodeMessagingService).start(mockNet.messagingNetwork, !mockNet.threadPerNode, id, myNotaryIdentity)
        }

        fun setMessagingServiceSpy(spy: MessagingServiceSpy) {
            spy._messagingService = network
            (network as MockNodeMessagingService).spy = spy
        }

        override fun makeKeyManagementService(identityService: PersistentIdentityService): KeyManagementServiceInternal {
            return BasicHSMKeyManagementService(cacheFactory, identityService, database, cryptoService)
        }

        override fun startShell() {
            //No mock shell
        }

        override fun initKeyStores() = keyStoreHandler.init(entropyCounter.updateAndGet { it.add(BigInteger.ONE) })

        // NodeInfo requires a non-empty addresses list and so we give it a dummy value for mock nodes.
        // The non-empty addresses check is important to have and so we tolerate the ugliness here.
        override fun myAddresses(): List<NetworkHostAndPort> = listOf(NetworkHostAndPort("mock.node", 1000))

        // Allow unit tests to modify the serialization whitelist list before the node start,
        // so they don't have to ServiceLoad test whitelists into all unit tests.
        private val _serializationWhitelists by lazy { super.serializationWhitelists.toMutableList() }
        override val serializationWhitelists: List<SerializationWhitelist>
            get() = _serializationWhitelists
        private var dbCloser: (() -> Any?)? = null

        override fun startDatabase() {
            super.startDatabase()
            dbCloser = database::close
            runOnStop += dbCloser!!
        }

        fun disableDBCloseOnStop() {
            runOnStop.remove(dbCloser)
        }

        fun manuallyCloseDB() {
            dbCloser?.invoke()
            dbCloser = null
        }

        var acceptableLiveFiberCountOnStop: Int = 0

        override fun acceptableLiveFiberCountOnStop(): Int = acceptableLiveFiberCountOnStop

        fun <T : FlowLogic<*>> registerInitiatedFlowFactory(initiatingFlowClass: Class<out FlowLogic<*>>, initiatedFlowClass: Class<T>, factory: InitiatedFlowFactory<T>, track: Boolean): Observable<T> {
            mockFlowManager.registerTestingFactory(initiatingFlowClass, factory)
            return if (track) {
                smm.changes.filter { it is StateMachineManager.Change.Add }.map { it.logic }.ofType(initiatedFlowClass)
            } else {
                Observable.empty<T>()
            }
        }

        override fun makeNetworkParametersStorage(): NetworkParametersStorage = MockNetworkParametersStorage()
    }

    fun createUnstartedNode(parameters: InternalMockNodeParameters = InternalMockNodeParameters()): MockNode {
        return createUnstartedNode(parameters, defaultFactory)
    }

    fun createUnstartedNode(parameters: InternalMockNodeParameters = InternalMockNodeParameters(), nodeFactory: (MockNodeArgs) -> MockNode): MockNode {
        return createNodeImpl(parameters, nodeFactory, false)
    }

    fun createNode(parameters: InternalMockNodeParameters = InternalMockNodeParameters()): TestStartedNode {
        return createNode(parameters, defaultFactory)
    }

    /** Like the other [createNode] but takes a [nodeFactory] and propagates its [MockNode] subtype. */
    fun createNode(parameters: InternalMockNodeParameters = InternalMockNodeParameters(), nodeFactory: (MockNodeArgs) -> MockNode): TestStartedNode {
        return uncheckedCast(createNodeImpl(parameters, nodeFactory, true).started)!!
    }

    private fun createNodeImpl(parameters: InternalMockNodeParameters, nodeFactory: (MockNodeArgs) -> MockNode, start: Boolean): MockNode {
        val id = parameters.forcedID ?: nextNodeId++
        val baseDirectory = baseDirectory(id)
        val certificatesDirectory = baseDirectory / "certificates"
        certificatesDirectory.createDirectories()
        val config = mockNodeConfiguration(certificatesDirectory).also {
            doReturn(baseDirectory).whenever(it).baseDirectory
            doReturn(baseDirectory).whenever(it).networkParametersPath
            doReturn(parameters.legalName ?: CordaX500Name("Mock Company $id", "London", "GB")).whenever(it).myLegalName
            doReturn(makeTestDataSourceProperties("node_${id}_net_$networkId")).whenever(it).dataSourceProperties
            doReturn(emptyList<SecureHash>()).whenever(it).extraNetworkMapKeys
            doReturn(listOf(baseDirectory / "cordapps")).whenever(it).cordappDirectories
            doReturn(emptyList<String>()).whenever(it).quasarExcludePackages
            parameters.configOverrides(it)
        }

        TestCordappInternal.installCordapps(baseDirectory, parameters.additionalCordapps.toSet(), combinedCordappsForAllNodes)

        val node = nodeFactory(MockNodeArgs(config, this, id, parameters.entropyRoot, parameters.version, flowManager = parameters.flowManager))
        _nodes += node
        if (start) {
            node.start()
        }
        return node
    }

    fun restartNode(
            node: TestStartedNode,
            parameters: InternalMockNodeParameters = InternalMockNodeParameters(),
            nodeFactory: (MockNodeArgs) -> MockNode = defaultFactory
    ): TestStartedNode {
        node.internals.disableDBCloseOnStop()
        node.dispose()
        return createNode(
                parameters.copy(legalName = node.internals.configuration.myLegalName, forcedID = node.internals.id),
                nodeFactory
        )
    }

    fun baseDirectory(node: TestStartedNode): Path = baseDirectory(node.internals.id)

    fun baseDirectory(nodeId: Int): Path = testDirectory / "nodes/$nodeId"

    /**
     * Asks every node in order to process any queued up inbound messages. This may in turn result in nodes
     * sending more messages to each other, thus, a typical usage is to call runNetwork with the [rounds]
     * parameter set to -1 (the default) which simply runs as many rounds as necessary to result in network
     * stability (no nodes sent any messages in the last round).
     */
    @JvmOverloads
    fun runNetwork(rounds: Int = -1) {
        check(!networkSendManuallyPumped) {
            "MockNetwork.runNetwork() should only be used when networkSendManuallyPumped == false. " +
                    "You can use MockNetwork.waitQuiescent() to wait for all the nodes to process all the messages on their queues instead."
        }

        if (rounds == -1) {
            do {
                awaitAsyncOperations()
            } while (pumpAll())
        } else {
            repeat(rounds) {
                pumpAll()
            }
        }
    }

    private fun pumpAll(): Boolean {
        val transferredMessages = messagingNetwork.endpoints.filter { it.active }
                .map { it.pumpReceive(false) }
        return transferredMessages.any { it != null }
    }

    /**
     * We wait for any flows that are suspended on an async operation completion to resume and either
     * finish the flow, or generate a response message.
     */
    private fun awaitAsyncOperations() {
        while (anyFlowsSuspendedOnAsyncOperation()) {
            Thread.sleep(50)
        }
    }

    /**
     * Returns true if there are any flows suspended waiting for an async operation to complete, except for async operations that
     * need to collect errors from sessions (e.g. [WaitForLedgerCommit]).
     */
    private fun anyFlowsSuspendedOnAsyncOperation(): Boolean {
        val allNodes = this._nodes
        val allActiveFlows = allNodes.flatMap { it.smm.snapshot() }

        return allActiveFlows.any {
            val flowState = it.snapshot().checkpoint.flowState
            flowState is FlowState.Started && when (flowState.flowIORequest) {
                is FlowIORequest.ExecuteAsyncOperation ->
                    !(flowState.flowIORequest as FlowIORequest.ExecuteAsyncOperation<*>).operation.collectErrorsFromSessions
                is FlowIORequest.Sleep -> true
                else -> false
            }
        }
    }

    @JvmOverloads
    fun createPartyNode(legalName: CordaX500Name? = null): TestStartedNode {
        return createNode(InternalMockNodeParameters(legalName = legalName))
    }

    @Suppress("unused") // This is used from the network visualiser tool.
    fun addressToNode(msgRecipient: MessageRecipients): MockNode {
        return when (msgRecipient) {
            is SingleMessageRecipient -> nodes.single { it.started!!.network.myAddress == msgRecipient }
            is InMemoryMessagingNetwork.DistributedServiceHandle -> {
                nodes.firstOrNull { it.started!!.info.isLegalIdentity(msgRecipient.party) }
                        ?: throw IllegalArgumentException("Couldn't find node advertising service with owning party name: ${msgRecipient.party.name} ")
            }
            else -> throw IllegalArgumentException("Method not implemented for different type of message recipients")
        }
    }

    fun startNodes() {
        require(nodes.isNotEmpty())
        nodes.forEach { it.started ?: it.start() }
    }

    fun stopNodes() {
        cordappClassLoader.use { _ ->
            // Serialization env must be unset even if other parts of this method fail.
            serializationEnv.use {
                nodes.forEach { node ->
                    node.started?.dispose()
                    DatabaseSnapshot.databaseFilename(node.configuration.baseDirectory).deleteIfExists()
                }
            }
            messagingNetwork.stop()
        }
    }

    /** Block until all scheduled activity, active flows and network activity has ceased. */
    fun waitQuiescent() {
        busyLatch.await(30000) // don't hang forever if for some reason things don't complete
    }

    override fun close() = stopNodes()
}

abstract class MessagingServiceSpy {
    internal var _messagingService: MessagingService? = null
        set(value) {
            check(field == null) { "Spy has already been attached to a node" }
            field = value
        }
    val messagingService: MessagingService get() = checkNotNull(_messagingService) { "Spy has not been attached to a node" }

    abstract fun send(message: Message, target: MessageRecipients, sequenceKey: Any)
}

private fun mockNodeConfiguration(certificatesDirectory: Path): NodeConfiguration {
    @DoNotImplement
    abstract class AbstractNodeConfiguration : NodeConfiguration

    val signingCertificateStore = CertificateStoreStubs.Signing.withCertificatesDirectory(certificatesDirectory)
    val p2pSslConfiguration = CertificateStoreStubs.P2P.withCertificatesDirectory(certificatesDirectory)

    return rigorousMock<AbstractNodeConfiguration>().also {
        doReturn(certificatesDirectory.createDirectories()).whenever(it).certificatesDirectory
        doReturn(p2pSslConfiguration).whenever(it).p2pSslOptions
        doReturn(signingCertificateStore).whenever(it).signingCertificateStore
        doReturn(emptyList<User>()).whenever(it).rpcUsers
        doReturn(null).whenever(it).notary
        doReturn(DatabaseConfig()).whenever(it).database
        doReturn("").whenever(it).emailAddress
        doReturn(null).whenever(it).jmxMonitoringHttpPort
        doReturn(true).whenever(it).devMode
        doReturn(emptyList<String>()).whenever(it).blacklistedAttachmentSigningKeys
        @Suppress("DEPRECATION")
        doReturn(null).whenever(it).compatibilityZoneURL
        doReturn(null).whenever(it).networkServices
        doReturn(VerifierType.InMemory).whenever(it).verifierType
        // Set to be long enough so retries don't trigger unless we override it
        doReturn(FlowTimeoutConfiguration(1.hours, 3, backoffBase = 1.0)).whenever(it).flowTimeout
        doReturn(5.seconds.toMillis()).whenever(it).additionalNodeInfoPollingFrequencyMsec
        doReturn(null).whenever(it).devModeOptions
        doReturn(NetworkParameterAcceptanceSettings()).whenever(it).networkParameterAcceptanceSettings
        doReturn(rigorousMock<ConfigurationWithOptions>()).whenever(it).configurationWithOptions
        doReturn(2).whenever(it).flowExternalOperationThreadPoolSize
        doReturn(false).whenever(it).reloadCheckpointAfterSuspend
    }
}

class MockNodeFlowManager : NodeFlowManager() {
    val testingRegistrations = HashMap<Class<out FlowLogic<*>>, InitiatedFlowFactory<*>>()
    override fun getFlowFactoryForInitiatingFlow(initiatedFlowClass: Class<out FlowLogic<*>>): InitiatedFlowFactory<*>? {
        if (initiatedFlowClass in testingRegistrations) {
            return testingRegistrations.get(initiatedFlowClass)
        }
        return super.getFlowFactoryForInitiatingFlow(initiatedFlowClass)
    }

    fun registerTestingFactory(initiator: Class<out FlowLogic<*>>, factory: InitiatedFlowFactory<*>) {
        testingRegistrations.put(initiator, factory)
    }
}

