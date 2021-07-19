package net.corda.testing.node

import com.google.common.collect.MutableClassToInstanceMap
import net.corda.core.contracts.Attachment
import net.corda.core.contracts.ContractClassName
import net.corda.core.contracts.StateRef
import net.corda.core.cordapp.CordappProvider
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.StateMachineRunId
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.internal.PLATFORM_VERSION
import net.corda.core.internal.requireSupportedHashType
import net.corda.core.messaging.DataFeed
import net.corda.core.messaging.FlowHandle
import net.corda.core.messaging.FlowProgressHandle
import net.corda.core.messaging.StateMachineTransactionMapping
import net.corda.core.node.*
import net.corda.core.node.services.*
import net.corda.core.node.services.diagnostics.DiagnosticsService
import net.corda.core.node.services.vault.CordaTransactionSupport
import net.corda.core.serialization.SerializeAsToken
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.node.VersionInfo
import net.corda.node.internal.ServicesForResolutionImpl
import net.corda.node.internal.cordapp.JarScanningCordappLoader
import net.corda.node.services.api.*
import net.corda.node.services.diagnostics.NodeDiagnosticsService
import net.corda.node.services.identity.InMemoryIdentityService
import net.corda.node.services.identity.PersistentIdentityService
import net.corda.node.services.keys.BasicHSMKeyManagementService
import net.corda.node.services.persistence.PublicKeyToOwningIdentityCacheImpl
import net.corda.node.services.schema.NodeSchemaService
import net.corda.node.services.transactions.InMemoryTransactionVerifierService
import net.corda.node.services.vault.NodeVaultService
import net.corda.nodeapi.internal.cordapp.CordappLoader
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.nodeapi.internal.persistence.DatabaseConfig
import net.corda.nodeapi.internal.persistence.contextTransaction
import net.corda.testing.common.internal.testNetworkParameters
import net.corda.testing.core.TestIdentity
import net.corda.coretesting.internal.DEV_ROOT_CA
import net.corda.node.services.network.PersistentMembershipGroupCache
import net.corda.testing.internal.MockCordappProvider
import net.corda.testing.internal.TestingNamedCacheFactory
import net.corda.testing.internal.configureDatabase
import net.corda.testing.node.internal.*
import net.corda.testing.services.MockAttachmentStorage
import java.io.ByteArrayOutputStream
import java.nio.file.Paths
import java.security.KeyPair
import java.sql.Connection
import java.time.Clock
import java.time.Instant
import java.util.*
import java.util.function.Consumer
import java.util.jar.JarFile
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import javax.persistence.EntityManager

/** Returns a simple [IdentityService] containing the supplied [identities]. */
fun makeTestIdentityService(vararg identities: PartyAndCertificate): IdentityService {
    return InMemoryIdentityService(identities.toList(), DEV_ROOT_CA.certificate)
}

/**
 * An implementation of [ServiceHub] that is designed for in-memory unit tests of contract validation logic. It has
 * enough functionality to do tests of code that queries the vault, inserts to the vault, and constructs/checks
 * transactions. However it isn't enough to test flows and other aspects of an app that require a network. For that
 * you should investigate [MockNetwork].
 *
 * There are a variety of constructors that can be used to supply enough data to simulate a node. Each mock service hub
 * must have at least an identity of its own. The other components have defaults that work in most situations.
 */
open class MockServices private constructor(
        private val cordappLoader: CordappLoader,
        override val validatedTransactions: TransactionStorage,
        override val identityService: IdentityService,
        initialNetworkParameters: NetworkParameters,
        private val initialIdentity: TestIdentity,
        private val moreKeys: Array<out KeyPair>,
        override val keyManagementService: KeyManagementService = MockKeyManagementService(
                identityService,
                *arrayOf(initialIdentity.keyPair) + moreKeys
        )
) : ServiceHub {

    companion object {
        private fun cordappLoaderForPackages(packages: Iterable<String>, versionInfo: VersionInfo = VersionInfo.UNKNOWN): CordappLoader {
            return JarScanningCordappLoader.fromJarUrls(cordappsForPackages(packages).map { it.jarFile.toUri().toURL() }, versionInfo)
        }

        /**
         * Make properties appropriate for creating a DataSource for unit tests.
         *
         * @param nodeName Reflects the "instance" of the in-memory database.  Defaults to a random string.
         */
        // TODO: Can we use an X509 principal generator here?
        @JvmStatic
        fun makeTestDataSourceProperties(nodeName: String = SecureHash.randomSHA256().toString()): Properties {
            val dbDir = Paths.get("","build", "mocknetworktestdb", nodeName)
                    .toAbsolutePath()
            val dbPath = dbDir.resolve("persistence")
            try {
                DatabaseSnapshot.copyDatabaseSnapshot(dbDir)
            } catch (ex: java.nio.file.FileAlreadyExistsException) {
                DriverDSLImpl.log.warn("Database already exists on disk, not attempting to pre-migrate database.")
            }
            val props = Properties()
            props.setProperty("dataSourceClassName", "org.h2.jdbcx.JdbcDataSource")
            props.setProperty("dataSource.url", "jdbc:h2:file:$dbPath;LOCK_TIMEOUT=10000;DB_CLOSE_ON_EXIT=FALSE")
            props.setProperty("dataSource.user", "sa")
            props.setProperty("dataSource.password", "")
            return props
        }

        /**
         * Makes database and mock services appropriate for unit tests.
         *
         * @param cordappPackages A [List] of cordapp packages to scan for any cordapp code, e.g. contract verification code, flows and services.
         * @param identityService An instance of [IdentityService], see [makeTestIdentityService].
         * @param initialIdentity The first (typically sole) identity the services will represent.
         * @param moreKeys A list of additional [KeyPair] instances to be used by [MockServices].
         * @return A pair where the first element is the instance of [CordaPersistence] and the second is [MockServices].
         */
        @JvmStatic
        @JvmOverloads
        fun makeTestDatabaseAndMockServices(cordappPackages: List<String>,
                                            identityService: IdentityService,
                                            initialIdentity: TestIdentity,
                                            networkParameters: NetworkParameters = testNetworkParameters(modifiedTime = Instant.MIN),
                                            vararg moreKeys: KeyPair): Pair<CordaPersistence, MockServices> {

            val cordappLoader = cordappLoaderForPackages(cordappPackages)
            val dataSourceProps = makeTestDataSourceProperties()
            val schemaService = NodeSchemaService(cordappLoader.cordappSchemas)
            val database = configureDatabase(dataSourceProps, DatabaseConfig(), identityService::wellKnownPartyFromX500Name, identityService::wellKnownPartyFromAnonymous, schemaService, schemaService.internalSchemas)
            val keyManagementService = MockKeyManagementService(
                    identityService,
                    *arrayOf(initialIdentity.keyPair) + moreKeys
            )
            val mockService = database.transaction {
                makeMockMockServices(cordappLoader, identityService, networkParameters, initialIdentity, moreKeys.toSet(), keyManagementService, schemaService, database)
            }
            return Pair(database, mockService)
        }

        /**
         * Makes database and persistent services appropriate for unit tests which require persistence across the vault, identity service
         * and key managment service.
         *
         * @param cordappPackages A [List] of cordapp packages to scan for any cordapp code, e.g. contract verification code,
         *        flows and services.
         * @param initialIdentity The first (typically sole) identity the services will represent.
         * @param moreKeys A list of additional [KeyPair] instances to be used by [MockServices].
         * @param moreIdentities A list of additional [KeyPair] instances to be used by [MockServices].
         * @param cacheFactory A custom cache factory to be used by the created [IdentityService]
         * @return A pair where the first element is the instance of [CordaPersistence] and the second is [MockServices].
         */
        @JvmStatic
        @JvmOverloads
        fun makeTestDatabaseAndPersistentServices(
                cordappPackages: List<String>,
                initialIdentity: TestIdentity,
                networkParameters: NetworkParameters = testNetworkParameters(modifiedTime = Instant.MIN),
                moreKeys: Set<KeyPair>,
                moreIdentities: Set<PartyAndCertificate>,
                cacheFactory: TestingNamedCacheFactory = TestingNamedCacheFactory()
        ): Pair<CordaPersistence, MockServices> {
            val cordappLoader = cordappLoaderForPackages(cordappPackages)
            val dataSourceProps = makeTestDataSourceProperties()
            val schemaService = NodeSchemaService(cordappLoader.cordappSchemas)
            val identityService = PersistentIdentityService(cacheFactory)
            val persistence = configureDatabase(
                    hikariProperties = dataSourceProps,
                    databaseConfig = DatabaseConfig(),
                    wellKnownPartyFromX500Name = identityService::wellKnownPartyFromX500Name,
                    wellKnownPartyFromAnonymous = identityService::wellKnownPartyFromAnonymous,
                    schemaService = schemaService,
                    internalSchemas = schemaService.internalSchemas
            )

            val pkToIdCache = PublicKeyToOwningIdentityCacheImpl(persistence, cacheFactory)

            // Create a persistent identity service and add all the supplied identities.
            identityService.apply {
                database = persistence
                start(setOf(DEV_ROOT_CA.certificate), initialIdentity.identity, pkToIdCache = pkToIdCache)
                persistence.transaction { identityService.loadIdentities(moreIdentities + initialIdentity.identity) }
            }
            val networkMapCache = PersistentMembershipGroupCache(cacheFactory, persistence, identityService)
            (moreIdentities + initialIdentity.identity).forEach {
                networkMapCache.addOrUpdateNode(NodeInfo(listOf(NetworkHostAndPort("localhost", 0)), listOf(it), PLATFORM_VERSION, 0))
            }

            // Create a persistent key management service and add the key pair which was created for the TestIdentity.
            // We only add the keypair for the initial identity and any other keys which this node may control. Note: We don't add the keys
            // for the other identities.
            val aliasKeyMap = mutableMapOf<String, KeyPair>()
            val aliasedMoreKeys = moreKeys.mapIndexed { index, keyPair ->
                val alias = "Extra key $index"
                aliasKeyMap[alias] = keyPair
                keyPair.public to alias
            }
            val identityAlias = "${initialIdentity.name} private key"
            aliasKeyMap[identityAlias] = initialIdentity.keyPair
            val aliasedIdentityKey = initialIdentity.publicKey to identityAlias
            val keyManagementService = BasicHSMKeyManagementService(
                    TestingNamedCacheFactory(),
                    identityService,
                    persistence,
                    MockCryptoService(aliasKeyMap)
            )
            persistence.transaction { keyManagementService.start(aliasedMoreKeys + aliasedIdentityKey) }

            val mockService = persistence.transaction {
                makeMockMockServices(cordappLoader, identityService, networkParameters, initialIdentity, moreKeys, keyManagementService, schemaService, persistence)
            }
            return Pair(persistence, mockService)
        }

        private fun makeMockMockServices(
                cordappLoader: CordappLoader,
                identityService: IdentityService,
                networkParameters: NetworkParameters,
                initialIdentity: TestIdentity,
                moreKeys: Set<KeyPair>,
                keyManagementService: KeyManagementService,
                schemaService: SchemaService,
                persistence: CordaPersistence
        ): MockServices {
            return object : MockServices(cordappLoader, identityService, networkParameters, initialIdentity, moreKeys.toTypedArray(), keyManagementService) {
                override var networkParametersService: NetworkParametersService = MockNetworkParametersStorage(networkParameters)
                override val vaultService: VaultService = makeVaultService(schemaService, persistence, cordappLoader)
                override fun recordTransactions(statesToRecord: StatesToRecord, txs: Iterable<SignedTransaction>) {
                    txs.forEach { requireSupportedHashType(it) }
                    ServiceHubInternal.recordTransactions(
                            statesToRecord,
                            txs as? Collection ?: txs.toList(),
                            validatedTransactions as WritableTransactionStorage,
                            mockStateMachineRecordedTransactionMappingStorage,
                            vaultService as VaultServiceInternal,
                            persistence
                    )
                }

                override fun jdbcSession(): Connection = persistence.createSession()

                override fun <T : Any?> withEntityManager(block: EntityManager.() -> T): T {
                    return block(contextTransaction.entityManager)
                }

                override fun withEntityManager(block: Consumer<EntityManager>) {
                    return block.accept(contextTransaction.entityManager)
                }
            }
        }

        // Because Kotlin is dumb and makes not publicly visible objects public, thus changing the public API.
        private val mockStateMachineRecordedTransactionMappingStorage = MockStateMachineRecordedTransactionMappingStorage()

        private val dummyAttachment by lazy {
            val inputStream = ByteArrayOutputStream().apply {
                ZipOutputStream(this).use {
                    with(it) {
                        putNextEntry(ZipEntry(JarFile.MANIFEST_NAME))
                    }
                }
            }.toByteArray().inputStream()
            val attachment = object : Attachment {
                override val id get() = throw UnsupportedOperationException()
                override fun open() = inputStream
                override val signerKeys get() = throw UnsupportedOperationException()
                override val signers: List<Party> get() = throw UnsupportedOperationException()
                override val size: Int = 512
            }
            attachment
        }
    }

    private class MockStateMachineRecordedTransactionMappingStorage : StateMachineRecordedTransactionMappingStorage {
        override fun addMapping(stateMachineRunId: StateMachineRunId, transactionId: SecureHash) {
            throw UnsupportedOperationException()
        }

        override fun track(): DataFeed<List<StateMachineTransactionMapping>, StateMachineTransactionMapping> {
            throw UnsupportedOperationException()
        }
    }

    private constructor(cordappLoader: CordappLoader, identityService: IdentityService, networkParameters: NetworkParameters,
                        initialIdentity: TestIdentity, moreKeys: Array<out KeyPair>, keyManagementService: KeyManagementService)
            : this(cordappLoader, MockTransactionStorage(), identityService, networkParameters, initialIdentity, moreKeys, keyManagementService)

    private constructor(cordappLoader: CordappLoader, identityService: IdentityService, networkParameters: NetworkParameters,
                        initialIdentity: TestIdentity, moreKeys: Array<out KeyPair>) : this(
            cordappLoader,
            MockTransactionStorage(),
            identityService,
            networkParameters,
            initialIdentity,
            moreKeys
    )

    /**
     * Create a mock [ServiceHub] that looks for app code in the given package names, uses the provided identity service
     * (you can get one from [makeTestIdentityService]) and represents the given identity.
     */
    @JvmOverloads
    constructor(cordappPackages: Iterable<String>,
                initialIdentity: TestIdentity,
                identityService: IdentityService = makeTestIdentityService(),
                vararg moreKeys: KeyPair) :
            this(cordappLoaderForPackages(cordappPackages), identityService, testNetworkParameters(modifiedTime = Instant.MIN), initialIdentity, moreKeys)

    constructor(cordappPackages: Iterable<String>,
                initialIdentity: TestIdentity,
                identityService: IdentityService,
                networkParameters: NetworkParameters,
                vararg moreKeys: KeyPair) :
            this(cordappLoaderForPackages(cordappPackages), identityService, networkParameters, initialIdentity, moreKeys)

    constructor(cordappPackages: Iterable<String>,
                initialIdentity: TestIdentity,
                identityService: IdentityService,
                networkParameters: NetworkParameters,
                vararg moreKeys: KeyPair,
                keyManagementService: KeyManagementService) :
            this(cordappLoaderForPackages(cordappPackages), identityService, networkParameters, initialIdentity, moreKeys, keyManagementService)

    /**
     * Create a mock [ServiceHub] that looks for app code in the given package names, uses the provided identity service
     * (you can get one from [makeTestIdentityService]) and represents the given identity.
     */
    @JvmOverloads
    constructor(cordappPackages: Iterable<String>, initialIdentityName: CordaX500Name, identityService: IdentityService = makeTestIdentityService(), key: KeyPair, vararg moreKeys: KeyPair) :
            this(cordappPackages, TestIdentity(initialIdentityName, key), identityService, *moreKeys)

    /**
     * Create a mock [ServiceHub] that can't load CorDapp code, which uses the provided identity service
     * (you can get one from [makeTestIdentityService]) and which represents the given identity.
     */
    @JvmOverloads
    constructor(cordappPackages: Iterable<String>, initialIdentityName: CordaX500Name, identityService: IdentityService = makeTestIdentityService()) :
            this(cordappPackages, TestIdentity(initialIdentityName), identityService)

    /**
     * Create a mock [ServiceHub] that can't load CorDapp code, and which uses a default service identity.
     */
    constructor(cordappPackages: Iterable<String>) : this(cordappPackages, CordaX500Name("TestIdentity", "", "GB"), makeTestIdentityService())

    /**
     * Create a mock [ServiceHub] which uses the package of the caller to find CorDapp code. It uses the provided identity service
     * (you can get one from [makeTestIdentityService]) and which represents the given identity.
     */
    @JvmOverloads
    constructor(initialIdentityName: CordaX500Name, identityService: IdentityService = makeTestIdentityService(), key: KeyPair, vararg moreKeys: KeyPair)
            : this(listOf(getCallerPackage(MockServices::class)!!), TestIdentity(initialIdentityName, key), identityService, *moreKeys)

    /**
     * Create a mock [ServiceHub] which uses the package of the caller to find CorDapp code. It uses the provided identity service
     * (you can get one from [makeTestIdentityService]) and which represents the given identity. It has no keys.
     */
    @JvmOverloads
    constructor(initialIdentityName: CordaX500Name, identityService: IdentityService = makeTestIdentityService())
            : this(listOf(getCallerPackage(MockServices::class)!!), TestIdentity(initialIdentityName), identityService)

    constructor(cordappPackages: List<String>, initialIdentityName: CordaX500Name, identityService: IdentityService, networkParameters: NetworkParameters)
            : this(cordappPackages, TestIdentity(initialIdentityName), identityService, networkParameters)

    constructor(cordappPackages: List<String>, initialIdentityName: CordaX500Name, identityService: IdentityService, networkParameters: NetworkParameters, key: KeyPair)
            : this(cordappPackages, TestIdentity(initialIdentityName, key), identityService, networkParameters)

    /**
     * A helper constructor that requires at least one test identity to be registered, and which takes the package of
     * the caller as the package in which to find app code. This is the most convenient constructor and the one that
     * is normally worth using. The first identity is the identity of this service hub, the rest are identities that
     * it is aware of.
     */
    constructor(firstIdentity: TestIdentity, vararg moreIdentities: TestIdentity) : this(
            listOf(getCallerPackage(MockServices::class)!!),
            firstIdentity,
            *moreIdentities
    )

    constructor(firstIdentity: TestIdentity, networkParameters: NetworkParameters, vararg moreIdentities: TestIdentity) : this(
            listOf(getCallerPackage(MockServices::class)!!),
            firstIdentity,
            networkParameters,
            *moreIdentities
    )

    constructor(cordappPackages: List<String>, firstIdentity: TestIdentity, vararg moreIdentities: TestIdentity) : this(
            cordappPackages,
            firstIdentity,
            makeTestIdentityService(*listOf(firstIdentity, *moreIdentities).map { it.identity }.toTypedArray()),
            firstIdentity.keyPair
    )

    constructor(cordappPackages: List<String>, firstIdentity: TestIdentity, networkParameters: NetworkParameters, vararg moreIdentities: TestIdentity) : this(
            cordappPackages,
            firstIdentity,
            makeTestIdentityService(*listOf(firstIdentity, *moreIdentities).map { it.identity }.toTypedArray()),
            networkParameters,
            firstIdentity.keyPair
    )

    /**
     * Create a mock [ServiceHub] which uses the package of the caller to find CorDapp code. It uses a default service
     * identity.
     */
    constructor() : this(listOf(getCallerPackage(MockServices::class)!!), CordaX500Name("TestIdentity", "", "GB"), makeTestIdentityService())

    /**
     * Returns the classloader containing all jar deployed in the 'cordapps' folder.
     */
    val cordappClassloader: ClassLoader
        get() = cordappLoader.appClassLoader

    override fun recordTransactions(statesToRecord: StatesToRecord, txs: Iterable<SignedTransaction>) {
        txs.forEach {
            (validatedTransactions as WritableTransactionStorage).addTransaction(it)
        }
    }

    override val networkParameters: NetworkParameters
        get() = networkParametersService.run { lookup(currentHash)!! }

    final override val attachments = MockAttachmentStorage()
    override val vaultService: VaultService get() = throw UnsupportedOperationException()
    override val contractUpgradeService: ContractUpgradeService get() = throw UnsupportedOperationException()
    override val networkMapCache: MembershipGroupCache get() = throw UnsupportedOperationException()
    override val clock: TestClock get() = TestClock(Clock.systemUTC())
    override val myInfo: NodeInfo
        get() {
            return NodeInfo(listOf(NetworkHostAndPort("mock.node.services", 10000)), listOf(initialIdentity.identity), 1, serial = 1L)
        }
    override val myMemberInfo: MemberInfo get() = throw UnsupportedOperationException()
    private val mockCordappProvider: MockCordappProvider = MockCordappProvider(cordappLoader, attachments).also {
        it.start()
    }
    override val transactionVerifierService: TransactionVerifierService
        get() = InMemoryTransactionVerifierService(
                numberOfWorkers = 2,
                cordappProvider = mockCordappProvider,
                attachments = attachments
        )
    override val cordappProvider: CordappProvider get() = mockCordappProvider
    override var networkParametersService: NetworkParametersService = MockNetworkParametersStorage(initialNetworkParameters)
    override val diagnosticsService: DiagnosticsService = NodeDiagnosticsService()

    protected val servicesForResolution: ServicesForResolution
        get() = ServicesForResolutionImpl(identityService, attachments, cordappProvider, networkParametersService, validatedTransactions)

    internal fun makeVaultService(schemaService: SchemaService, database: CordaPersistence, cordappLoader: CordappLoader): VaultServiceInternal {
        return NodeVaultService(clock, keyManagementService, servicesForResolution, database, schemaService, cordappLoader.appClassLoader).apply { start() }
    }

    // This needs to be internal as MutableClassToInstanceMap is a guava type and shouldn't be part of our public API
    /** A map of available [CordaService] implementations */
    internal val cordappServices: MutableClassToInstanceMap<SerializeAsToken> = MutableClassToInstanceMap.create<SerializeAsToken>()

    override fun <T : SerializeAsToken> cordaService(type: Class<T>): T {
        require(type.isAnnotationPresent(CordaService::class.java)) { "${type.name} is not a Corda service" }
        return cordappServices.getInstance(type)
                ?: throw IllegalArgumentException("Corda service ${type.name} does not exist")
    }

    override fun jdbcSession(): Connection = throw UnsupportedOperationException()

    override fun <T : Any?> withEntityManager(block: EntityManager.() -> T): T {
        throw UnsupportedOperationException()
    }

    override fun withEntityManager(block: Consumer<EntityManager>) {
        throw UnsupportedOperationException()
    }

    override fun registerUnloadHandler(runOnStop: () -> Unit) = throw UnsupportedOperationException()

    /** Add the given package name to the list of packages which will be scanned for cordapp contract verification code */
    fun addMockCordapp(contractClassName: ContractClassName) {
        mockCordappProvider.addMockCordapp(contractClassName, attachments)
    }

    override fun loadState(stateRef: StateRef) = servicesForResolution.loadState(stateRef)
    override fun loadStates(stateRefs: Set<StateRef>) = servicesForResolution.loadStates(stateRefs)

    /** Returns a dummy Attachment, in context of signature constrains non-downgrade rule this default to contract class version `1`. */
    override fun loadContractAttachment(stateRef: StateRef) = dummyAttachment
}

/**
 * Function which can be used to create a mock [CordaService] for use within testing, such as an Oracle.
 */
fun <T : SerializeAsToken> createMockCordaService(serviceHub: MockServices, serviceConstructor: (AppServiceHub) -> T): T {
    class MockAppServiceHubImpl<out T : SerializeAsToken>(val serviceHub: MockServices, serviceConstructor: (AppServiceHub) -> T) : AppServiceHub, ServiceHub by serviceHub {
        val serviceInstance: T = serviceConstructor(this)

        init {
            serviceHub.cordappServices.putInstance(serviceInstance.javaClass, serviceInstance)
        }

        override fun <T> startFlow(flow: FlowLogic<T>): FlowHandle<T> {
            throw UnsupportedOperationException()
        }

        override fun <T> startTrackedFlow(flow: FlowLogic<T>): FlowProgressHandle<T> {
            throw UnsupportedOperationException()
        }

        override val database: CordaTransactionSupport
            get() = throw UnsupportedOperationException()

        override fun register(priority: Int, observer: ServiceLifecycleObserver) {
            throw UnsupportedOperationException()
        }
    }
    return MockAppServiceHubImpl(serviceHub, serviceConstructor).serviceInstance
}
