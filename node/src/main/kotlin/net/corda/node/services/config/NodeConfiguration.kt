package net.corda.node.services.config

import com.typesafe.config.Config
import net.corda.common.configuration.parsing.internal.Configuration
import net.corda.common.configuration.parsing.internal.ConfigurationWithOptionsContainer
import net.corda.common.validation.internal.Validated
import net.corda.core.context.AuthServiceId
import net.corda.core.identity.CordaX500Name
import net.corda.core.internal.TimedFlow
import net.corda.core.internal.notary.NotaryServiceFlow
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.node.services.config.rpc.NodeRpcOptions
import net.corda.node.services.config.schema.v1.V1NodeConfigurationSpec
import net.corda.nodeapi.internal.config.FileBasedCertificateStoreSupplier
import net.corda.nodeapi.internal.config.MutualSslConfiguration
import net.corda.nodeapi.internal.config.User
import net.corda.nodeapi.internal.persistence.DatabaseConfig
import net.corda.notary.experimental.bftsmart.BFTSmartConfig
import net.corda.notary.experimental.raft.RaftConfig
import net.corda.tools.shell.SSHDConfiguration
import java.net.URL
import java.nio.file.Path
import java.time.Duration
import java.util.*
import javax.security.auth.x500.X500Principal

val Int.MB: Long get() = this * 1024L * 1024L

interface NodeConfiguration : ConfigurationWithOptionsContainer {
    val myLegalName: CordaX500Name
    val emailAddress: String
    val jmxMonitoringHttpPort: Int?
    val dataSourceProperties: Properties
    val rpcUsers: List<User>
    val security: SecurityConfiguration?
    val devMode: Boolean
    val devModeOptions: DevModeOptions?
    @Deprecated(message = "Use of single compatibility zone URL is deprecated", replaceWith = ReplaceWith("networkServices.networkMapURL"))
    val compatibilityZoneURL: URL?
    val networkServices: NetworkServicesConfig?
    @Suppress("DEPRECATION")
    val certificateChainCheckPolicies: List<CertChainPolicyConfig>
    val verifierType: VerifierType
    val flowTimeout: FlowTimeoutConfiguration
    val notary: NotaryConfig?
    val additionalNodeInfoPollingFrequencyMsec: Long
    val p2pAddress: NetworkHostAndPort
    val additionalP2PAddresses: List<NetworkHostAndPort>
    val rpcOptions: NodeRpcOptions
    val messagingServerAddress: NetworkHostAndPort?
    val messagingServerExternal: Boolean
    // TODO Move into DevModeOptions
    val useTestClock: Boolean get() = false
    val lazyBridgeStart: Boolean
    val detectPublicIp: Boolean get() = false
    val sshd: SSHDConfiguration?
    val localShellAllowExitInSafeMode: Boolean
    val localShellUnsafe: Boolean
    val database: DatabaseConfig
    val noLocalShell: Boolean get() = false
    val transactionCacheSizeBytes: Long get() = defaultTransactionCacheSize
    val attachmentContentCacheSizeBytes: Long get() = defaultAttachmentContentCacheSize
    val attachmentCacheBound: Long get() = defaultAttachmentCacheBound
    // do not change this value without syncing it with ScheduledFlowsDrainingModeTest
    val drainingModePollPeriod: Duration get() = Duration.ofSeconds(5)
    val extraNetworkMapKeys: List<UUID>
    val tlsCertCrlDistPoint: URL?
    val tlsCertCrlIssuer: X500Principal?
    val effectiveH2Settings: NodeH2Settings?
    val flowMonitorPeriodMillis: Duration get() = DEFAULT_FLOW_MONITOR_PERIOD_MILLIS
    val flowMonitorSuspensionLoggingThresholdMillis: Duration get() = DEFAULT_FLOW_MONITOR_SUSPENSION_LOGGING_THRESHOLD_MILLIS
    val crlCheckSoftFail: Boolean
    val crlCheckArtemisServer: Boolean
    val jmxReporterType: JmxReporterType? get() = defaultJmxReporterType

    val baseDirectory: Path
    val certificatesDirectory: Path
    // signingCertificateStore is used to store certificate chains.
    // However, BCCryptoService is reusing this to store keys as well.
    val signingCertificateStore: FileBasedCertificateStoreSupplier
    val p2pSslOptions: MutualSslConfiguration

    val cordappDirectories: List<Path>
    val flowOverrides: FlowOverrideConfig?

    val cordappSignerKeyFingerprintBlacklist: List<String>

    val networkParameterAcceptanceSettings: NetworkParameterAcceptanceSettings?

    val blacklistedAttachmentSigningKeys: List<String>

    val flowExternalOperationThreadPoolSize: Int

    val quasarExcludePackages: List<String>

    val reloadCheckpointAfterSuspend: Boolean

    val customTransitionInterceptor: String?

    companion object {
        // default to at least 8MB and a bit extra for larger heap sizes
        val defaultTransactionCacheSize: Long = 8.MB + getAdditionalCacheMemory()

        internal val DEFAULT_FLOW_MONITOR_PERIOD_MILLIS: Duration = Duration.ofMinutes(1)
        internal val DEFAULT_FLOW_MONITOR_SUSPENSION_LOGGING_THRESHOLD_MILLIS: Duration = Duration.ofMinutes(1)

        // add 5% of any heapsize over 300MB to the default transaction cache size
        private fun getAdditionalCacheMemory(): Long {
            return Math.max((Runtime.getRuntime().maxMemory() - 300.MB) / 20, 0)
        }

        internal val defaultAttachmentContentCacheSize: Long = 10.MB
        internal const val defaultAttachmentCacheBound = 1024L

        const val cordappDirectoriesKey = "cordappDirectories"

        internal val defaultJmxReporterType = JmxReporterType.JOLOKIA
    }
}

data class FlowOverrideConfig(val overrides: List<FlowOverride> = listOf())
data class FlowOverride(val initiator: String, val responder: String)

/**
 * Currently registered JMX Reporters.
 */
enum class JmxReporterType {
    JOLOKIA, NEW_RELIC
}

data class DevModeOptions(
    @Deprecated(
        "The checkpoint checker has been replaced by the ability to reload a checkpoint from the database after every suspend" +
                "Use [NodeConfiguration.disableReloadCheckpointAfterSuspend] instead."
    )
    val disableCheckpointChecker: Boolean = Defaults.disableCheckpointChecker,
    val allowCompatibilityZone: Boolean = Defaults.allowCompatibilityZone,
    val djvm: DJVMOptions? = null
) {
    internal object Defaults {
        val disableCheckpointChecker = false
        val allowCompatibilityZone = false
    }
}

data class DJVMOptions(
   val bootstrapSource: String?,
   val cordaSource: List<String>
)

fun NodeConfiguration.shouldStartSSHDaemon() = this.sshd != null
fun NodeConfiguration.shouldStartLocalShell() = !this.noLocalShell && System.console() != null && this.devMode
fun NodeConfiguration.shouldInitCrashShell() = shouldStartLocalShell() || shouldStartSSHDaemon()

data class NotaryConfig(
        /** Specifies whether the notary validates transactions or not. */
        val validating: Boolean,
        /** The legal name of the notary service identity. */
        val serviceLegalName: CordaX500Name? = null,
        /** The name of the notary service class to load. */
        val className: String? = null,
        /**
         * If the wait time estimate on the internal queue exceeds this value, the notary may send
         * a wait time update to the client (implementation specific and dependent on the counter
         * party version).
         */
        val etaMessageThresholdSeconds: Int = NotaryServiceFlow.defaultEstimatedWaitTime.seconds.toInt(),
        /** Notary implementation-specific configuration parameters. */
        val extraConfig: Config? = null,
        val raft: RaftConfig? = null,
        val bftSMaRt: BFTSmartConfig? = null
)

/**
 * Used as an alternative to the older compatibilityZoneURL to allow the doorman and network map
 * services for a node to be configured as different URLs. Cannot be set at the same time as the
 * compatibilityZoneURL, and will be defaulted (if not set) to both point at the configured
 * compatibilityZoneURL.
 *
 * @property doormanURL The URL of the tls certificate signing service.
 * @property networkMapURL The URL of the Network Map service.
 * @property pnm If the compatibility zone operator supports the private network map option, have the node
 * at registration automatically join that private network.
 * @property inferred Non user setting that indicates weather the Network Services configuration was
 * set explicitly ([inferred] == false) or weather they have been inferred via the compatibilityZoneURL parameter
 * ([inferred] == true) where both the network map and doorman are running on the same endpoint. Only one,
 * compatibilityZoneURL or networkServices, can be set at any one time.
 * @property csrToken Optional token to provide alongside the certificate signing request (CSR) as part of the HTTP header during
 * node registration.
 */
data class NetworkServicesConfig(
        val doormanURL: URL,
        val networkMapURL: URL,
        val pnm: UUID? = null,
        val inferred: Boolean = false,
        val csrToken: String? = null
)

/**
 * Specifies the auto-acceptance behaviour for network parameter updates
 *
 * @property autoAcceptEnabled Specifies whether network parameter auto-accepting is enabled. Only parameters annotated with the
 * AutoAcceptable annotation can be auto-accepted.
 * @property excludedAutoAcceptableParameters Set of parameters to explicitly exclude from auto-accepting. Note that if [autoAcceptEnabled]
 * is false then this parameter is redundant.
 */
data class NetworkParameterAcceptanceSettings(
        val autoAcceptEnabled: Boolean = true,
        val excludedAutoAcceptableParameters: Set<String> = emptySet()
)

/**
 * Currently only used for notarisation requests.
 *
 * Specifies the configuration for timing out and restarting a [TimedFlow].
 */
data class FlowTimeoutConfiguration(
        val timeout: Duration,
        val maxRestartCount: Int,
        val backoffBase: Double
)

internal typealias Valid<TARGET> = Validated<TARGET, Configuration.Validation.Error>

fun Config.parseAsNodeConfiguration(options: Configuration.Options = Configuration.Options(strict = true)): Valid<NodeConfiguration> = V1NodeConfigurationSpec.parse(this, options)

data class NodeH2Settings(
        val address: NetworkHostAndPort?
)

enum class VerifierType {
    InMemory
}

enum class CertChainPolicyType {
    Any,
    RootMustMatch,
    LeafMustMatch,
    MustContainOneOf,
    UsernameMustMatch
}

@Deprecated("Do not use")
data class CertChainPolicyConfig(val role: String, private val policy: CertChainPolicyType, private val trustedAliases: Set<String>)

// Supported types of authentication/authorization data providers
enum class AuthDataSourceType {
    // External RDBMS
    DB,

    // Static dataset hard-coded in config
    INMEMORY
}

// Password encryption scheme
enum class PasswordEncryption {

    // Password stored in clear
    NONE,

    // Password salt-hashed using Apache Shiro flexible encryption format
    // [org.apache.shiro.crypto.hash.format.Shiro1CryptFormat]
    SHIRO_1_CRYPT
}

// Subset of Node configuration related to security aspects
data class SecurityConfiguration(val authService: SecurityConfiguration.AuthService) {

    // Configure RPC/Shell users authentication/authorization service
    data class AuthService(val dataSource: AuthService.DataSource,
                           val id: AuthServiceId = defaultAuthServiceId(dataSource.type),
                           val options: AuthService.Options? = null) {

        init {
            require(!(dataSource.type == AuthDataSourceType.INMEMORY &&
                    options?.cache != null)) {
                "No cache supported for INMEMORY data provider"
            }
        }

        // Optional components: cache
        data class Options(val cache: Options.Cache?) {

            // Cache parameters
            data class Cache(val expireAfterSecs: Long, val maxEntries: Long) {
                init {
                    require(expireAfterSecs >= 0) {
                        "Expected positive value for 'cache.expireAfterSecs'"
                    }
                    require(maxEntries > 0) {
                        "Expected positive value for 'cache.maxEntries'"
                    }
                }
            }
        }

        // Provider of users credentials and permissions data
        data class DataSource(val type: AuthDataSourceType,
                              val passwordEncryption: PasswordEncryption = Defaults.passwordEncryption,
                              val connection: Properties? = null,
                              val users: List<User>? = null) {
            init {
                when (type) {
                    AuthDataSourceType.INMEMORY -> require(users != null && connection == null) { "In-memory authentication must specify a user list, and must not configure a database" }
                    AuthDataSourceType.DB -> require(users == null && connection != null) { "Database-backed authentication must not specify a user list, and must configure a database" }
                }
            }

            internal object Defaults {
                val passwordEncryption = PasswordEncryption.NONE
            }
        }

        companion object {
            // If unspecified, we assign an AuthServiceId by default based on the
            // underlying data provider
            fun defaultAuthServiceId(type: AuthDataSourceType) = when (type) {
                AuthDataSourceType.INMEMORY -> AuthServiceId("NODE_CONFIG")
                AuthDataSourceType.DB -> AuthServiceId("REMOTE_DATABASE")
            }

            fun fromUsers(users: List<User>, encryption: PasswordEncryption = PasswordEncryption.NONE) =
                    AuthService(
                            dataSource = DataSource(
                                    type = AuthDataSourceType.INMEMORY,
                                    users = users,
                                    passwordEncryption = encryption),
                            id = AuthServiceId("NODE_CONFIG"))
        }
    }
}
