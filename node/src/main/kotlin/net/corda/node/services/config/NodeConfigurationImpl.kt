package net.corda.node.services.config

import com.typesafe.config.ConfigException
import net.corda.common.configuration.parsing.internal.ConfigurationWithOptions
import net.corda.core.identity.CordaX500Name
import net.corda.core.internal.div
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.loggerFor
import net.corda.core.utilities.seconds
import net.corda.node.services.config.rpc.NodeRpcOptions
import net.corda.nodeapi.BrokerRpcSslOptions
import net.corda.nodeapi.internal.DEV_PUB_KEY_HASHES
import net.corda.nodeapi.internal.config.FileBasedCertificateStoreSupplier
import net.corda.nodeapi.internal.config.MutualSslConfiguration
import net.corda.nodeapi.internal.config.SslConfiguration
import net.corda.nodeapi.internal.config.User
import net.corda.nodeapi.internal.persistence.DatabaseConfig
import net.corda.tools.shell.SSHDConfiguration
import java.net.URL
import java.nio.file.Path
import java.time.Duration
import java.util.*
import javax.security.auth.x500.X500Principal

data class NodeConfigurationImpl(
        /** This is not retrieved from the config file but rather from a command line argument. */
        override val baseDirectory: Path,
        override val myLegalName: CordaX500Name,
        override val jmxMonitoringHttpPort: Int? = Defaults.jmxMonitoringHttpPort,
        override val emailAddress: String,
        private val keyStorePassword: String,
        private val trustStorePassword: String,
        override val crlCheckSoftFail: Boolean,
        override val crlCheckArtemisServer: Boolean = Defaults.crlCheckArtemisServer,
        override val dataSourceProperties: Properties,
        override val compatibilityZoneURL: URL? = Defaults.compatibilityZoneURL,
        override var networkServices: NetworkServicesConfig? = Defaults.networkServices,
        override val tlsCertCrlDistPoint: URL? = Defaults.tlsCertCrlDistPoint,
        override val tlsCertCrlIssuer: X500Principal? = Defaults.tlsCertCrlIssuer,
        override val rpcUsers: List<User>,
        override val security: SecurityConfiguration? = Defaults.security,
        override val verifierType: VerifierType,
        override val flowTimeout: FlowTimeoutConfiguration,
        override val p2pAddress: NetworkHostAndPort,
        override val additionalP2PAddresses: List<NetworkHostAndPort> = Defaults.additionalP2PAddresses,
        private val rpcAddress: NetworkHostAndPort? = Defaults.rpcAddress,
        private val rpcSettings: NodeRpcSettings,
        override val messagingServerAddress: NetworkHostAndPort?,
        override val messagingServerExternal: Boolean = Defaults.messagingServerExternal(messagingServerAddress),
        override val notary: NotaryConfig?,
        @Suppress("DEPRECATION")
        @Deprecated("Do not configure")
        override val certificateChainCheckPolicies: List<CertChainPolicyConfig> = Defaults.certificateChainCheckPolicies,
        override val devMode: Boolean = Defaults.devMode,
        override val noLocalShell: Boolean = Defaults.noLocalShell,
        override val devModeOptions: DevModeOptions? = Defaults.devModeOptions,
        override val useTestClock: Boolean = Defaults.useTestClock,
        override val lazyBridgeStart: Boolean = Defaults.lazyBridgeStart,
        override val detectPublicIp: Boolean = Defaults.detectPublicIp,
        // TODO See TODO above. Rename this to nodeInfoPollingFrequency and make it of type Duration
        override val additionalNodeInfoPollingFrequencyMsec: Long = Defaults.additionalNodeInfoPollingFrequencyMsec,
        override val sshd: SSHDConfiguration? = Defaults.sshd,
        override val localShellAllowExitInSafeMode: Boolean = Defaults.localShellAllowExitInSafeMode,
        override val localShellUnsafe: Boolean = Defaults.localShellUnsafe,
        override val database: DatabaseConfig = Defaults.database(devMode),
        private val transactionCacheSizeMegaBytes: Int? = Defaults.transactionCacheSizeMegaBytes,
        private val attachmentContentCacheSizeMegaBytes: Int? = Defaults.attachmentContentCacheSizeMegaBytes,
        override val attachmentCacheBound: Long = Defaults.attachmentCacheBound,
        override val extraNetworkMapKeys: List<UUID> = Defaults.extraNetworkMapKeys,
        // do not use or remove (breaks DemoBench together with rejection of unknown configuration keys during parsing)
        private val h2port: Int? = Defaults.h2port,
        private val h2Settings: NodeH2Settings? = Defaults.h2Settings,
        // do not use or remove (used by Capsule)
        private val jarDirs: List<String> = Defaults.jarDirs,
        override val flowMonitorPeriodMillis: Duration = Defaults.flowMonitorPeriodMillis,
        override val flowMonitorSuspensionLoggingThresholdMillis: Duration = Defaults.flowMonitorSuspensionLoggingThresholdMillis,
        override val cordappDirectories: List<Path> = Defaults.cordappsDirectories(baseDirectory),
        override val jmxReporterType: JmxReporterType? = Defaults.jmxReporterType,
        override val flowOverrides: FlowOverrideConfig?,
        override val cordappSignerKeyFingerprintBlacklist: List<String> = Defaults.cordappSignerKeyFingerprintBlacklist,
        override val networkParameterAcceptanceSettings: NetworkParameterAcceptanceSettings? =
                Defaults.networkParameterAcceptanceSettings,
        override val blacklistedAttachmentSigningKeys: List<String> = Defaults.blacklistedAttachmentSigningKeys,
        override val configurationWithOptions: ConfigurationWithOptions,
        override val flowExternalOperationThreadPoolSize: Int = Defaults.flowExternalOperationThreadPoolSize,
        override val quasarExcludePackages: List<String> = Defaults.quasarExcludePackages,
        override val reloadCheckpointAfterSuspend: Boolean = Defaults.reloadCheckpointAfterSuspend,
        override val customTransitionInterceptor: String? = Defaults.customTransitionInterceptor

) : NodeConfiguration {
    internal object Defaults {
        val jmxMonitoringHttpPort: Int? = null
        val compatibilityZoneURL: URL? = null
        val networkServices: NetworkServicesConfig? = null
        val tlsCertCrlDistPoint: URL? = null
        val tlsCertCrlIssuer: X500Principal? = null
        const val crlCheckArtemisServer: Boolean = false
        val security: SecurityConfiguration? = null
        val additionalP2PAddresses: List<NetworkHostAndPort> = emptyList()
        val rpcAddress: NetworkHostAndPort? = null
        @Suppress("DEPRECATION")
        val certificateChainCheckPolicies: List<CertChainPolicyConfig> = emptyList()
        const val devMode: Boolean = false
        const val noLocalShell: Boolean = false
        val devModeOptions: DevModeOptions? = null
        const val useTestClock: Boolean = false
        const val lazyBridgeStart: Boolean = true
        const val detectPublicIp: Boolean = false
        val additionalNodeInfoPollingFrequencyMsec: Long = 5.seconds.toMillis()
        val sshd: SSHDConfiguration? = null
        const val localShellAllowExitInSafeMode: Boolean = true
        const val localShellUnsafe: Boolean = false
        val transactionCacheSizeMegaBytes: Int? = null
        val attachmentContentCacheSizeMegaBytes: Int? = null
        const val attachmentCacheBound: Long = NodeConfiguration.defaultAttachmentCacheBound
        val extraNetworkMapKeys: List<UUID> = emptyList()
        val h2port: Int? = null
        val h2Settings: NodeH2Settings? = null
        val jarDirs: List<String> = emptyList()
        val flowMonitorPeriodMillis: Duration = NodeConfiguration.DEFAULT_FLOW_MONITOR_PERIOD_MILLIS
        val flowMonitorSuspensionLoggingThresholdMillis: Duration = NodeConfiguration.DEFAULT_FLOW_MONITOR_SUSPENSION_LOGGING_THRESHOLD_MILLIS
        val jmxReporterType: JmxReporterType = NodeConfiguration.defaultJmxReporterType
        val cordappSignerKeyFingerprintBlacklist: List<String> = DEV_PUB_KEY_HASHES.map { it.toString() }
        val networkParameterAcceptanceSettings: NetworkParameterAcceptanceSettings = NetworkParameterAcceptanceSettings()
        val blacklistedAttachmentSigningKeys: List<String> = emptyList()
        const val flowExternalOperationThreadPoolSize: Int = 1
        val quasarExcludePackages: List<String> = emptyList()
        val reloadCheckpointAfterSuspend: Boolean = System.getProperty("reloadCheckpointAfterSuspend", "false")!!.toBoolean()
        val customTransitionInterceptor: String? = null

        fun cordappsDirectories(baseDirectory: Path) = listOf(baseDirectory / CORDAPPS_DIR_NAME_DEFAULT)

        fun messagingServerExternal(messagingServerAddress: NetworkHostAndPort?) = messagingServerAddress != null

        fun database(devMode: Boolean) = DatabaseConfig(
                exportHibernateJMXStatistics = devMode
        )
    }

    companion object {
        private const val CORDAPPS_DIR_NAME_DEFAULT = "cordapps"

        private val logger = loggerFor<NodeConfigurationImpl>()
    }

    private val actualRpcSettings: NodeRpcSettings

    init {
        actualRpcSettings = when {
            rpcAddress != null -> {
                require(rpcSettings.address == null) { "Can't provide top-level rpcAddress and rpcSettings.address (they control the same property)." }
                logger.warn("Top-level declaration of property 'rpcAddress' is deprecated. Please use 'rpcSettings.address' instead.")

                rpcSettings.copy(address = rpcAddress)
            }
            else -> {
                rpcSettings.address ?: throw ConfigException.Missing("rpcSettings.address")
                rpcSettings
            }
        }

        // This is a sanity feature do not remove.
        require(!useTestClock || devMode) { "Cannot use test clock outside of dev mode" }
        require(devModeOptions == null || devMode) { "Cannot use devModeOptions outside of dev mode" }
        require(security == null || rpcUsers.isEmpty()) {
            "Cannot specify both 'rpcUsers' and 'security' in configuration"
        }
        @Suppress("DEPRECATION")
        if (certificateChainCheckPolicies.isNotEmpty()) {
            logger.warn("""You are configuring certificateChainCheckPolicies. This is a setting that is not used, and will be removed in a future version.
                |Please contact the R3 team on the public Slack to discuss your use case.
            """.trimMargin())
        }

        // Support the deprecated method of configuring network services with a single compatibilityZoneURL option
        @Suppress("DEPRECATION")
        if (compatibilityZoneURL != null && networkServices == null) {
            networkServices = NetworkServicesConfig(compatibilityZoneURL, compatibilityZoneURL, inferred = true)
        }
        require(h2port == null || h2Settings == null) { "Cannot specify both 'h2port' and 'h2Settings' in configuration" }
    }

    override val certificatesDirectory = baseDirectory / "certificates"

    private val signingCertificateStorePath = certificatesDirectory / "nodekeystore.jks"
    private val p2pKeystorePath: Path get() = certificatesDirectory / "sslkeystore.jks"

    // TODO: There are two implications here:
    // 1. "signingCertificateStore" and "p2pKeyStore" have the same passwords. In the future we should re-visit this "rule" and see of they can be made different;
    // 2. The passwords for store and for keys in this store are the same, this is due to limitations of Artemis.
    override val signingCertificateStore = FileBasedCertificateStoreSupplier(signingCertificateStorePath, keyStorePassword, keyStorePassword)
    private val p2pKeyStore = FileBasedCertificateStoreSupplier(p2pKeystorePath, keyStorePassword, keyStorePassword)

    private val p2pTrustStoreFilePath: Path get() = certificatesDirectory / "truststore.jks"
    private val p2pTrustStore = FileBasedCertificateStoreSupplier(p2pTrustStoreFilePath, trustStorePassword, trustStorePassword)
    override val p2pSslOptions: MutualSslConfiguration = SslConfiguration.mutual(p2pKeyStore, p2pTrustStore)

    override val rpcOptions: NodeRpcOptions
        get() {
            return actualRpcSettings.asOptions()
        }

    override val transactionCacheSizeBytes: Long
        get() = transactionCacheSizeMegaBytes?.MB ?: super.transactionCacheSizeBytes
    override val attachmentContentCacheSizeBytes: Long
        get() = attachmentContentCacheSizeMegaBytes?.MB ?: super.attachmentContentCacheSizeBytes

    override val effectiveH2Settings: NodeH2Settings?
        get() = when {
            h2port != null -> NodeH2Settings(address = NetworkHostAndPort(host = "localhost", port = h2port))
            else -> h2Settings
        }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()
        errors += validateDevModeOptions()
        val rpcSettingsErrors = validateRpcSettings(rpcSettings)
        errors += rpcSettingsErrors
        if (rpcSettingsErrors.isEmpty()) {
            // Forces lazy property to initialise in order to throw exceptions
            rpcOptions
        }
        errors += validateTlsCertCrlConfig()
        errors += validateNetworkServices()
        errors += validateH2Settings()
        return errors
    }

    private fun validateTlsCertCrlConfig(): List<String> {
        val errors = mutableListOf<String>()
        if (tlsCertCrlIssuer != null) {
            if (tlsCertCrlDistPoint == null) {
                errors += "'tlsCertCrlDistPoint' is mandatory when 'tlsCertCrlIssuer' is specified"
            }
        }
        if (!crlCheckSoftFail && tlsCertCrlDistPoint == null) {
            errors += "'tlsCertCrlDistPoint' is mandatory when 'crlCheckSoftFail' is false"
        }
        return errors
    }

    private fun validateH2Settings(): List<String> {
        val errors = mutableListOf<String>()
        if (h2port != null && h2Settings != null) {
            errors += "cannot specify both 'h2port' and 'h2Settings'"
        }
        return errors
    }

    private fun validateRpcSettings(options: NodeRpcSettings): List<String> {
        val errors = mutableListOf<String>()
        if (options.adminAddress == null) {
            errors += "'rpcSettings.adminAddress' is mandatory"
        }
        if (options.useSsl && options.ssl == null) {
            errors += "'rpcSettings.ssl' is mandatory when 'rpcSettings.useSsl' is specified"
        }
        return errors
    }

    private fun validateDevModeOptions(): List<String> {
        if (devMode) {
            @Suppress("DEPRECATION")
            compatibilityZoneURL?.let {
                if (devModeOptions?.allowCompatibilityZone != true) {
                    return listOf("cannot specify 'compatibilityZoneURL' when 'devMode' is true, unless 'devModeOptions.allowCompatibilityZone' is also true")
                }
            }

            // if compatibilityZoneURL is set then it will be copied into the networkServices field and thus skipping
            // this check by returning above is fine.
            networkServices?.let {
                if (devModeOptions?.allowCompatibilityZone != true) {
                    return listOf("cannot specify 'networkServices' when 'devMode' is true, unless 'devModeOptions.allowCompatibilityZone' is also true")
                }
            }
        }
        return emptyList()
    }

    private fun validateNetworkServices(): List<String> {
        val errors = mutableListOf<String>()

        @Suppress("DEPRECATION")
        if (compatibilityZoneURL != null && networkServices != null && !(networkServices!!.inferred)) {
            errors += "cannot specify both 'compatibilityZoneUrl' and 'networkServices'"
        }

        return errors
    }
}

data class NodeRpcSettings(
        val address: NetworkHostAndPort?,
        val adminAddress: NetworkHostAndPort?,
        val standAloneBroker: Boolean = Defaults.standAloneBroker,
        val useSsl: Boolean = Defaults.useSsl,
        val ssl: BrokerRpcSslOptions?
) {
    internal object Defaults {
        val standAloneBroker = false
        val useSsl = false
    }

    fun asOptions(): NodeRpcOptions {
        return object : NodeRpcOptions {
            override val address = this@NodeRpcSettings.address!!
            override val adminAddress = this@NodeRpcSettings.adminAddress!!
            override val standAloneBroker = this@NodeRpcSettings.standAloneBroker
            override val useSsl = this@NodeRpcSettings.useSsl
            override val sslConfig = this@NodeRpcSettings.ssl

            override fun toString(): String {
                return "address: $address, adminAddress: $adminAddress, standAloneBroker: $standAloneBroker, useSsl: $useSsl, sslConfig: $sslConfig"
            }
        }
    }
}