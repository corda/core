package net.corda.nodeapi.internal.network

import com.typesafe.config.Config
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory
import net.corda.common.configuration.parsing.internal.Configuration
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.*
import net.corda.core.internal.concurrent.fork
import net.corda.core.internal.concurrent.transpose
import net.corda.core.node.NetworkParameters
import net.corda.core.node.NodeInfo
import net.corda.core.node.NotaryInfo
import net.corda.core.node.services.AttachmentId
import net.corda.core.serialization.SerializationContext
import net.corda.core.serialization.SerializedBytes
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.internal.SerializationEnvironment
import net.corda.core.serialization.internal._contextSerializationEnv
import net.corda.core.utilities.days
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.seconds
import net.corda.nodeapi.internal.*
import net.corda.nodeapi.internal.config.getBooleanCaseInsensitive
import net.corda.nodeapi.internal.network.NodeInfoFilesCopier.Companion.NODE_INFO_FILE_NAME_PREFIX
import net.corda.serialization.internal.AMQP_P2P_CONTEXT
import net.corda.serialization.internal.CordaSerializationMagic
import net.corda.serialization.internal.SerializationFactoryImpl
import net.corda.serialization.internal.amqp.AbstractAMQPSerializationScheme
import net.corda.serialization.internal.amqp.amqpMagic
import java.io.File
import java.net.URL
import java.nio.file.FileAlreadyExistsException
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.security.PublicKey
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.jar.JarInputStream
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.set
import kotlin.concurrent.schedule
import kotlin.streams.toList

/**
 * Class to bootstrap a local network of Corda nodes on the same filesystem.
 */
// TODO Move this to tools:bootstrapper
class NetworkBootstrapper
@VisibleForTesting
constructor(private val initSerEnv: Boolean,
                     private val embeddedCordaJar: () -> URL,
                     private val nodeInfosGenerator: (List<Path>) -> List<Path>,
                     private val contractsJarConverter: (Path, Boolean) -> ContractsJar) : NetworkBootstrapperWithOverridableParameters {

    constructor() : this(
            initSerEnv = true,
            embeddedCordaJar = ::extractEmbeddedCordaJar,
            nodeInfosGenerator = ::generateNodeInfos,
            contractsJarConverter = ::ContractsJarFile
    )

    companion object {
        // TODO This will probably need to change once we start using a bundled JVM
        private val nodeInfoGenCmd = listOf(
                Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                "-jar",
                "corda.jar",
                "generate-node-info"
        )

        private const val LOGS_DIR_NAME = "logs"

        private val jarsThatArentCordapps = setOf("corda.jar", "runnodes.jar")

        private fun extractEmbeddedCordaJar(): URL {
            return Thread.currentThread().contextClassLoader.getResource("corda.jar")
        }

        private fun generateNodeInfos(nodeDirs: List<Path>): List<Path> {
            val numParallelProcesses = Runtime.getRuntime().availableProcessors()
            val timePerNode = 40.seconds // On the test machine, generating the node info takes 7 seconds for a single node.
            val tExpected = maxOf(timePerNode, timePerNode * nodeDirs.size.toLong() / numParallelProcesses.toLong())
            val warningTimer = Timer("WarnOnSlowMachines", true).schedule(tExpected.toMillis()) {
                println("... still waiting. If this is taking longer than usual, check the node logs.")
            }
            val executor = Executors.newFixedThreadPool(numParallelProcesses)
            return try {
                nodeDirs.map { executor.fork { generateNodeInfo(it) } }.transpose().getOrThrow()
            } finally {
                warningTimer.cancel()
                executor.shutdownNow()
            }
        }

        private fun generateNodeInfo(nodeDir: Path): Path {
            val logsDir = (nodeDir / LOGS_DIR_NAME).createDirectories()
            val nodeInfoGenFile = (logsDir / "node-info-gen.log").toFile()
            val process = ProcessBuilder(nodeInfoGenCmd)
                    .directory(nodeDir.toFile())
                    .redirectErrorStream(true)
                    .redirectOutput(nodeInfoGenFile)
                    .apply { environment()["CAPSULE_CACHE_DIR"] = "../.cache" }
                    .start()
            try {
                if (!process.waitFor(3, TimeUnit.MINUTES)) {
                    process.destroyForcibly()
                    printNodeInfoGenLogToConsole(nodeInfoGenFile)
                }
                printNodeInfoGenLogToConsole(nodeInfoGenFile) { process.exitValue() == 0 }
                return nodeDir.list { paths ->
                    paths.filter { it.fileName.toString().startsWith(NODE_INFO_FILE_NAME_PREFIX) }.findFirst().get()
                }
            } catch (e: InterruptedException) {
                // Don't leave this process dangling if the thread is interrupted.
                process.destroyForcibly()
                throw e
            }
        }

        private fun printNodeInfoGenLogToConsole(nodeInfoGenFile: File, check: (() -> Boolean) = { true }) {
            if (!check.invoke()) {
                val nodeDir = nodeInfoGenFile.parent
                val nodeIdentifier = try {
                    ConfigFactory.parseFile((nodeDir / "node.conf").toFile()).getString("myLegalName")
                } catch (e: ConfigException) {
                    nodeDir
                }
                System.err.println("#### Error while generating node info file $nodeIdentifier ####")
                nodeInfoGenFile.inputStream().copyTo(System.err)
                throw IllegalStateException("Error while generating node info file. Please check the logs in $nodeDir.")
            }
        }

        const val DEFAULT_MAX_MESSAGE_SIZE: Int = 10485760
        const val DEFAULT_MAX_TRANSACTION_SIZE: Int = 524288000
    }

    sealed class NotaryCluster {
        data class BFT(val name: CordaX500Name) : NotaryCluster()
        data class CFT(val name: CordaX500Name) : NotaryCluster()
    }

    data class DirectoryAndConfig(val directory: Path, val config: Config)

    private fun notaryClusters(configs: Map<Path, Config>): Map<NotaryCluster, List<Path>> {
        val clusteredNotaries = configs.flatMap { (path, config) ->
            if (config.hasPath("notary.serviceLegalName")) {
                listOf(CordaX500Name.parse(config.getString("notary.serviceLegalName")) to DirectoryAndConfig(path, config))
            } else {
                emptyList()
            }
        }
        return clusteredNotaries.groupBy { it.first }.map { (k, vs) ->
            val cs = vs.map { it.second.config }
            if (cs.any { isBFTNotary(it) }) {
                require(cs.all { isBFTNotary(it) }) { "Mix of BFT and non-BFT notaries with service name $k" }
                NotaryCluster.BFT(k) to vs.map { it.second.directory }
            } else {
                NotaryCluster.CFT(k) to vs.map { it.second.directory }
            }
        }.toMap()
    }

    private fun isBFTNotary(config: Config): Boolean {
        // TODO: pass a commandline parameter to the bootstrapper instead. Better yet, a notary config map
        //       specifying the notary identities and the type (single-node, CFT, BFT) of each notary to set up.
        return config.hasPath("notary.bftSMaRt")
    }

    private fun generateServiceIdentitiesForNotaryClusters(configs: Map<Path, Config>) {
        notaryClusters(configs).forEach { (cluster, directories) ->
            when (cluster) {
                is NotaryCluster.BFT -> DevIdentityGenerator.generateDistributedNotaryCompositeIdentity(
                        directories,
                        cluster.name,
                        threshold = 1 + 2 * directories.size / 3
                )
                is NotaryCluster.CFT -> DevIdentityGenerator.generateDistributedNotarySingularIdentity(directories, cluster.name)
            }
        }
    }

    /** Old Entry point for Cordform
     *
     * TODO: Remove once the gradle plugins are updated to 4.0.30
     */
    fun bootstrap(directory: Path, cordappJars: List<Path>) {
        bootstrap(directory, cordappJars, CopyCordapps.Yes, fromCordform = true)
    }

    /** Entry point for Cordform */
    fun bootstrapCordform(directory: Path, cordappJars: List<Path>) {
        bootstrap(directory, cordappJars, CopyCordapps.No, fromCordform = true)
    }

    /**
     * Entry point for Cordform with extra configurations
     * @param directory - directory on which the network will be deployed
     * @param cordappJars - List of CordApps to deploy
     * @param extraConfigurations - HOCON representation of extra configuration parameters
     */
    fun bootstrapCordform(directory: Path, cordappJars: List<Path>, extraConfigurations: String) {
        val configuration = ConfigFactory.parseString(extraConfigurations).resolve().getObject("networkParameterOverrides").toConfig().parseAsNetworkParametersConfiguration()
        val networkParametersOverrides = configuration.doOnErrors(::reportErrors).optional ?: throw IllegalStateException("Invalid configuration passed.")
        bootstrap(directory, cordappJars, CopyCordapps.No, fromCordform = true, networkParametersOverrides = networkParametersOverrides)
    }

    private fun reportErrors(errors: Set<Configuration.Validation.Error>) {
        System.err.println("Error(s) found parsing the networkParameterOverrides:")
        errors.forEach { System.err.println("Error parsing ${it.pathAsString}: ${it.message}") }
    }

    /** Entry point for the tool */
    override fun bootstrap(directory: Path, copyCordapps: CopyCordapps, networkParameterOverrides: NetworkParametersOverrides) {
        require(networkParameterOverrides.minimumPlatformVersion == null || networkParameterOverrides.minimumPlatformVersion <= PLATFORM_VERSION) { "Minimum platform version cannot be greater than $PLATFORM_VERSION" }
        // Don't accidentally include the bootstrapper jar as a CorDapp!
        val bootstrapperJar = javaClass.location.toPath()
        val cordappJars = directory.list { paths ->
            paths.filter { it.toString().endsWith(".jar") && !it.isSameAs(bootstrapperJar) && !jarsThatArentCordapps.contains(it.fileName.toString().toLowerCase()) }
                    .toList()
        }
        bootstrap(directory, cordappJars, copyCordapps, fromCordform = false, networkParametersOverrides = networkParameterOverrides)
    }

    private fun bootstrap(
            directory: Path,
            cordappJars: List<Path>,
            copyCordapps: CopyCordapps,
            fromCordform: Boolean,
            networkParametersOverrides: NetworkParametersOverrides = NetworkParametersOverrides()
    ) {
        directory.createDirectories()
        println("Bootstrapping local test network in $directory")
        val networkAlreadyExists = createNodeDirectoriesIfNeeded(directory, fromCordform)
        val nodeDirs = gatherNodeDirectories(directory)

        require(nodeDirs.isNotEmpty()) { "No nodes found" }
        if (!fromCordform) {
            println("Nodes found in the following sub-directories: ${nodeDirs.map { it.fileName }}")
        }

        val configs = nodeDirs.associateBy({ it }, { ConfigFactory.parseFile((it / "node.conf").toFile()) })
        checkForDuplicateLegalNames(configs.values)

        copyCordapps.copy(cordappJars, nodeDirs, networkAlreadyExists, fromCordform)

        generateServiceIdentitiesForNotaryClusters(configs)
        if (initSerEnv) {
            initialiseSerialization()
        }
        try {
            println("Waiting for all nodes to generate their node-info files...")
            val nodeInfoFiles = nodeInfosGenerator(nodeDirs)
            println("Distributing all node-info files to all nodes")
            distributeNodeInfos(nodeDirs, nodeInfoFiles)
            print("Loading existing network parameters... ")
            val existingNetParams = loadNetworkParameters(nodeDirs)
            println(existingNetParams ?: "none found")
            println("Gathering notary identities")
            val notaryInfos = gatherNotaryInfos(nodeInfoFiles, configs)
            println("Generating contract implementations whitelist")
            val signedJars = cordappJars.filter { isSigned(it) } // signed JARs are excluded by default, optionally include them in order to transition states from CZ whitelist to signature constraint
            val unsignedJars = cordappJars - signedJars
            val contractsJarConverterWithAdditionalFlag: (Path) -> ContractsJar = { contractsJarConverter(it, false) }
            val newWhitelist = generateWhitelist(existingNetParams, readExcludeWhitelist(directory), unsignedJars.map(contractsJarConverterWithAdditionalFlag),
                    readIncludeWhitelist(directory), signedJars.map(contractsJarConverterWithAdditionalFlag))
            val newNetParams = installNetworkParameters(notaryInfos, newWhitelist, existingNetParams, nodeDirs, networkParametersOverrides)
            if (newNetParams != existingNetParams) {
                println("${if (existingNetParams == null) "New" else "Updated"} $newNetParams")
            } else {
                println("Network parameters unchanged")
            }
            println("Bootstrapping complete!")
        } finally {
            if (initSerEnv) {
                _contextSerializationEnv.set(null)
            }
        }
    }

    private fun Path.listEndingWith(suffix: String): List<Path> {
        return list { file -> file.filter { it.toString().endsWith(suffix) }.toList() }
    }

    private fun createNodeDirectoriesIfNeeded(directory: Path, fromCordform: Boolean): Boolean {
        var networkAlreadyExists = false
        val cordaJar = directory / "corda.jar"
        var usingEmbedded = false
        if (!cordaJar.exists()) {
            embeddedCordaJar().openStream().use { it.copyTo(cordaJar) }
            usingEmbedded = true
        } else if (!fromCordform) {
            println("Using corda.jar in root directory")
        }

        val confFiles = directory.listEndingWith("_node.conf")
        val webServerConfFiles = directory.listEndingWith("_web-server.conf")

        for (confFile in confFiles) {
            val nodeName = confFile.fileName.toString().removeSuffix("_node.conf")
            println("Generating node directory for $nodeName")
            if ((directory / nodeName).exists()) {
                //directory already exists, so assume this network has been bootstrapped before
                networkAlreadyExists = true
            }
            val nodeDir = (directory / nodeName).createDirectories()
            confFile.copyTo(nodeDir / "node.conf", REPLACE_EXISTING)
            webServerConfFiles.firstOrNull { directory.relativize(it).toString().removeSuffix("_web-server.conf") == nodeName }
                    ?.copyTo(nodeDir / "web-server.conf", REPLACE_EXISTING)
            cordaJar.copyToDirectory(nodeDir, REPLACE_EXISTING)
        }

        val nodeDirs = directory.list { subDir -> subDir.filter { (it / "node.conf").exists() && !(it / "corda.jar").exists() }.toList() }
        for (nodeDir in nodeDirs) {
            println("Copying corda.jar into node directory ${nodeDir.fileName}")
            cordaJar.copyToDirectory(nodeDir)
        }

        if (fromCordform) {
            confFiles.forEach(Path::delete)
            webServerConfFiles.forEach(Path::delete)
        }

        if (fromCordform || usingEmbedded) {
            cordaJar.delete()
        }
        return networkAlreadyExists
    }

    private fun gatherNodeDirectories(directory: Path): List<Path> {
        val nodeDirs = directory.list { subDir -> subDir.filter { (it / "corda.jar").exists() }.toList() }
        for (nodeDir in nodeDirs) {
            require((nodeDir / "node.conf").exists()) { "Missing node.conf in node directory ${nodeDir.fileName}" }
        }
        return nodeDirs
    }

    private fun distributeNodeInfos(nodeDirs: List<Path>, nodeInfoFiles: List<Path>) {
        for (nodeDir in nodeDirs) {
            val additionalNodeInfosDir = (nodeDir / NODE_INFO_DIRECTORY).createDirectories()
            for (nodeInfoFile in nodeInfoFiles) {
                nodeInfoFile.copyToDirectory(additionalNodeInfosDir, REPLACE_EXISTING)
            }
        }
    }

    private fun checkForDuplicateLegalNames(nodeConfigs: Collection<Config>) {
        val duplicateLegalNames = nodeConfigs
                .groupBy { it.getString("myLegalName") }
                .mapNotNull { if (it.value.size > 1) it.key else null }
        check(duplicateLegalNames.isEmpty()) {
            "Nodes must have unique legal names. The following are used more than once: $duplicateLegalNames"
        }
    }

    private fun gatherNotaryInfos(nodeInfoFiles: List<Path>, configs: Map<Path, Config>): List<NotaryInfo> {
        return nodeInfoFiles.mapNotNull { nodeInfoFile ->
            // The config contains the notary type
            val nodeConfig = configs[nodeInfoFile.parent]!!
            if (nodeConfig.hasPath("notary")) {
                val validating = nodeConfig.getBooleanCaseInsensitive("notary.validating")
                // And the node-info file contains the notary's identity
                val nodeInfo = nodeInfoFile.readObject<SignedNodeInfo>().verified()
                NotaryInfo(nodeInfo.notaryIdentity(), validating)
            } else {
                null
            }
        }.distinct() // We need distinct as nodes part of a distributed notary share the same notary identity
    }

    private fun loadNetworkParameters(nodeDirs: List<Path>): NetworkParameters? {
        val netParamsFilesGrouped = nodeDirs.mapNotNull {
            val netParamsFile = it / NETWORK_PARAMS_FILE_NAME
            if (netParamsFile.exists()) netParamsFile else null
        }.groupBy { SerializedBytes<SignedNetworkParameters>(it.readAll()) }

        when (netParamsFilesGrouped.size) {
            0 -> return null
            1 -> return netParamsFilesGrouped.keys.first().deserialize().verifiedNetworkParametersCert(DEV_ROOT_CA.certificate)
        }

        val msg = StringBuilder("Differing sets of network parameters were found. Make sure all the nodes have the same " +
                "network parameters by copying the correct $NETWORK_PARAMS_FILE_NAME file across.\n\n")

        netParamsFilesGrouped.forEach { bytes, netParamsFiles ->
            netParamsFiles.map { it.parent.fileName }.joinTo(msg, ", ")
            msg.append(":\n")
            val netParamsString = try {
                bytes.deserialize().verifiedNetworkParametersCert(DEV_ROOT_CA.certificate).toString()
            } catch (e: Exception) {
                "Invalid network parameters file: $e"
            }
            msg.append(netParamsString)
            msg.append("\n\n")
        }

        throw IllegalStateException(msg.toString())
    }

    private fun defaultNetworkParametersWith(notaryInfos: List<NotaryInfo>,
                                             whitelist: Map<String, List<AttachmentId>>): NetworkParameters {
        return NetworkParameters(
                minimumPlatformVersion = PLATFORM_VERSION,
                notaries = notaryInfos,
                modifiedTime = Instant.now(),
                maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE,
                maxTransactionSize = DEFAULT_MAX_TRANSACTION_SIZE,
                whitelistedContractImplementations = whitelist,
                packageOwnership = emptyMap(),
                epoch = 1,
                eventHorizon = 30.days
        )
    }

    private fun installNetworkParameters(
            notaryInfos: List<NotaryInfo>,
            whitelist: Map<String, List<AttachmentId>>,
            existingNetParams: NetworkParameters?,
            nodeDirs: List<Path>,
            networkParametersOverrides: NetworkParametersOverrides
    ): NetworkParameters {
        val netParams = if (existingNetParams != null) {
            val newNetParams = existingNetParams
                    .copy(notaries = notaryInfos, whitelistedContractImplementations = whitelist)
                    .overrideWith(networkParametersOverrides)
            if (newNetParams != existingNetParams) {
                newNetParams.copy(
                        modifiedTime = Instant.now(),
                        epoch = existingNetParams.epoch + 1
                )
            } else {
                existingNetParams
            }
        } else {
            defaultNetworkParametersWith(notaryInfos, whitelist).overrideWith(networkParametersOverrides)
        }
        val copier = NetworkParametersCopier(netParams, overwriteFile = true)
        nodeDirs.forEach(copier::install)
        return netParams
    }

    private fun NodeInfo.notaryIdentity(): Party {
        return when (legalIdentities.size) {
            // Single node notaries have just one identity like all other nodes. This identity is the notary identity
            1 -> legalIdentities[0]
            // Nodes which are part of a distributed notary have a second identity which is the composite identity of the
            // cluster and is shared by all the other members. This is the notary identity.
            2 -> legalIdentities[1]
            else -> throw IllegalArgumentException("Not sure how to get the notary identity in this scenario: $this")
        }
    }

    // We need to to set serialization env, because generation of parameters is run from Cordform.
    private fun initialiseSerialization() {
        _contextSerializationEnv.set(SerializationEnvironment.with(
                SerializationFactoryImpl().apply {
                    registerScheme(AMQPParametersSerializationScheme())
                },
                AMQP_P2P_CONTEXT)
        )
    }

    private class AMQPParametersSerializationScheme : AbstractAMQPSerializationScheme(emptyList()) {
        override fun rpcClientSerializerFactory(context: SerializationContext) = throw UnsupportedOperationException()
        override fun rpcServerSerializerFactory(context: SerializationContext) = throw UnsupportedOperationException()

        override fun canDeserializeVersion(magic: CordaSerializationMagic, target: SerializationContext.UseCase): Boolean {
            return magic == amqpMagic && target == SerializationContext.UseCase.P2P
        }
    }

    private fun isSigned(file: Path): Boolean = file.read {
        JarInputStream(it).use {
            JarSignatureCollector.collectSigningParties(it).isNotEmpty()
        }
    }
}

fun NetworkParameters.overrideWith(override: NetworkParametersOverrides): NetworkParameters {
    return this.copy(
            minimumPlatformVersion = override.minimumPlatformVersion ?: this.minimumPlatformVersion,
            maxMessageSize = override.maxMessageSize ?: this.maxMessageSize,
            maxTransactionSize = override.maxTransactionSize ?: this.maxTransactionSize,
            eventHorizon = override.eventHorizon ?: this.eventHorizon,
            packageOwnership = override.packageOwnership?.map { it.javaPackageName to it.publicKey }?.toMap()
                    ?: this.packageOwnership
    )
}

data class PackageOwner(val javaPackageName: String, val publicKey: PublicKey)

data class NetworkParametersOverrides(
        val minimumPlatformVersion: Int? = null,
        val maxMessageSize: Int? = null,
        val maxTransactionSize: Int? = null,
        val packageOwnership: List<PackageOwner>? = null,
        val eventHorizon: Duration? = null
)

interface NetworkBootstrapperWithOverridableParameters {
    fun bootstrap(directory: Path, copyCordapps: CopyCordapps, networkParameterOverrides: NetworkParametersOverrides = NetworkParametersOverrides())
}

enum class CopyCordapps {
    FirstRunOnly {
        override fun copyTo(cordappJars: List<Path>, nodeDirs: List<Path>, networkAlreadyExists: Boolean, fromCordform: Boolean) {
            if (networkAlreadyExists) {
                if (!fromCordform) {
                    println("Not copying CorDapp JARs as --copy-cordapps is set to FirstRunOnly, and it looks like this network has already been bootstrapped.")
                }
                return
            }
            cordappJars.copy(nodeDirs)
        }
    },

    Yes {
        override fun copyTo(cordappJars: List<Path>, nodeDirs: List<Path>, networkAlreadyExists: Boolean, fromCordform: Boolean) = cordappJars.copy(nodeDirs)
    },

    No {
        override fun copyTo(cordappJars: List<Path>, nodeDirs: List<Path>, networkAlreadyExists: Boolean, fromCordform: Boolean) {
            if (!fromCordform) {
                println("Not copying CorDapp JARs as --copy-cordapps is set to No.")
            }
        }
    };

    protected abstract fun copyTo(cordappJars: List<Path>, nodeDirs: List<Path>, networkAlreadyExists: Boolean, fromCordform: Boolean)

    protected fun List<Path>.copy(nodeDirs: List<Path>) {
        if (this.isNotEmpty()) {
            println("Copying CorDapp JARs into node directories")
            for (nodeDir in nodeDirs) {
                val cordappsDir = (nodeDir / "cordapps").createDirectories()
                this.forEach {
                    try {
                        it.copyToDirectory(cordappsDir)
                    } catch (e: FileAlreadyExistsException) {
                        println("WARNING: ${it.fileName} already exists in $cordappsDir, ignoring and leaving existing CorDapp untouched")
                    }
                }
            }
        }
    }

    fun copy(cordappJars: List<Path>, nodeDirs: List<Path>, networkAlreadyExists: Boolean, fromCordform: Boolean) {
        if (!fromCordform) {
            println("Found the following CorDapps: ${cordappJars.map(Path::getFileName)}")
        }
        this.copyTo(cordappJars, nodeDirs, networkAlreadyExists, fromCordform)
    }
}