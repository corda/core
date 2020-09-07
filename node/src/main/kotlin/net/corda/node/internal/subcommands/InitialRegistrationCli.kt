package net.corda.node.internal.subcommands

import net.corda.cliutils.CliWrapperBase
import net.corda.cliutils.ExitCodes
import net.corda.core.internal.createFile
import net.corda.core.internal.div
import net.corda.core.internal.exists
import net.corda.node.InitialRegistrationCmdLineOptions
import net.corda.node.NodeRegistrationOption
import net.corda.node.internal.*
import net.corda.node.internal.NodeStartupLogging.Companion.logger
import net.corda.node.services.config.NodeConfiguration
import net.corda.node.utilities.registration.HTTPNetworkRegistrationService
import net.corda.node.utilities.registration.NodeRegistrationConfiguration
import net.corda.node.utilities.registration.NodeRegistrationHelper
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.util.function.Consumer

class InitialRegistrationCli(val startup: NodeStartup): CliWrapperBase("initial-registration", "Start initial node registration with Corda network to obtain certificate from the permissioning server.") {
    @Option(names = ["-t", "--network-root-truststore"], description = ["Network root trust store obtained from network operator."])
    var networkRootTrustStorePathParameter: Path? = null

    @Option(names = ["-p", "--network-root-truststore-password"], description = ["Network root trust store password obtained from network operator."], required = true)
    var networkRootTrustStorePassword: String = ""

    @Option(names = ["-s", "--skip-schema-creation"], description = ["Prevent database migration scripts to run during initial node registration "], required = false)
    var skipSchemaCreation: Boolean = false

    override fun runProgram() : Int {
        val networkRootTrustStorePath: Path = networkRootTrustStorePathParameter ?: cmdLineOptions.baseDirectory / "certificates" / "network-root-truststore.jks"

        //EG-197 luca.debiasi
        // Execute the code described in InitialRegistration class that is modified to do not require any action in the DB,
        // The normal process to start the node picks up any change in certificates and network parameters to cache them in the DB.
        // Consider the InitialRegistration process create a Node instance, it must be disposed properly before a new Node is created
        // during normal startup.
        var exitCode = startup.initialiseAndRun(
                cmdLineOptions,
                InitialRegistration(
                        cmdLineOptions.baseDirectory,
                        networkRootTrustStorePath,
                        networkRootTrustStorePassword,
                        skipSchemaCreation,
                        startup))
        if (exitCode == ExitCodes.SUCCESS) {
            // Continue to start the Node.
            // Remove the process-id marker file to allow startup to bootstrap the Node as no other Node is run before.
            // Before this point any eventual Node should perform node.stop().
            val pidPath = (cmdLineOptions.baseDirectory / "process-id")
            Files.deleteIfExists(pidPath)
            // Bootstrap the node 'normally', initial registration is done above.
            exitCode = startup.initialiseAndRun(cmdLineOptions, object : RunAfterNodeInitialisation {
                val startupTime = System.currentTimeMillis()
                override fun run(node: Node) = startup.startNode(node, startupTime)
            }, requireCertificates = true)
        }
        // Return the exit code of initial registration, if something bad occurred, or of node running after initial registration.
        return exitCode
        //~ EG-197 luca.debiasi
    }

    override fun initLogging(): Boolean = this.initLogging(cmdLineOptions.baseDirectory)

    @Mixin
    val cmdLineOptions = InitialRegistrationCmdLineOptions()
}

class InitialRegistration(val baseDirectory: Path, private val networkRootTrustStorePath: Path, networkRootTrustStorePassword: String,
                          private val skipSchemaMigration: Boolean, private val startup: NodeStartup) : RunAfterNodeInitialisation, NodeStartupLogging {
    companion object {
        private const val INITIAL_REGISTRATION_MARKER = ".initialregistration"

        fun checkRegistrationMode(baseDirectory: Path): Boolean {
            // If the node was started with `--initial-registration`, create marker file.
            // We do this here to ensure the marker is created even if parsing the args with NodeArgsParser fails.
            val marker = baseDirectory / INITIAL_REGISTRATION_MARKER
            if (!marker.exists()) {
                return false
            }
            try {
                marker.createFile()
            } catch (e: Exception) {
                logger.warn("Could not create marker file for `initial-registration`.", e)
            }
            return true
        }
    }

    private val nodeRegistration = NodeRegistrationOption(networkRootTrustStorePath, networkRootTrustStorePassword)

    private fun registerWithNetwork(conf: NodeConfiguration) {
        val versionInfo = startup.getVersionInfo()

        println("\n" +
                "*******************************************************************\n" +
                "*                                                                 *\n" +
                "*      Registering as a new participant with a Corda network      *\n" +
                "*                                                                 *\n" +
                "*******************************************************************\n")

        NodeRegistrationHelper(NodeRegistrationConfiguration(conf),
                HTTPNetworkRegistrationService(
                        requireNotNull(conf.networkServices),
                        versionInfo),
                nodeRegistration).generateKeysAndRegister()

        // Minimal changes to make registration tool create node identity.
        // TODO: Move node identity generation logic from node to registration helper.

        // EG-197 luca.debiasi
        // The following statement...
        // val node = startup.createNode(conf, versionInfo)
        // if(!skipSchemaMigration) {
        //    node.runDatabaseMigrationScripts(updateCoreSchemas = true, updateAppSchemas = true, updateAppSchemasWithCheckpoints = false)
        //}
        //node.generateAndSaveNodeInfo()
        // ...are removed because replicated during normal node starts.
        println("Successfully registered Corda node with compatibility zone, node identity keys and certificates are stored in '${conf.certificatesDirectory}', it is advised to backup the private keys and certificates.")
        // The following statement should be removed.
        //println("Corda node will now terminate.")
        //~ EG-197 luca.debiasi
    }

    private fun initialRegistration(config: NodeConfiguration) {
        // Null checks for [compatibilityZoneURL], [rootTruststorePath] and
        // [rootTruststorePassword] have been done in [CmdLineOptions.loadConfig]
        attempt { registerWithNetwork(config) }.doOnFailure(Consumer(this::handleRegistrationError)).getOrThrow()
        
        // At this point the node registration was successful. We can delete the marker file.
        deleteNodeRegistrationMarker(baseDirectory)
    }

    private fun deleteNodeRegistrationMarker(baseDir: Path) {
        try {
            val marker = File((baseDir / INITIAL_REGISTRATION_MARKER).toUri())
            if (marker.exists()) {
                marker.delete()
            }
        } catch (e: Exception) {
            e.logAsUnexpected( "Could not delete the marker file that was created for `initial-registration`.", print = logger::warn)
        }
    }

    override fun run(node: Node) {
        require(networkRootTrustStorePath.exists()) { "Network root trust store path: '$networkRootTrustStorePath' doesn't exist" }
        if (checkRegistrationMode(baseDirectory)) {
            println("Node was started before with `--initial-registration`, but the registration was not completed.\nResuming registration.")
        }
        initialRegistration(node.configuration)
    }
}
