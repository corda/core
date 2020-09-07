package net.corda.node.internal

import io.netty.channel.unix.Errors
import net.corda.cliutils.CliWrapperBase
import net.corda.cliutils.CordaCliWrapper
import net.corda.cliutils.ExitCodes
import net.corda.cliutils.printError
import net.corda.common.logging.CordaVersion
import net.corda.common.logging.errorReporting.CordaErrorContextProvider
import net.corda.common.logging.errorReporting.DoubleInitializationException
import net.corda.common.logging.errorReporting.ErrorCode
import net.corda.core.contracts.HashAttachmentConstraint
import net.corda.core.crypto.Crypto
import net.corda.core.internal.*
import net.corda.core.internal.concurrent.thenMatch
import net.corda.core.internal.cordapp.CordappImpl
import net.corda.core.internal.errors.AddressBindingException
import net.corda.core.utilities.Try
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.loggerFor
import net.corda.node.*
import net.corda.common.logging.errorReporting.ErrorReporting
import net.corda.common.logging.errorReporting.report
import net.corda.core.serialization.internal.nodeSerializationEnv
import net.corda.node.internal.Node.Companion.isInvalidJavaVersion
import net.corda.node.internal.cordapp.MultipleCordappsForFlowException
import net.corda.node.internal.subcommands.*
import net.corda.node.internal.subcommands.ValidateConfigurationCli.Companion.logConfigurationErrors
import net.corda.node.internal.subcommands.ValidateConfigurationCli.Companion.logRawConfig
import net.corda.node.services.config.NodeConfiguration
import net.corda.node.services.config.shouldStartLocalShell
import net.corda.node.services.config.shouldStartSSHDaemon
import net.corda.node.utilities.registration.NodeRegistrationException
import net.corda.nodeapi.internal.JVMAgentUtilities
import net.corda.nodeapi.internal.addShutdownHook
import net.corda.nodeapi.internal.persistence.CouldNotCreateDataSourceException
import net.corda.nodeapi.internal.persistence.DatabaseIncompatibleException
import net.corda.tools.shell.InteractiveShell
import org.fusesource.jansi.Ansi
import org.slf4j.bridge.SLF4JBridgeHandler
import picocli.CommandLine.Mixin
import java.io.IOException
import java.io.RandomAccessFile
import java.lang.NullPointerException
import java.lang.management.ManagementFactory
import java.net.InetAddress
import java.nio.channels.UnresolvedAddressException
import java.nio.file.Path
import java.time.DayOfWeek
import java.time.ZonedDateTime
import java.util.function.Consumer

/** An interface that can be implemented to tell the node what to do once it's intitiated. */
interface RunAfterNodeInitialisation {
    fun run(node: Node)
}

/** Base class for subcommands to derive from that initialises the logs and provides standard options. */
abstract class NodeCliCommand(alias: String, description: String, val startup: NodeStartup) : CliWrapperBase(alias, description), NodeStartupLogging {
    companion object {
        const val LOGS_DIRECTORY_NAME = "logs"
    }

    override fun initLogging(): Boolean = this.initLogging(cmdLineOptions.baseDirectory)

    @Mixin
    val cmdLineOptions = SharedNodeCmdLineOptions()
}

/** Main corda entry point. */
open class NodeStartupCli : CordaCliWrapper("corda", "Runs a Corda Node") {
    open val startup = NodeStartup()

    @Mixin
    val cmdLineOptions = NodeCmdLineOptions()

    private val networkCacheCli by lazy { ClearNetworkCacheCli(startup) }
    private val justGenerateNodeInfoCli by lazy { GenerateNodeInfoCli(startup) }
    private val justGenerateRpcSslCertsCli by lazy { GenerateRpcSslCertsCli(startup) }
    private val initialRegistrationCli by lazy { InitialRegistrationCli(startup) }
    private val validateConfigurationCli by lazy { ValidateConfigurationCli() }
    private val runMigrationScriptsCli by lazy { RunMigrationScriptsCli(startup) }
    private val synchroniseAppSchemasCli by lazy { SynchroniseSchemasCli(startup) }

    override fun initLogging(): Boolean = this.initLogging(cmdLineOptions.baseDirectory)

    override fun additionalSubCommands() = setOf(networkCacheCli,
            justGenerateNodeInfoCli,
            justGenerateRpcSslCertsCli,
            initialRegistrationCli,
            validateConfigurationCli,
            runMigrationScriptsCli,
            synchroniseAppSchemasCli)

    override fun call(): Int {
        if (!validateBaseDirectory()) {
            return ExitCodes.FAILURE
        }
        return super.call()
    }

    private fun validateBaseDirectory(): Boolean {
        //Ensure that the base directory actually exists before initialising and the rest of the node.
        val baseDirectory = cmdLineOptions.baseDirectory

        if (!baseDirectory.exists()) {
            printError("Base directory $baseDirectory does not exist. Node will now shutdown")
            return false
        }
        return true
    }

    override fun runProgram(): Int {
        return when {
            InitialRegistration.checkRegistrationMode(cmdLineOptions.baseDirectory) -> {
                println("Node was started before in `initial-registration` mode, but the registration was not completed.\nResuming registration.")
                initialRegistrationCli.cmdLineOptions.copyFrom(cmdLineOptions)
                initialRegistrationCli.runProgram()
            }
            //deal with legacy flags and redirect to subcommands
            cmdLineOptions.isRegistration -> {
                Node.printWarning("The --initial-registration flag has been deprecated and will be removed in a future version. Use the initial-registration command instead.")
                requireNotNull(cmdLineOptions.networkRootTrustStorePassword) { "Network root trust store password must be provided in registration mode using --network-root-truststore-password." }
                initialRegistrationCli.networkRootTrustStorePassword = cmdLineOptions.networkRootTrustStorePassword!!
                initialRegistrationCli.networkRootTrustStorePathParameter = cmdLineOptions.networkRootTrustStorePathParameter
                initialRegistrationCli.skipSchemaCreation = cmdLineOptions.skipSchemaCreation
                initialRegistrationCli.cmdLineOptions.copyFrom(cmdLineOptions)
                initialRegistrationCli.runProgram()
            }
            cmdLineOptions.clearNetworkMapCache -> {
                Node.printWarning("The --clear-network-map-cache flag has been deprecated and will be removed in a future version. Use the clear-network-cache command instead.")
                networkCacheCli.cmdLineOptions.copyFrom(cmdLineOptions)
                networkCacheCli.runProgram()
            }
            cmdLineOptions.justGenerateNodeInfo -> {
                Node.printWarning("The --just-generate-node-info flag has been deprecated and will be removed in a future version. Use the generate-node-info command instead.")
                justGenerateNodeInfoCli.cmdLineOptions.copyFrom(cmdLineOptions)
                justGenerateNodeInfoCli.runProgram()
            }
            cmdLineOptions.justGenerateRpcSslCerts -> {
                Node.printWarning("The --just-generate-rpc-ssl-settings flag has been deprecated and will be removed in a future version. Use the generate-rpc-ssl-settings command instead.")
                justGenerateRpcSslCertsCli.cmdLineOptions.copyFrom(cmdLineOptions)
                justGenerateRpcSslCertsCli.runProgram()
            }
            else -> startup.initialiseAndRun(cmdLineOptions, object : RunAfterNodeInitialisation {
                val startupTime = System.currentTimeMillis()
                override fun run(node: Node) = startup.startNode(node, startupTime)
            }, requireCertificates = true)
        }
    }
}

/** This class provides a common set of functionality for starting a Node from command line arguments. */
open class NodeStartup : NodeStartupLogging {
    companion object {
        private val logger by lazy { loggerFor<Node>() } // I guess this is lazy to allow for logging init, but why Node?
        const val LOGS_DIRECTORY_NAME = "logs"
        const val LOGS_CAN_BE_FOUND_IN_STRING = "Logs can be found in"
        const val ERROR_CODE_RESOURCE_LOCATION = "error-codes"
    }

    lateinit var cmdLineOptions: SharedNodeCmdLineOptions

    fun initialiseAndRun(cmdLineOptions: SharedNodeCmdLineOptions, afterNodeInitialisation: RunAfterNodeInitialisation, requireCertificates: Boolean = false): Int {
        this.cmdLineOptions = cmdLineOptions

        // Step 1. Check for supported Java version.
        if (isInvalidJavaVersion()) return ExitCodes.FAILURE

        // Step 2. We do the single node check before we initialise logging so that in case of a double-node start it
        // doesn't mess with the running node's logs.
        if (!isNodeRunningAt(cmdLineOptions.baseDirectory)) return ExitCodes.FAILURE

        // Step 3. Register all cryptography [Provider]s.
        // Required to install our [SecureRandom] before e.g., UUID asks for one.
        // This needs to go after initLogging(netty clashes with our logging)
        Crypto.registerProviders()

        // Step 4. Print banner and basic node info.
        val versionInfo = getVersionInfo()
        drawBanner(versionInfo)
        Node.printBasicNodeInfo(LOGS_CAN_BE_FOUND_IN_STRING, System.getProperty("log-path"))

        // Step 5. Load and validate node configuration.
        val rawConfig = cmdLineOptions.rawConfiguration().doOnErrors(cmdLineOptions::logRawConfigurationErrors).optional
                ?: return ExitCodes.FAILURE
        val configuration = cmdLineOptions.parseConfiguration(rawConfig).doIfValid { logRawConfig(rawConfig) }
                .doOnErrors(::logConfigurationErrors).optional
                ?: return ExitCodes.FAILURE
        // EG-197 luca.debiasi
        // This method should be run once to create the node and start it.
        // However, initial registration subcommand calls this method twice:
        // the simplest solution is to wrap and filter DoubleInitializationException.
        try {
            ErrorReporting()
                    .usingResourcesAt(ERROR_CODE_RESOURCE_LOCATION)
                    .withContextProvider(CordaErrorContextProvider())
                    .initialiseReporting()
        } catch (e: DoubleInitializationException) {
            logger.warn("{$e.message} filtered: the node starts...")
        }
        //~ EG-197 luca.debiasi

        // Step 6. Check if we can access the certificates directory
        if (requireCertificates && !canReadCertificatesDirectory(configuration.certificatesDirectory, configuration.devMode)) return ExitCodes.FAILURE

        // Step 7. Configuring special serialisation requirements, i.e., bft-smart relies on Java serialization.
        if (attempt { banJavaSerialisation(configuration) }.doOnFailure(Consumer { error -> error.logAsUnexpected("Exception while configuring serialisation") }) !is Try.Success) return ExitCodes.FAILURE

        // Step 8. Any actions required before starting up the Corda network layer.
        if (attempt { preNetworkRegistration(configuration) }.doOnFailure(Consumer(::handleRegistrationError)) !is Try.Success) return ExitCodes.FAILURE

        // Step 9. Log startup info.
        logStartupInfo(versionInfo, configuration)

        // Step 10. Start node: create the node, check for other command-line options, add extra logging etc.
        if (attempt {
                    // EG-197 luca.debiasi
                    val node = createNode(configuration, versionInfo)
                    afterNodeInitialisation.run(node)
                    node.stop()
                    //~ EG-197 luca.debiasi
                }.doOnFailure(Consumer(::handleStartError)) !is Try.Success) return ExitCodes.FAILURE

        return ExitCodes.SUCCESS
    }

    protected open fun preNetworkRegistration(conf: NodeConfiguration) = Unit

    open fun createNode(conf: NodeConfiguration, versionInfo: VersionInfo): Node = Node(conf, versionInfo, allowHibernateToManageAppSchema = cmdLineOptions.allowHibernateToManageAppSchema)

    fun startNode(node: Node, startTime: Long) {
        if (node.configuration.devMode) {
            Emoji.renderIfSupported {
                Node.printWarning("This node is running in development mode! ${Emoji.developer} This is not safe for production deployment.")
            }
        } else {
            logger.info("The Corda node is running in production mode. If this is a developer environment you can set 'devMode=true' in the node.conf file.")
        }

        if (HashAttachmentConstraint.disableHashConstraints) {
            Node.printWarning("Hash constraints checking has been disabled by the node operator.")
        }

        val nodeInfo = node.start()
        val loadedCodapps = node.services.cordappProvider.cordapps.filter { it.isLoaded }
        logLoadedCorDapps(loadedCodapps)

        node.nodeReadyFuture.thenMatch({
            // Elapsed time in seconds. We used 10 / 100.0 and not directly / 1000.0 to only keep two decimal digits.
            val elapsed = (System.currentTimeMillis() - startTime) / 10 / 100.0
            val name = nodeInfo.legalIdentitiesAndCerts.first().name.organisation
            Node.printBasicNodeInfo("Node for \"$name\" started up and registered in $elapsed sec")

            // EG-197 luca.debiasi
            // This change assure the node always try to reach startupComplete state
            // and print basic node info.
            // Then, if has a console attached, start the CRaSH shell.
            // The reach of startupComplete completation is important to allow the properly stop the node later.
            node.startupComplete.then {
                Node.printBasicNodeInfo("Node for \"$name\" started up and registered in $elapsed sec")
                // Don't start the shell if there's no console attached.
                if (node.configuration.shouldStartLocalShell()) {
                    try {
                        InteractiveShell.runLocalShell(node::stop)
                    } catch (e: Exception) {
                        logger.error("Shell failed to start", e)
                    }
                }
            }
            //~ EG-197 luca.debiasi

            if (node.configuration.shouldStartSSHDaemon()) {
                Node.printBasicNodeInfo("SSH server listening on port", node.configuration.sshd!!.port.toString())
            }
        },
                { th ->
                    logger.error("Unexpected exception during registration", th)
                })
        node.run()
    }

    protected open fun logStartupInfo(versionInfo: VersionInfo, conf: NodeConfiguration) {
        logger.info("Vendor: ${versionInfo.vendor}")
        logger.info("Release: ${versionInfo.releaseVersion}")
        logger.info("Platform Version: ${versionInfo.platformVersion}")
        logger.info("Revision: ${versionInfo.revision}")
        val info = ManagementFactory.getRuntimeMXBean()
        logger.info("PID: ${info.name.split("@").firstOrNull()}")  // TODO Java 9 has better support for this
        logger.info("Main class: ${NodeConfiguration::class.java.location.toURI().path}")
        logger.info("CommandLine Args: ${info.inputArguments.joinToString(" ")}")
        // JDK 11 (bootclasspath no longer supported from JDK 9)
        if (info.isBootClassPathSupported) logger.info("bootclasspath: ${info.bootClassPath}")
        logger.info("classpath: ${info.classPath}")
        logger.info("VM ${info.vmName} ${info.vmVendor} ${info.vmVersion}")
        logger.info("Machine: ${lookupMachineNameAndMaybeWarn()}")
        logger.info("Working Directory: ${cmdLineOptions.baseDirectory}")
        JVMAgentUtilities.parseDebugPort(info.inputArguments)?.let {
            logger.info("Debug port: $it")
        }

        var nodeStartedMessage = "Starting as node on ${conf.p2pAddress}"
        if (conf.extraNetworkMapKeys.isNotEmpty()) {
            nodeStartedMessage = "$nodeStartedMessage with additional Network Map keys ${conf.extraNetworkMapKeys.joinToString(prefix = "[", postfix = "]", separator = ", ")}"
        }
        logger.info(nodeStartedMessage)
    }

    protected open fun banJavaSerialisation(conf: NodeConfiguration) {
        // Note that in dev mode this filter can be overridden by a notary service implementation.
        SerialFilter.install(::defaultSerialFilter)
    }

    open fun getVersionInfo(): VersionInfo {
        return VersionInfo(
                PLATFORM_VERSION,
                CordaVersion.releaseVersion,
                CordaVersion.revision,
                CordaVersion.vendor
        )
    }

    protected open fun logLoadedCorDapps(corDapps: List<CordappImpl>) {
        Node.printBasicNodeInfo("Loaded ${corDapps.size} CorDapp(s)", corDapps.map { it.info }.joinToString(", "))
        corDapps.filter { it.info.hasUnknownFields() }.forEach {
            logger.warn("${it.info} will be unable to run on Corda in the future due to missing entries in JAR's manifest file.")
        }
    }

    fun isNodeRunningAt(baseDirectory: Path): Boolean {
        // Write out our process ID (which may or may not resemble a UNIX process id - to us it's just a string) to a
        // file that we'll do our best to delete on exit. But if we don't, it'll be overwritten next time. If it already
        // exists, we try to take the file lock first before replacing it and if that fails it means we're being started
        // twice with the same directory: that's a user error and we should bail out.
        val pidFile = (baseDirectory / "process-id").toFile()
        try {
            pidFile.createNewFile()
            val pidFileRw = RandomAccessFile(pidFile, "rw")
            val pidFileLock = pidFileRw.channel.tryLock()

            if (pidFileLock == null) {
                println("It appears there is already a node running with the specified data directory $baseDirectory")
                println("Shut that other node down and try again. It may have process ID ${pidFile.readText()}")
                return false
            }
            pidFile.deleteOnExit()
            // Avoid the lock being garbage collected. We don't really need to release it as the OS will do so for us
            // when our process shuts down, but we try in stop() anyway just to be nice.
            addShutdownHook {
                pidFileLock.release()
            }
            val ourProcessID: String = ManagementFactory.getRuntimeMXBean().name.split("@")[0]
            pidFileRw.setLength(0)
            pidFileRw.write(ourProcessID.toByteArray())
        } catch (ex: IOException) {
            val appUser = System.getProperty("user.name")
            println("Application user '$appUser' does not have necessary permissions for Node base directory '$baseDirectory'.")
            println("Corda Node process in now exiting. Please check directory permissions and try starting the Node again.")
            return false
        }
        return true
    }

    private fun canReadCertificatesDirectory(certDirectory: Path, devMode: Boolean): Boolean {
        //Test for access to the certificates path and shutdown if we are unable to reach it.
        //We don't do this if devMode==true because the certificates would be created anyway
        if (devMode) return true

        if (!certDirectory.isDirectory()) {
            logger.error("Unable to access certificates directory ${certDirectory}. This could be because the node has not been registered with the Identity Operator. Node will now shutdown")
            return false
        }
        return true
    }

    private fun lookupMachineNameAndMaybeWarn(): String {
        val start = System.currentTimeMillis()
        val hostName: String = InetAddress.getLocalHost().hostName
        val elapsed = System.currentTimeMillis() - start
        if (elapsed > 1000 && hostName.endsWith(".local")) {
            // User is probably on macOS and experiencing this problem: http://stackoverflow.com/questions/10064581/how-can-i-eliminate-slow-resolving-loading-of-localhost-virtualhost-a-2-3-secon
            //
            // Also see https://bugs.openjdk.java.net/browse/JDK-8143378
            val messages = listOf(
                    "Your computer took over a second to resolve localhost due an incorrect configuration. Corda will work but start very slowly until this is fixed."
            )
            logger.warn(messages.joinToString(""))
            Emoji.renderIfSupported {
                print(Ansi.ansi().fgBrightRed())
                messages.forEach {
                    println("${Emoji.sleepingFace}$it")
                }
                print(Ansi.ansi().reset())
            }
        }
        return hostName
    }

    open fun drawBanner(versionInfo: VersionInfo) {
        Emoji.renderIfSupported {
            val messages = arrayListOf(
                    "The only distributed ledger that pays\nhomage to Pac Man in its logo.",
                    "You know, I was a banker\nonce ... but I lost interest. ${Emoji.bagOfCash}",
                    "It's not who you know, it's who you know\nknows what you know you know.",
                    "It runs on the JVM because QuickBasic\nis apparently not 'professional' enough.",
                    "\"It's OK computer, I go to sleep after\ntwenty minutes of inactivity too!\"",
                    "It's kind of like a block chain but\ncords sounded healthier than chains.",
                    "Computer science and finance together.\nYou should see our crazy Christmas parties!",
                    "I met my bank manager yesterday and asked\nto check my balance ... he pushed me over!",
                    "A banker left to their own devices may find\nthemselves .... a-loan! <applause>",
                    "Whenever I go near my bank\nI get withdrawal symptoms ${Emoji.coolGuy}",
                    "There was an earthquake in California,\na local bank went into de-fault.",
                    "I asked for insurance if the nearby\nvolcano erupted. They said I'd be covered.",
                    "I had an account with a bank in the\nNorth Pole, but they froze all my assets ${Emoji.santaClaus}",
                    "Check your contracts carefully. The fine print\nis usually a clause for suspicion ${Emoji.santaClaus}",
                    "Some bankers are generous ...\nto a vault! ${Emoji.bagOfCash} ${Emoji.coolGuy}",
                    "What you can buy for a dollar these\ndays is absolute non-cents! ${Emoji.bagOfCash}",
                    "Old bankers never die, they\njust... pass the buck",
                    "I won $3M on the lottery so I donated a quarter\nof it to charity. Now I have $2,999,999.75.",
                    "There are two rules for financial success:\n1) Don't tell everything you know.",
                    "Top tip: never say \"oops\", instead\nalways say \"Ah, Interesting!\"",
                    "Computers are useless. They can only\ngive you answers.  -- Picasso",
                    "Regular naps prevent old age, especially\nif you take them whilst driving.",
                    "Always borrow money from a pessimist.\nHe won't expect it back.",
                    "War does not determine who is right.\nIt determines who is left.",
                    "A bus stops at a bus station. A train stops at a\ntrain station. What happens at a workstation?",
                    "I got a universal remote control yesterday.\nI thought, this changes everything.",
                    "Did you ever walk into an office and\nthink, whiteboards are remarkable!",
                    "The good thing about lending out your time machine\nis that you basically get it back immediately.",
                    "I used to work in a shoe recycling\nshop. It was sole destroying.",
                    "What did the fish say\nwhen he hit a wall? Dam.",
                    "You should really try a seafood diet.\nIt's easy: you see food and eat it.",
                    "I recently sold my vacuum cleaner,\nall it was doing was gathering dust.",
                    "My professor accused me of plagiarism.\nHis words, not mine!",
                    "Change is inevitable, except\nfrom a vending machine.",
                    "If at first you don't succeed, destroy\nall the evidence that you tried.",
                    "If at first you don't succeed, \nthen we have something in common!",
                    "Moses had the first tablet that\ncould connect to the cloud.",
                    "How did my parents fight boredom before the internet?\nI asked my 17 siblings and they didn't know either.",
                    "Cats spend two thirds of their lives sleeping\nand the other third making viral videos.",
                    "The problem with troubleshooting\nis that trouble shoots back.",
                    "I named my dog 'Six Miles' so I can tell\npeople I walk Six Miles every day.",
                    "People used to laugh at me when I said I wanted\nto be a comedian. Well they're not laughing now!",
                    "My wife just found out I replaced our bed\nwith a trampoline; she hit the roof.",
                    "My boss asked me who is the stupid one, me or him?\nI said everyone knows he doesn't hire stupid people.",
                    "Don't trust atoms.\nThey make up everything.",
                    "Keep the dream alive:\nhit the snooze button.",
                    "Rest in peace, boiled water.\nYou will be mist.",
                    "When I discovered my toaster wasn't\nwaterproof, I was shocked.",
                    "Where do cryptographers go for\nentertainment? The security theatre.",
                    "How did the Java programmer get rich?\nThey inherited a factory.",
                    "Why did the developer quit his job?\nHe didn't get ar-rays.",
                    "Quantum computer jokes are both\n funny and not funny at the same time."
            )

            if (Emoji.hasEmojiTerminal)
                messages += "Kind of like a regular database but\nwith emojis, colours and ascii art. ${Emoji.coolGuy}"


            if (ZonedDateTime.now().dayOfWeek == DayOfWeek.FRIDAY) {
                // Make it quite likely people see it.
                repeat(20) { messages += "Ah, Friday.\nMy second favourite F-word." }
            }

            val (msg1, msg2) = messages.randomOrNull()!!.split('\n')

            println(Ansi.ansi().newline().fgBrightRed().a(
                    """   ______               __""").newline().a(
                    """  / ____/     _________/ /___ _""").newline().a(
                    """ / /     __  / ___/ __  / __ `/         """).fgBrightBlue().a(msg1).newline().fgBrightRed().a(
                    """/ /___  /_/ / /  / /_/ / /_/ /          """).fgBrightBlue().a(msg2).newline().fgBrightRed().a(
                    """\____/     /_/   \__,_/\__,_/""").reset().newline().newline().fgBrightDefault().bold()
                    .a("--- ${versionInfo.vendor} ${versionInfo.releaseVersion} (${versionInfo.revision.take(7)}) -------------------------------------------------------------")
                    .newline().newline().reset())

        }
    }
}

/** Provide some common logging methods for node startup commands. */
interface NodeStartupLogging {
    companion object {
        val logger by lazy { contextLogger() }
        val startupErrors = setOf(MultipleCordappsForFlowException::class, CheckpointIncompatibleException::class, AddressBindingException::class, NetworkParametersReader::class, DatabaseIncompatibleException::class)

        @Suppress("TooGenericExceptionCaught")
        val PRINT_ERRORS_TO_STD_ERR = try {
            System.getProperty("net.corda.node.printErrorsToStdErr") == "true"
        } catch (e: NullPointerException) {
            false
        } catch (e: IllegalArgumentException) {
            false
        }
    }

    fun <RESULT> attempt(action: () -> RESULT): Try<RESULT> = Try.on(action).throwError()

    fun Throwable.logAsExpected(message: String? = this.message, print: (String?) -> Unit = logger::error) = print(message)

    fun Throwable.logAsUnexpected(
            message: String? = this.message,
            error: Throwable = this,
            print: (String?, Throwable) -> Unit = { s, e ->
                logger.error(s, e)
                if (PRINT_ERRORS_TO_STD_ERR) {
                    System.err.println(s)
                    e.printStackTrace()
                }
            }
    ) {
        print("$message${this.message?.let { ": $it" } ?: ""}", error)
    }

    fun handleRegistrationError(error: Throwable) {
        when (error) {
            is NodeRegistrationException -> error.logAsExpected("Issue with Node registration: ${error.message}")
            else -> error.logAsUnexpected("Exception during node registration")
        }
    }

    fun Throwable.isOpenJdkKnownIssue() = message?.startsWith("Unknown named curve:") == true

    fun Throwable.isExpectedWhenStartingNode() = startupErrors.any { error -> error.isInstance(this) }

    fun handleStartError(error: Throwable) {
        when {
            error is ErrorCode<*> -> logger.report(error)
            error.isExpectedWhenStartingNode() -> error.logAsExpected()
            error is CouldNotCreateDataSourceException -> error.logAsUnexpected()
            error is Errors.NativeIoException && error.message?.contains("Address already in use") == true -> error.logAsExpected("One of the ports required by the Corda node is already in use.")
            error is Errors.NativeIoException && error.message?.contains("Can't assign requested address") == true -> error.logAsExpected("Exception during node startup. Check that addresses in node config resolve correctly.")
            error is UnresolvedAddressException -> error.logAsExpected("Exception during node startup. Check that addresses in node config resolve correctly.")
            error is java.nio.file.AccessDeniedException -> error.logAsExpected("Exception during node startup. Corda started with insufficient privileges to access ${error.file}")
            error is java.nio.file.NoSuchFileException -> error.logAsExpected("Exception during node startup. Corda cannot find file ${error.file}")
            error.isOpenJdkKnownIssue() -> error.logAsExpected("Exception during node startup - ${error.message}. This is a known OpenJDK issue on some Linux distributions, please use OpenJDK from zulu.org or Oracle JDK.")
            else -> error.logAsUnexpected("Exception during node startup")
        }
    }
}

fun CliWrapperBase.initLogging(baseDirectory: Path): Boolean {
    System.setProperty("defaultLogLevel", specifiedLogLevel) // These properties are referenced from the XML config file.
    System.setProperty("log-path", (baseDirectory / NodeCliCommand.LOGS_DIRECTORY_NAME).toString())
    if (verbose) {
        System.setProperty("consoleLoggingEnabled", "true")
        System.setProperty("consoleLogLevel", specifiedLogLevel)
        Node.renderBasicInfoToConsole = false
    }

    //Test for access to the logging path and shutdown if we are unable to reach it.
    val logPath = baseDirectory / NodeCliCommand.LOGS_DIRECTORY_NAME
    try {
        logPath.safeSymbolicRead().createDirectories()
    } catch (e: IOException) {
        printError("Unable to create logging directory ${logPath.toString()}. Node will now shutdown.")
        return false
    } catch (e: SecurityException) {
        printError("Current user is unable to access logging directory ${logPath.toString()}. Node will now shutdown.")
        return false
    }
    if (!logPath.isDirectory()) {
        printError("Unable to access logging directory ${logPath.toString()}. Node will now shutdown.")
        return false
    }

    SLF4JBridgeHandler.removeHandlersForRootLogger() // The default j.u.l config adds a ConsoleHandler.
    SLF4JBridgeHandler.install()
    return true
}
