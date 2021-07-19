package net.corda.cliutils

import net.corda.core.internal.rootMessage
import net.corda.core.utilities.contextLogger
import org.fusesource.jansi.AnsiConsole
import org.slf4j.event.Level
import picocli.CommandLine
import picocli.CommandLine.*
import java.io.PrintWriter
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.Callable
import kotlin.system.exitProcess

/**
 * When we have errors in command line flags that are not handled by picocli (e.g. non existing files), an error is thrown
 * without any command line help afterwards. This can be called from run() method.
 */
interface Validated {
    companion object {
        val logger = contextLogger()
    }

    /**
     * Check that provided command line parameters are valid, e.g. check file existence. Return list of error strings.
     */
    fun validator(): List<String>

    /**
     * Function that provides nice error handing of command line validation.
     */
    fun validate() {
        val errors = validator()
        if (errors.isNotEmpty()) {
            logger.error(ShellConstants.RED + "Exceptions when parsing command line arguments:")
            logger.error(errors.joinToString("\n") + ShellConstants.RESET)
            CommandLine(this).usage(System.err)
            exitProcess(ExitCodes.FAILURE)
        }
    }
}

/** This is generally covered by commons-lang. */
object CordaSystemUtils {
    private const val OS_NAME = "os.name"
    private const val MAC_PREFIX = "Mac"
    private const val WIN_PREFIX = "Windows"

    fun isOsMac(): Boolean = getOsName().startsWith(MAC_PREFIX)
    fun isOsWindows(): Boolean = getOsName().startsWith(WIN_PREFIX)
    fun getOsName(): String = System.getProperty(OS_NAME)
}

object ShellConstants {
    const val RED = "\u001B[31m"
    const val YELLOW = "\u001B[33m"
    const val RESET = "\u001B[0m"
}

fun CordaCliWrapper.start(args: Array<String>) {
    this.args = args

    // This line makes sure ANSI escapes work on Windows, where they aren't supported out of the box.
    AnsiConsole.systemInstall()

    val defaultAnsiMode = if (CordaSystemUtils.isOsWindows()) {
        Help.Ansi.ON
    } else {
        Help.Ansi.AUTO
    }

    // only print stacktraces if verbose requested by users
    cmd.executionExceptionHandler = IExecutionExceptionHandler { ex: Exception, _: CommandLine, _: ParseResult ->
        val throwable = ex.cause ?: ex
        if (this@start.verbose) {
            throwable.printStackTrace()
        }
        printError(throwable.rootMessage ?: "Use --verbose for more details")
        ExitCodes.FAILURE
    }

    // init logging before invoking the business logic
    cmd.executionStrategy = IExecutionStrategy { parseResult: ParseResult ->
        initLogging()
        RunLast().execute(parseResult)
    }


    cmd.err = PrintWriter(System.err)
    cmd.out = PrintWriter(System.out)
    cmd.colorScheme = Help.defaultColorScheme(defaultAnsiMode)

    @Suppress("SpreadOperator")
    cmd.execute(*args).let {
        if (it == ExitCode.OK) {
            exitProcess(ExitCodes.SUCCESS)
        } else {
            exitProcess(ExitCodes.FAILURE)
        }
    }
}

@Command(mixinStandardHelpOptions = true,
        versionProvider = CordaVersionProvider::class,
        sortOptions = false,
        showDefaultValues = true,
        synopsisHeading = "%n@|bold,underline Usage|@:%n%n",
        descriptionHeading = "%n@|bold,underline Description|@:%n%n",
        parameterListHeading = "%n@|bold,underline Parameters|@:%n%n",
        optionListHeading = "%n@|bold,underline Options|@:%n%n",
        commandListHeading = "%n@|bold,underline Commands|@:%n%n")
abstract class CliWrapperBase(val alias: String, val description: String) : Callable<Int> {
    companion object {
        private val logger by lazy { contextLogger() }
    }

    // Raw args are provided for use in logging - this is a lateinit var rather than a constructor parameter as the class
    // needs to be parameterless for autocomplete to work.
    lateinit var args: Array<String>

    // Override this function with the actual method to be run once all the arguments have been parsed. The return number
    // is the exit code to be returned
    abstract fun runProgram(): Int

    override fun call(): Int {
        logger.info("Application Args: ${args.joinToString(" ")}")
        return runProgram()
    }
}

/**
 * Simple base class for handling help, version, verbose and logging-level commands.
 * As versionProvider information from the MANIFEST file is used. It can be overwritten by custom version providers (see: Node)
 * Picocli will prioritise versionProvider from the `@Command` annotation on the subclass, see: https://picocli.info/#_reuse_combinations
 */
abstract class CordaCliWrapper(alias: String, description: String) : CliWrapperBase(alias, description) {
    companion object {
        private val logger by lazy { contextLogger() }
    }

    private val installShellExtensionsParser = InstallShellExtensionsParser(this)

    @Option(names = ["-v", "--verbose", "--log-to-console"], scope = ScopeType.INHERIT,
            description = ["If set, prints logging to the console as well as to a file."])
    var verbose: Boolean = false

    @Option(names = ["--logging-level"], scope = ScopeType.INHERIT,
            completionCandidates = LoggingLevelConverter.LoggingLevels::class,
            description = ["Enable logging at this level and higher. Possible values: \${COMPLETION-CANDIDATES}"],
            converter = [LoggingLevelConverter::class]
    )
    var loggingLevel: Level = Level.INFO

    val specifiedLogLevel: String by lazy {
        System.getProperty("log4j2.level")?.toLowerCase(Locale.ENGLISH) ?: loggingLevel.name.toLowerCase(Locale.ENGLISH)
    }

    // This needs to be called before loggers (See: NodeStartup.kt:51 logger called by lazy, initLogging happens before).
    // Node's logging is more rich. In corda configurations two properties, defaultLoggingLevel and consoleLogLevel, are usually used.
    open fun initLogging(): Boolean {
        System.setProperty("defaultLogLevel", specifiedLogLevel) // These properties are referenced from the XML config file.
        if (verbose) {
            System.setProperty("consoleLogLevel", specifiedLogLevel)
        }
        System.setProperty("log-path", Paths.get(".").toString())
        return true
    }

    protected open fun additionalSubCommands(): Set<CliWrapperBase> = emptySet()

    val cmd by lazy {
        CommandLine(this).apply {
            // Make sure any provided paths are absolute. Relative paths have caused issues and are less clear in logs.
            registerConverter(Path::class.java) { Paths.get(it).toAbsolutePath().normalize() }
            commandSpec.name(alias)
            commandSpec.usageMessage().description(description)
            subCommands.forEach {
                val subCommand = CommandLine(it)
                it.args = args
                subCommand.commandSpec.usageMessage().description(it.description)
                commandSpec.addSubcommand(it.alias, subCommand)
            }
        }
    }

    val subCommands: Set<CliWrapperBase> by lazy {
        additionalSubCommands() + installShellExtensionsParser
    }

    override fun call(): Int {
        if (!initLogging()) {
            return ExitCodes.FAILURE
        }
        logger.info("Application Args: ${args.joinToString(" ")}")
        installShellExtensionsParser.updateShellExtensions()
        return runProgram()
    }

    fun printHelp() = cmd.usage(System.out)
}

fun printWarning(message: String) = System.err.println("${ShellConstants.YELLOW}$message${ShellConstants.RESET}")
fun printError(message: String) = System.err.println("${ShellConstants.RED}$message${ShellConstants.RESET}")

/**
 * Useful commonly used constants applicable to many CLI tools
 */
object CommonCliConstants {
    const val BASE_DIR = "--base-directory"
    const val CONFIG_FILE = "--config-file"
}

/**
 * Converter from String to slf4j logging Level.
 */
class LoggingLevelConverter : ITypeConverter<Level> {
    override fun convert(value: String?): Level {
        return value?.let { Level.valueOf(it.toUpperCase()) }
                ?: throw TypeConversionException("Unknown option for --logging-level: $value")
    }

    class LoggingLevels : ArrayList<String>(Level.values().map { it.toString() })
}
