package net.corda.node.internal

import net.corda.cliutils.CommonCliConstants
import net.corda.core.internal.div
import net.corda.core.internal.exists
import net.corda.nodeapi.internal.config.UnknownConfigKeysPolicy
import org.assertj.core.api.Assertions
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import picocli.CommandLine
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class NodeStartupCliTest {
    private val startup = NodeStartupCli()

    companion object {
        private lateinit var workingDirectory: Path
        private lateinit var rootDirectory: Path
        private var customNodeConf = "custom_node.conf"
        @BeforeClass
        @JvmStatic
        fun initDirectories() {
            workingDirectory = Paths.get(".").normalize().toAbsolutePath()
            rootDirectory = Paths.get("/").normalize().toAbsolutePath()
        }
    }

    @Test(timeout=300_000)
	fun `no command line arguments`() {
        CommandLine.populateCommand(startup)
        Assertions.assertThat(startup.cmdLineOptions.baseDirectory).isEqualTo(workingDirectory)
        Assertions.assertThat(startup.cmdLineOptions.configFile).isEqualTo(workingDirectory / "node.conf")
        Assertions.assertThat(startup.verbose).isEqualTo(false)
        Assertions.assertThat(startup.loggingLevel).isEqualTo(Level.INFO)
        Assertions.assertThat(startup.cmdLineOptions.noLocalShell).isEqualTo(false)
        Assertions.assertThat(startup.cmdLineOptions.sshdServer).isEqualTo(false)
        Assertions.assertThat(startup.cmdLineOptions.justGenerateNodeInfo).isEqualTo(false)
        Assertions.assertThat(startup.cmdLineOptions.justGenerateRpcSslCerts).isEqualTo(false)
        Assertions.assertThat(startup.cmdLineOptions.unknownConfigKeysPolicy)
                .isEqualTo(UnknownConfigKeysPolicy.FAIL)
        Assertions.assertThat(startup.cmdLineOptions.devMode).isEqualTo(null)
        Assertions.assertThat(startup.cmdLineOptions.clearNetworkMapCache).isEqualTo(false)
        Assertions.assertThat(startup.cmdLineOptions.networkRootTrustStorePathParameter).isEqualTo(null)
    }

    @Test(timeout=300_000)
	fun `--base-directory`() {
        CommandLine.populateCommand(startup, CommonCliConstants.BASE_DIR, (workingDirectory / "another-base-dir").toString())
        Assertions.assertThat(startup.cmdLineOptions.baseDirectory).isEqualTo(workingDirectory / "another-base-dir")
        Assertions.assertThat(startup.cmdLineOptions.configFile).isEqualTo(workingDirectory / "another-base-dir" / "node.conf")
        Assertions.assertThat(startup.cmdLineOptions.networkRootTrustStorePathParameter).isEqualTo(null)
    }

    @Test(timeout=300_000)
    fun `--nodeconf using relative path will be changed to absolute path`() {
        CommandLine.populateCommand(startup, CommonCliConstants.CONFIG_FILE, customNodeConf)
        Assertions.assertThat(startup.cmdLineOptions.configFile).isEqualTo(workingDirectory / customNodeConf)
    }

    @Test(timeout=300_000)
    fun `--nodeconf using absolute path will not be changed`() {
        CommandLine.populateCommand(startup, CommonCliConstants.CONFIG_FILE, (rootDirectory / customNodeConf).toString())
        Assertions.assertThat(startup.cmdLineOptions.configFile).isEqualTo(rootDirectory / customNodeConf)
    }

    @Test(timeout=3_000)
    @Ignore
    fun `test logs are written to correct location correctly if verbose flag set`() {
        val node = NodeStartupCli()
        val dir = Files.createTempDirectory("verboseLoggingTest")
        node.verbose = true
        // With verbose set, initLogging can accidentally attempt to access a logger before all required system properties are set. This
        // causes the logging config to be parsed too early, resulting in logs being written to the wrong directory
        node.initLogging()
        LoggerFactory.getLogger("").debug("Test message")
        assertTrue(dir.resolve("logs").exists())
        assertFalse(Paths.get("./logs").exists())
    }

    @Test(timeout = 300_000)
    fun `assert that logging options are parsed are set correctly when are placed before a command`() {

        val temporaryFolder = TemporaryFolder()
        temporaryFolder.create()
        val baseDir = temporaryFolder.newFolder("baseDir")

        startup.args = arrayOf()

        startup.cmd.parseArgs("--logging-level=DEBUG", "--log-to-console",
                "--base-directory", baseDir.absolutePath,
                "initial-registration", "--network-root-truststore-password=password")

        Assertions.assertThat(startup.loggingLevel).isEqualTo(Level.DEBUG)
        Assertions.assertThat(startup.verbose).isEqualTo(true)
        Assertions.assertThat(startup.cmdLineOptions.baseDirectory.toString()).isEqualTo(baseDir.absolutePath)
    }

    @Test(timeout = 300_000)
    fun `assert that logging options are parsed are set correctly when are placed after a command`() {

        val temporaryFolder = TemporaryFolder()
        temporaryFolder.create()
        val baseDir = temporaryFolder.newFolder("baseDir")

        startup.args = arrayOf()

        startup.cmd.parseArgs(
                "--base-directory", baseDir.absolutePath,
                "initial-registration", "--network-root-truststore-password=password",
                "--logging-level=DEBUG", "--log-to-console"
        )

        Assertions.assertThat(startup.loggingLevel).isEqualTo(Level.DEBUG)
        Assertions.assertThat(startup.verbose).isEqualTo(true)
        Assertions.assertThat(startup.cmdLineOptions.baseDirectory.toString()).isEqualTo(baseDir.absolutePath)
    }
}