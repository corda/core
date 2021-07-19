package net.corda.node.internal.subcommands

import com.typesafe.config.Config
import com.typesafe.config.ConfigRenderOptions
import net.corda.cliutils.CliWrapperBase
import net.corda.cliutils.ExitCodes
import net.corda.common.configuration.parsing.internal.Configuration
import net.corda.core.utilities.loggerFor
import net.corda.node.SharedNodeCmdLineOptions
import net.corda.node.services.config.schema.v1.V1NodeConfigurationSpec
import net.corda.nodeapi.internal.config.toConfigValue
import picocli.CommandLine.Mixin

internal class ValidateConfigurationCli : CliWrapperBase("validate-configuration", "Validate the configuration without starting the node.") {
    internal companion object {
        private val logger by lazy { loggerFor<ValidateConfigurationCli>() }

        private val configRenderingOptions = ConfigRenderOptions.defaults().setFormatted(true).setComments(false).setOriginComments(false)

        internal fun logConfigurationErrors(errors: Iterable<Configuration.Validation.Error>) {
            logger.error(errors.joinToString(System.lineSeparator(), "Error(s) while parsing node configuration:${System.lineSeparator()}") { error -> "\t- ${error.description()}" })
        }

        private fun Configuration.Validation.Error.description(): String {
            return "for path: \"$pathAsString\": $message"
        }

        internal fun logRawConfig(config: Config) = logger.info("Actual configuration:\n${V1NodeConfigurationSpec.describe(config, Any?::toConfigValue, Configuration.Options()).render(configRenderingOptions)}")
    }

    @Mixin
    private val cmdLineOptions = SharedNodeCmdLineOptions()

    override fun runProgram(): Int {
        val rawConfig = cmdLineOptions.rawConfiguration().doOnErrors(cmdLineOptions::logRawConfigurationErrors).optional ?: return ExitCodes.FAILURE
        return cmdLineOptions.parseConfiguration(rawConfig).doIfValid { logRawConfig(rawConfig) }.doOnErrors(::logConfigurationErrors).optional?.let { ExitCodes.SUCCESS } ?: ExitCodes.FAILURE
    }
}
