@file:JvmName("Main")

package net.corda.networkbuilder

import javafx.application.Application
import net.corda.networkbuilder.backends.Backend
import net.corda.networkbuilder.backends.Backend.BackendType.AZURE
import net.corda.networkbuilder.cli.AzureParser
import net.corda.networkbuilder.cli.CliParser
import net.corda.networkbuilder.cli.CommandLineInterface
import net.corda.networkbuilder.docker.DockerUtils
import net.corda.networkbuilder.gui.Gui
import net.corda.networkbuilder.serialization.SerializationEngine
import picocli.CommandLine
import javax.ws.rs.ProcessingException
import kotlin.system.exitProcess

val baseArgs = CliParser()

fun main(args: Array<String>) {
    SerializationEngine.init()
    CommandLine(baseArgs).parseArgs(*args)
    testDockerConnectivity()

    if (baseArgs.usageHelpRequested) {
        CommandLine.usage(CliParser(), System.out)
        return
    }

    if (baseArgs.gui) {
        Application.launch(Gui::class.java)
        return
    }

    val argParser: CliParser = when (baseArgs.backendType) {
        AZURE -> {
            val azureArgs = AzureParser()
            CommandLine(azureArgs).parseArgs(*args)
            azureArgs
        }
        Backend.BackendType.LOCAL_DOCKER -> baseArgs
    }
    CommandLineInterface().run(argParser)
}

private fun testDockerConnectivity() {
    try {
        DockerUtils.createLocalDockerClient().listImagesCmd().exec()
    } catch (se: ProcessingException) {
        if (baseArgs.gui) {
            GuiUtils.showAndQuit("Could not connect to Docker", "Please ensure that docker is running locally", null)
        } else {
            System.err.println("Could not connect to Docker, please ensure that docker is running locally")
            exitProcess(1)
        }
    }
}
