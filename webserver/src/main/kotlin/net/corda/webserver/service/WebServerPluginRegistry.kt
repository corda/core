package net.corda.webserver.services

import net.corda.core.messaging.CordaRPCOps
import java.util.function.Function

/**
 * Implement this interface on a class advertised in a META-INF/services/net.corda.webserver.services.WebServerPluginRegistry file
 * to create web API to connect to Corda node via RPC.
 */
abstract class WebServerPluginRegistry {
    /**
     * List of lambdas returning JAX-RS objects. They may only depend on the RPC interface, as the webserver lives
     * in a process separate from the node itself.
     */
    open val webApis: List<Function<CordaRPCOps, out Any>> get() = emptyList()

    /**
     * Map of static serving endpoints to the matching resource directory. All endpoints will be prefixed with "/web" and postfixed with "\*.
     * Resource directories can be either on disk directories (especially when debugging) in the form "a/b/c". Serving from a JAR can
     *  be specified with: javaClass.getResource("<folder-in-jar>").toExternalForm()
     */
    open val staticServeDirs: Map<String, String> get() = emptyMap()

}