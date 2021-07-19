package net.corda.testing.driver

import net.corda.core.DoNotImplement
import net.corda.core.concurrent.CordaFuture
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.concurrent.map
import net.corda.nodeapi.internal.ShutdownHook
import net.corda.testing.node.NotarySpec
import net.corda.testing.node.User
import java.nio.file.Path

enum class VerifierType {
    InMemory
}

/**
 * Underlying interface for the driver DSL. Do not instantiate directly, instead use the [driver] function.
 * */
@DoNotImplement
interface DriverDSL {
    /** Returns a list of [NotaryHandle]s matching the list of [NotarySpec]s passed into [driver]. */
    val notaryHandles: List<NotaryHandle>

    /**
     * Returns the [NotaryHandle] for the single notary on the network. Throws if there are none or more than one.
     * @see notaryHandles
     */
    val defaultNotaryHandle: NotaryHandle
        get() {
            return when (notaryHandles.size) {
                0 -> throw IllegalStateException("There are no notaries defined on the network")
                1 -> notaryHandles[0]
                else -> throw IllegalStateException("There is more than one notary defined on the network")
            }
        }

    /**
     * Returns the identity of the single notary on the network. Throws if there are none or more than one.
     * @see defaultNotaryHandle
     */
    val defaultNotaryIdentity: Party get() = defaultNotaryHandle.identity

    /**
     * Returns a [CordaFuture] on the [NodeHandle] for the single-node notary on the network. Throws if there
     * are no notaries or more than one, or if the notary is a distributed cluster.
     * @see defaultNotaryHandle
     * @see notaryHandles
     */
    val defaultNotaryNode: CordaFuture<NodeHandle>
        get() {
            return defaultNotaryHandle.nodeHandles.map {
                it.singleOrNull() ?: throw IllegalStateException("Default notary is not a single node")
            }
        }

    var autoShutdownNodes: Boolean

    /**
     * Start a node using the default values of [NodeParameters].
     *
     * @return A [CordaFuture] on the [NodeHandle] to the node. The future will complete when the node is available and
     * it sees all previously started nodes, including the notaries.
     */
    fun startNode(): CordaFuture<NodeHandle> = startNode(NodeParameters())

    /**
     * Start a node using the parameter values of the given [NodeParameters].
     *
     * @param parameters The node parameters.
     *
     * @return A [CordaFuture] on the [NodeHandle] to the node. The future will complete when the node is available and
     * it sees all previously started nodes, including the notaries.
     */
    fun startNode(parameters: NodeParameters): CordaFuture<NodeHandle>

    /**
     * Start a node.
     *
     * NOTE: This method does not provide all the node parameters that are available and only exists for backwards compatibility. It is
     * recommended you use [NodeParameters].
     *
     * @param defaultParameters The default parameters for the node. If any of the remaining parameters to this method are specified then
     * their values are taken instead of the corresponding value in [defaultParameters].
     * @param providedName Optional name of the node, which will be its legal name in [Party]. Defaults to something
     *     random. Note that this must be unique as the driver uses it as a primary key!
     * @param rpcUsers List of users who are authorised to use the RPC system. Defaults to empty list.
     * @param verifierType The type of transaction verifier to use. See: [VerifierType].
     * @param customOverrides A map of custom node configuration overrides.
     * @param startInSameProcess Determines if the node should be started inside the same process the Driver is running
     *     in. If null the Driver-level value will be used.
     * @param maximumHeapSize The maximum JVM heap size to use for the node as a [String]. By default a number is interpreted
     *     as being in bytes. Append the letter 'k' or 'K' to the value to indicate Kilobytes, 'm' or 'M' to indicate
     *     megabytes, and 'g' or 'G' to indicate gigabytes. The default value is "512m" = 512 megabytes.
     * @param logLevelOverride log4j log level used to override the default value of info.
     * @return A [CordaFuture] on the [NodeHandle] to the node. The future will complete when the node is available and
     *     it sees all previously started nodes, including the notaries.
     */
    fun startNode(
            defaultParameters: NodeParameters = NodeParameters(),
            providedName: CordaX500Name? = defaultParameters.providedName,
            rpcUsers: List<User> = defaultParameters.rpcUsers,
            verifierType: VerifierType = defaultParameters.verifierType,
            customOverrides: Map<String, Any?> = defaultParameters.customOverrides,
            startInSameProcess: Boolean? = defaultParameters.startInSameProcess,
            maximumHeapSize: String = defaultParameters.maximumHeapSize,
            logLevelOverride: String? = defaultParameters.logLevelOverride
    ): CordaFuture<NodeHandle> {
        return startNode(defaultParameters.copy(
                providedName = providedName,
                rpcUsers = rpcUsers,
                verifierType = verifierType,
                customOverrides = customOverrides,
                startInSameProcess = startInSameProcess,
                maximumHeapSize = maximumHeapSize,
                logLevelOverride = logLevelOverride
        ))
    }

    /**
     * Start a node.
     *
     * NOTE: This method does not provide all the node parameters that are available and only exists for backwards compatibility. It is
     * recommended you use [NodeParameters].
     *
     * @param defaultParameters The default parameters for the node. If any of the remaining parameters to this method are specified then
     * their values are taken instead of the corresponding value in [defaultParameters].
     * @param providedName Optional name of the node, which will be its legal name in [Party]. Defaults to something
     *     random. Note that this must be unique as the driver uses it as a primary key!
     * @param rpcUsers List of users who are authorised to use the RPC system. Defaults to empty list.
     * @param verifierType The type of transaction verifier to use. See: [VerifierType].
     * @param customOverrides A map of custom node configuration overrides.
     * @param startInSameProcess Determines if the node should be started inside the same process the Driver is running
     *     in. If null the Driver-level value will be used.
     * @param maximumHeapSize The maximum JVM heap size to use for the node as a [String]. By default a number is interpreted
     *     as being in bytes. Append the letter 'k' or 'K' to the value to indicate Kilobytes, 'm' or 'M' to indicate
     *     megabytes, and 'g' or 'G' to indicate gigabytes. The default value is "512m" = 512 megabytes.
     * @return A [CordaFuture] on the [NodeHandle] to the node. The future will complete when the node is available and
     *     it sees all previously started nodes, including the notaries.
     */
    fun startNode(
            defaultParameters: NodeParameters = NodeParameters(),
            providedName: CordaX500Name? = defaultParameters.providedName,
            rpcUsers: List<User> = defaultParameters.rpcUsers,
            verifierType: VerifierType = defaultParameters.verifierType,
            customOverrides: Map<String, Any?> = defaultParameters.customOverrides,
            startInSameProcess: Boolean? = defaultParameters.startInSameProcess,
            maximumHeapSize: String = defaultParameters.maximumHeapSize
    ): CordaFuture<NodeHandle> {
        return startNode(defaultParameters.copy(
                providedName = providedName,
                rpcUsers = rpcUsers,
                verifierType = verifierType,
                customOverrides = customOverrides,
                startInSameProcess = startInSameProcess,
                maximumHeapSize = maximumHeapSize
        ))
    }

    /** Call [startWebserver] with a default maximumHeapSize. */
    @Suppress("DEPRECATION")
    fun startWebserver(handle: NodeHandle): CordaFuture<WebserverHandle> = startWebserver(handle, System.getenv("DRIVER_WEB_MEMORY")
            ?: "512m")

    /**
     * Starts a web server for a node
     * @param handle The handle for the node that this webserver connects to via RPC.
     * @param maximumHeapSize Argument for JVM -Xmx option e.g. "200m".
     */
    @Suppress("DEPRECATION")
    fun startWebserver(handle: NodeHandle, maximumHeapSize: String): CordaFuture<WebserverHandle>

    /**
     * Returns the base directory for a node with the given [CordaX500Name]. This method is useful if the base directory
     * is needed before the node is started.
     */
    fun baseDirectory(nodeName: CordaX500Name): Path

    /**
     * Returns the next port to use when instantiating test processes that must not conflict on the same machine
     */
    fun nextPort() = PortAllocation.defaultAllocator.nextPort()

    fun handleAutoShutdown(hook: ShutdownHook, closeable: AutoCloseable?)
}
