@file:Suppress("DEPRECATION")

package net.corda.testing.node.internal

import net.corda.core.concurrent.CordaFuture
import net.corda.core.identity.CordaX500Name
import net.corda.core.internal.ThreadBox
import net.corda.core.internal.concurrent.doneFuture
import net.corda.core.internal.div
import net.corda.core.internal.uncheckedCast
import net.corda.core.utilities.contextLogger
import net.corda.nodeapi.internal.addShutdownHook
import net.corda.testing.driver.DriverParameters
import net.corda.testing.driver.NodeHandle
import net.corda.testing.driver.NodeParameters
import net.corda.testing.driver.NotaryHandle
import net.corda.testing.driver.ReusableDriverDsl
import net.corda.testing.driver.WebserverHandle
import net.corda.testing.driver.internal.NodeHandleInternal
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class ReusableDriver private constructor(private val driver: InternalDriverDSL) : InternalDriverDSL, ReusableDriverDsl, AutoCloseable {
    companion object {
        private class Drivers {
            val drivers = mutableMapOf<DriverParameters, ReusableDriver>()

            fun cleanUpOldDrivers() {
                while(drivers.size > MAX_NUMBER_OF_DRIVERS_TO_REUSE) {
                    val driverToRemove = drivers.filter {
                        it.value.usedCount.get() == 0
                    }.minBy {
                        it.value.lastUsed.get()
                    }
                    if(driverToRemove == null) {
                        // Can't remove anything, will try again later
                        return
                    }
                    drivers.remove(driverToRemove.key)
                    driverToRemove.value.shutdown()
                }
            }
        }
        private val drivers = ThreadBox(Drivers())


        private val shutdownHook = addShutdownHook { closeAll() }

        private var serializationEnabler: AutoCloseable? = null

        private val startTime = Instant.now()

        internal val log = contextLogger()

        private const val MAX_NUMBER_OF_DRIVERS_TO_REUSE = 1
        private const val MAX_NUMBER_OF_NODES_TO_REUSE = 25

        private fun createDriver(parameters: DriverParameters): ReusableDriver {
            val driver = DriverDSLImpl(
                    portAllocation = parameters.portAllocation,
                    debugPortAllocation = parameters.debugPortAllocation,
                    systemProperties = parameters.systemProperties,
                    driverDirectory = parameters.driverDirectory.toAbsolutePath(),
                    useTestClock = parameters.useTestClock,
                    isDebug = parameters.isDebug,
                    startNodesInProcess = parameters.startNodesInProcess,
                    waitForAllNodesToFinish = parameters.waitForAllNodesToFinish,
                    extraCordappPackagesToScan = @Suppress("DEPRECATION") parameters.extraCordappPackagesToScan,
                    notarySpecs = parameters.notarySpecs,
                    jmxPolicy = parameters.jmxPolicy,
                    compatibilityZone = null,
                    networkParameters = parameters.networkParameters,
                    notaryCustomOverrides = parameters.notaryCustomOverrides,
                    inMemoryDB = parameters.inMemoryDB,
                    cordappsForAllNodes = uncheckedCast(parameters.cordappsForAllNodes),
                    djvmBootstrapSource = parameters.djvmBootstrapSource,
                    djvmCordaSource = parameters.djvmCordaSource,
                    environmentVariables = parameters.environmentVariables)
            if (serializationEnabler == null) {
                serializationEnabler = setDriverSerialization(driver.cordappsClassLoader)
            }
            driver.start()
            return ReusableDriver(driver)
        }

        private fun closeAll() {
            shutdownHook.cancel()
            drivers.locked {
                drivers.values.forEach { it.shutdown() }
                drivers.clear()
            }
            serializationEnabler?.close()
        }

        operator fun invoke(parameters: DriverParameters): ReusableDriver {
            val parametersKey = parameters
                    .withDriverDirectory(Paths.get("build") / "node-driver" / "reusable-driver")
                    .withNetworkParameters(parameters.networkParameters.copy(modifiedTime = startTime))

            return drivers.locked {
                val driver = drivers.compute(parametersKey) { _, driver ->
                    driver?.recover() ?: createDriver(parameters)
                }!!

                cleanUpOldDrivers()

                driver
            }
        }

    }

    private val usedCount = AtomicInteger(1)
    private val lastUsed = AtomicLong(System.currentTimeMillis())

    @Suppress("TooGenericExceptionCaught")
    private fun recover(): ReusableDriver? =
        state.locked {
            try {
                recoveries.forEach { it.invoke() }
                recoveries.clear()
                usedCount.incrementAndGet()
                lastUsed.set(System.currentTimeMillis())
                this@ReusableDriver
            } catch (e: Exception) {
                log.warn("Can not recover driver, will use a new one", e)
                shutdown()
                null
            }
        }

    override fun close(): Unit  = state.locked {
        val nodes = startedNodes.groupBy {
            it.second.isDone }

        nodes[false]?.forEach {
            it.second.then { future ->
                future.get()?.stop()
            }
        }

        nodes[true]?.map {
            it.first to it.second.get()
        }?.filter { it.second.running() }
                ?.forEach {
                    if (it.second is NodeHandleInternal) {
                        reusableNodes.getOrPut(it.first, { mutableListOf() })
                                .add(ReusableNodeInfo(it.second as NodeHandleInternal))
                    }
                }

        cleanUpOldNodes()

        startedNodes.clear()
        usedCount.decrementAndGet()
    }



    private data class ReusableNodeInfo(val node: NodeHandleInternal, val lastAccessTime: Long = System.currentTimeMillis())

    private class ThreadSafeState {
        val startedNodes = mutableListOf<Pair<NodeKey, CordaFuture<NodeHandle>>>()
        val reusableNodes = mutableMapOf<NodeKey, MutableList<ReusableNodeInfo>>()
        val runningNamedDriver = mutableMapOf<CordaX500Name, NodeKey>()
        val recoveries = mutableListOf<() -> Unit>()

        fun cleanUpOldNodes()  = {
            val allNodes = reusableNodes.flatMap {pair ->
                pair.value.map {node->
                    node to pair.key } }.
            sortedBy {
                it.first.lastAccessTime }
            allNodes.take(maxOf(0,allNodes.size - MAX_NUMBER_OF_NODES_TO_REUSE)).forEach { pair ->
                if(reusableNodes.getOrDefault(pair.second, mutableListOf()).remove(pair.first)) {
                    pair.first.node.stop()
                }
            }
        }

    }
    private val state = ThreadBox(ThreadSafeState())

    override val shutdownManager: ShutdownManager
        get() = driver.shutdownManager
    override val cordappsClassLoader: ClassLoader?
        get() = driver.cordappsClassLoader

    override fun baseDirectory(nodeName: CordaX500Name): Path =
            driver.baseDirectory(nodeName)

    override fun <A> pollUntilNonNull(pollName: String, pollInterval: Duration, warnCount: Int, check: () -> A?): CordaFuture<A> =
            driver.pollUntilNonNull(pollName, pollInterval, warnCount, check)

    override fun start() =
            driver.start()

    override fun shutdown() =
            driver.shutdown()

    override fun startNode(parameters: NodeParameters, bytemanPort: Int?): CordaFuture<NodeHandle> =
            state.locked{

                val key = NodeKey(parameters, bytemanPort)
                if (parameters.providedName != null) {
                    val runningKey = runningNamedDriver[parameters.providedName]
                    if ((runningKey != null) && (runningKey != key)) {
                        val runningNodes = reusableNodes.remove(runningKey)
                        runningNodes?.forEach { it.node.stop() }
                    }
                    runningNamedDriver[parameters.providedName] = key
                }
                val reusableNodes = reusableNodes.getOrDefault(key, mutableListOf())
                val size = reusableNodes.size
                val nodeFuture = if (size > 0) {
                    log.debug("Reusing node $parameters")
                    val node = reusableNodes.removeAt(size - 1)
                    doneFuture(node.node as NodeHandle)
                } else {
                    log.debug("Creating node $parameters")
                    val node = driver.startNode(parameters, bytemanPort)
                    node
                }
                startedNodes.add(key to nodeFuture)
                return nodeFuture
            }

    override fun startNode(parameters: NodeParameters): CordaFuture<NodeHandle> = startNode(parameters, bytemanPort = null)

    override fun recoverBy(recovery: () -> Unit): Unit = state.locked{
        recoveries.add(recovery)
    }

    override val notaryHandles: List<NotaryHandle>
        get() = driver.notaryHandles

    override fun startWebserver(handle: NodeHandle, maximumHeapSize: String): CordaFuture<WebserverHandle> =
            driver.startWebserver(handle, maximumHeapSize)

    private data class NodeKey(val parameters: NodeParameters, val bytemanPort: Int?)
}