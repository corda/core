package net.corda.testing.driver.internal

import net.corda.core.flows.FlowLogic
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.node.NodeInfo
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.node.internal.NodeWithInfo
import net.corda.node.services.api.StartedNodeServices
import net.corda.node.services.config.NodeConfiguration
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.testing.driver.InProcess
import net.corda.testing.driver.NodeHandle
import net.corda.testing.driver.OutOfProcess
import net.corda.testing.node.User
import rx.Observable
import java.nio.file.Path
import javax.validation.constraints.NotNull

interface NodeHandleInternal : NodeHandle {
    val configuration: NodeConfiguration
    val useHTTPS: Boolean
    val webAddress: NetworkHostAndPort
    override val p2pAddress: NetworkHostAndPort get() = configuration.p2pAddress
    override val rpcAddress: NetworkHostAndPort get() = configuration.rpcOptions.address
    override val rpcAdminAddress: NetworkHostAndPort get() = configuration.rpcOptions.adminAddress
    override val jmxAddress: NetworkHostAndPort? get() = configuration.jmxMonitoringHttpPort?.let { NetworkHostAndPort("localhost", it) }
    override val baseDirectory: Path get() = configuration.baseDirectory
}

data class OutOfProcessImpl(
        override val nodeInfo: NodeInfo,
        override val rpc: CordaRPCOps,
        override val configuration: NodeConfiguration,
        override val webAddress: NetworkHostAndPort,
        override val useHTTPS: Boolean,
        val debugPort: Int?,
        override val process: Process,
        val onStopCallback: () -> Unit
) : OutOfProcess, NodeHandleInternal {
    override val rpcUsers: List<User> = configuration.rpcUsers.map { User(it.username, it.password, it.permissions) }
    override fun stop() {
        with(process) {
            destroy()
            waitFor()
        }
        onStopCallback()
    }

    override fun close() = stop()
}

data class InProcessImpl(
        override val nodeInfo: NodeInfo,
        override val rpc: CordaRPCOps,
        override val configuration: NodeConfiguration,
        override val webAddress: NetworkHostAndPort,
        override val useHTTPS: Boolean,
        private val nodeThread: Thread,
        private val onStopCallback: () -> Unit,
        private val node: NodeWithInfo
) : InProcess, NodeHandleInternal {
    val database: CordaPersistence = node.node.database
    override val services: StartedNodeServices get() = node.services
    override val rpcUsers: List<User> = configuration.rpcUsers.map { User(it.username, it.password, it.permissions) }
    override fun stop() {
        node.dispose()
        with(nodeThread) {
            interrupt()
            join()
        }
        onStopCallback()
    }

    override fun close() = stop()
    @NotNull
    override fun <T : FlowLogic<*>> registerInitiatedFlow(initiatedFlowClass: Class<T>): Observable<T> {
        node.registerInitiatedFlow(initiatedFlowClass)
        return Observable.empty()
    }

    override fun running(): Boolean = nodeThread.isAlive
}

val InProcess.internalServices: StartedNodeServices get() = services as StartedNodeServices
