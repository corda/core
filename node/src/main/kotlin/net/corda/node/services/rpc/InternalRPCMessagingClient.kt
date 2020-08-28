package net.corda.node.services.rpc

import net.corda.core.identity.CordaX500Name
import net.corda.core.internal.NamedCacheFactory
import net.corda.core.messaging.RPCOps
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.serialization.internal.nodeSerializationEnv
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.node.internal.security.RPCSecurityManager
import net.corda.nodeapi.internal.ArtemisConstants
import net.corda.nodeapi.internal.ArtemisMessagingComponent.Companion.NODE_RPC_USER
import net.corda.nodeapi.internal.ArtemisTcpTransport
import net.corda.nodeapi.internal.config.MutualSslConfiguration
import org.apache.activemq.artemis.api.core.client.ActiveMQClient
import org.apache.activemq.artemis.api.core.client.ServerLocator
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl

/**
 * Used by the Node to communicate with the RPC broker.
 */
class InternalRPCMessagingClient(val sslConfig: MutualSslConfiguration, val serverAddress: NetworkHostAndPort, val maxMessageSize: Int, val nodeName: CordaX500Name, val rpcServerConfiguration: RPCServerConfiguration) : SingletonSerializeAsToken(), AutoCloseable {
    private var locator: ServerLocator? = null
    private var rpcServer: RPCServer? = null

    fun init(rpcOps: RPCOps, securityManager: RPCSecurityManager, cacheFactory: NamedCacheFactory, lowMemoryMode: Boolean = false) = synchronized(this) {

        val tcpTransport = ArtemisTcpTransport.rpcInternalClientTcpTransport(serverAddress, sslConfig)
        locator = ActiveMQClient.createServerLocatorWithoutHA(tcpTransport).apply {
            // Never time out on our loopback Artemis connections. If we switch back to using the InVM transport this
            // would be the default and the two lines below can be deleted.
            connectionTTL = 60000
            clientFailureCheckPeriod = 30000
            minLargeMessageSize = maxMessageSize
            isUseGlobalPools = nodeSerializationEnv != null
            threadPoolMaxSize = if (lowMemoryMode) ArtemisConstants.LOW_MEMORY_MODE_THREAD_POOL_MAX_SIZE else ArtemisConstants.DEFAULT_THREAD_POOL_MAX_SIZE
        }

        rpcServer = RPCServer(rpcOps, NODE_RPC_USER, NODE_RPC_USER, locator!!, securityManager, nodeName, rpcServerConfiguration, cacheFactory)
    }

    fun start(serverControl: ActiveMQServerControl) = synchronized(this) {
        rpcServer!!.start(serverControl)
    }

    fun stop(): Unit = synchronized(this) {
        rpcServer?.close()
        locator?.close()
    }

    override fun close() = stop()
}
