package net.corda.client.rpc.internal

import net.corda.client.rpc.CordaRPCClientConfiguration
import net.corda.client.rpc.RPCConnection
import net.corda.client.rpc.UnrecoverableRPCException
import net.corda.core.context.Actor
import net.corda.core.context.Trace
import net.corda.core.crypto.random63BitValue
import net.corda.core.identity.CordaX500Name
import net.corda.core.internal.logElapsedTime
import net.corda.core.internal.uncheckedCast
import net.corda.core.messaging.ClientRpcSslOptions
import net.corda.core.messaging.RPCOps
import net.corda.core.serialization.SerializationContext
import net.corda.core.serialization.SerializationDefaults
import net.corda.core.serialization.internal.nodeSerializationEnv
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.contextLogger
import net.corda.nodeapi.RPCApi
import net.corda.nodeapi.internal.ArtemisConstants
import net.corda.nodeapi.internal.ArtemisTcpTransport.Companion.rpcConnectorTcpTransport
import net.corda.nodeapi.internal.ArtemisTcpTransport.Companion.rpcConnectorTcpTransportsFromList
import net.corda.nodeapi.internal.ArtemisTcpTransport.Companion.rpcInternalClientTcpTransport
import net.corda.nodeapi.internal.config.SslConfiguration
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.activemq.artemis.api.core.TransportConfiguration
import org.apache.activemq.artemis.api.core.client.ActiveMQClient
import java.lang.reflect.Proxy

/**
 * This runs on the client JVM
 */
class RPCClient<I : RPCOps>(
        val transport: TransportConfiguration,
        val rpcConfiguration: CordaRPCClientConfiguration = CordaRPCClientConfiguration.DEFAULT,
        val serializationContext: SerializationContext = SerializationDefaults.RPC_CLIENT_CONTEXT,
        val haPoolTransportConfigurations: List<TransportConfiguration> = emptyList(),
        val lowMemoryMode: Boolean = false
) {
    constructor(
            hostAndPort: NetworkHostAndPort,
            sslConfiguration: ClientRpcSslOptions? = null,
            configuration: CordaRPCClientConfiguration = CordaRPCClientConfiguration.DEFAULT,
            serializationContext: SerializationContext = SerializationDefaults.RPC_CLIENT_CONTEXT,
            lowMemoryMode: Boolean = false
    ) : this(rpcConnectorTcpTransport(hostAndPort, sslConfiguration, lowMemoryMode = lowMemoryMode), configuration, serializationContext)

    constructor(
            hostAndPort: NetworkHostAndPort,
            sslConfiguration: SslConfiguration,
            configuration: CordaRPCClientConfiguration = CordaRPCClientConfiguration.DEFAULT,
            serializationContext: SerializationContext = SerializationDefaults.RPC_CLIENT_CONTEXT
    ) : this(rpcInternalClientTcpTransport(hostAndPort, sslConfiguration), configuration, serializationContext)

    constructor(
            haAddressPool: List<NetworkHostAndPort>,
            sslConfiguration: ClientRpcSslOptions? = null,
            configuration: CordaRPCClientConfiguration = CordaRPCClientConfiguration.DEFAULT,
            serializationContext: SerializationContext = SerializationDefaults.RPC_CLIENT_CONTEXT,
            lowMemoryMode: Boolean = false
    ) : this(rpcConnectorTcpTransport(haAddressPool.first(), sslConfiguration, lowMemoryMode = lowMemoryMode),
            configuration, serializationContext, rpcConnectorTcpTransportsFromList(haAddressPool, sslConfiguration, lowMemoryMode = lowMemoryMode), lowMemoryMode)

    companion object {
        private val log = contextLogger()
    }

    fun start(
            rpcOpsClass: Class<I>,
            username: String,
            password: String,
            externalTrace: Trace? = null,
            impersonatedActor: Actor? = null,
            targetLegalIdentity: CordaX500Name? = null
    ): RPCConnection<I> {
        return log.logElapsedTime("Startup") {
            val clientAddress = SimpleString("${RPCApi.RPC_CLIENT_QUEUE_NAME_PREFIX}.$username.${random63BitValue()}")

            val serverLocator = (if (haPoolTransportConfigurations.isEmpty()) {
                ActiveMQClient.createServerLocatorWithoutHA(transport)
            } else {
                ActiveMQClient.createServerLocatorWithoutHA(*haPoolTransportConfigurations.toTypedArray())
            }).apply {
                connectionTTL = 60000
                clientFailureCheckPeriod = 30000
                retryInterval = rpcConfiguration.connectionRetryInterval.toMillis()
                retryIntervalMultiplier = rpcConfiguration.connectionRetryIntervalMultiplier
                maxRetryInterval = rpcConfiguration.connectionMaxRetryInterval.toMillis()
                reconnectAttempts = if (haPoolTransportConfigurations.isEmpty()) rpcConfiguration.maxReconnectAttempts else 0
                minLargeMessageSize = rpcConfiguration.maxFileSize
                isUseGlobalPools = nodeSerializationEnv != null
                threadPoolMaxSize = if (lowMemoryMode) ArtemisConstants.LOW_MEMORY_MODE_THREAD_POOL_MAX_SIZE else ArtemisConstants.DEFAULT_THREAD_POOL_MAX_SIZE
            }
            val sessionId = Trace.SessionId.newInstance()
            val proxyHandler = RPCClientProxyHandler(rpcConfiguration, username, password, serverLocator, clientAddress,
                    rpcOpsClass, serializationContext, sessionId, externalTrace, impersonatedActor, targetLegalIdentity)
            try {
                proxyHandler.start()
                val ops: I = uncheckedCast(Proxy.newProxyInstance(rpcOpsClass.classLoader, arrayOf(rpcOpsClass), proxyHandler))
                val serverProtocolVersion = ops.protocolVersion
                if (serverProtocolVersion < rpcConfiguration.minimumServerProtocolVersion) {
                    throw UnrecoverableRPCException("Requested minimum protocol version " +
                            "(${rpcConfiguration.minimumServerProtocolVersion}) is higher" +
                            " than the server's supported protocol version ($serverProtocolVersion)")
                }
                proxyHandler.setServerProtocolVersion(serverProtocolVersion)

                log.debug("RPC connected, returning proxy")
                object : RPCConnection<I> {
                    override val proxy = ops
                    override val serverProtocolVersion = serverProtocolVersion

                    private fun close(notify: Boolean) {
                        if (notify) {
                            proxyHandler.notifyServerAndClose()
                        } else {
                            proxyHandler.forceClose()
                        }
                        serverLocator.close()
                    }

                    override fun notifyServerAndClose() {
                        close(true)
                    }

                    override fun forceClose() {
                        close(false)
                    }
                }
            } catch (exception: Throwable) {
                proxyHandler.notifyServerAndClose()
                serverLocator.close()
                throw exception
            }
        }
    }
}

