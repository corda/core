package net.corda.nodeapi.internal

import net.corda.core.messaging.ClientRpcSslOptions
import net.corda.core.serialization.internal.nodeSerializationEnv
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.nodeapi.BrokerRpcSslOptions
import net.corda.nodeapi.internal.config.CertificateStore
import net.corda.nodeapi.internal.config.FileBasedCertificateStoreSupplier
import net.corda.nodeapi.internal.config.MutualSslConfiguration
import net.corda.nodeapi.internal.config.SslConfiguration
import org.apache.activemq.artemis.api.core.TransportConfiguration
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants
import java.nio.file.Path

// This avoids internal types from leaking in the public API. The "external" ArtemisTcpTransport delegates to this internal one.
class ArtemisTcpTransport {
    companion object {
        val CIPHER_SUITES = listOf(
                "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
        )

        val TLS_VERSIONS = listOf("TLSv1.2")

        internal fun defaultArtemisOptions(hostAndPort: NetworkHostAndPort) = mapOf(
                // Basic TCP target details.
                TransportConstants.HOST_PROP_NAME to hostAndPort.host,
                TransportConstants.PORT_PROP_NAME to hostAndPort.port,

                // Turn on AMQP support, which needs the protocol jar on the classpath.
                // Unfortunately we cannot disable core protocol as artemis only uses AMQP for interop.
                // It does not use AMQP messages for its own messages e.g. topology and heartbeats.
                // TODO further investigate how to ensure we use a well defined wire level protocol for Node to Node communications.
                TransportConstants.PROTOCOLS_PROP_NAME to "CORE,AMQP",
                TransportConstants.USE_GLOBAL_WORKER_POOL_PROP_NAME to (nodeSerializationEnv != null),
                TransportConstants.REMOTING_THREADS_PROPNAME to (if (nodeSerializationEnv != null) -1 else 1),
                // turn off direct delivery in Artemis - this is latency optimisation that can lead to
                //hick-ups under high load (CORDA-1336)
                TransportConstants.DIRECT_DELIVER to false)

        internal val defaultSSLOptions = mapOf(
                TransportConstants.ENABLED_CIPHER_SUITES_PROP_NAME to CIPHER_SUITES.joinToString(","),
                TransportConstants.ENABLED_PROTOCOLS_PROP_NAME to TLS_VERSIONS.joinToString(","))

        private fun SslConfiguration.toTransportOptions(): Map<String, Any> {

            val options = mutableMapOf<String, Any>()
            (keyStore to trustStore).addToTransportOptions(options)
            return options
        }

        private fun Pair<FileBasedCertificateStoreSupplier?, FileBasedCertificateStoreSupplier?>.addToTransportOptions(options: MutableMap<String, Any>) {

            val keyStore = first
            val trustStore = second
            keyStore?.let {
                with (it) {
                    path.requireOnDefaultFileSystem()
                    options.putAll(get().toKeyStoreTransportOptions(path))
                }
            }
            trustStore?.let {
                with (it) {
                    path.requireOnDefaultFileSystem()
                    options.putAll(get().toTrustStoreTransportOptions(path))
                }
            }
        }

        private fun CertificateStore.toKeyStoreTransportOptions(path: Path) = mapOf(
                TransportConstants.SSL_ENABLED_PROP_NAME to true,
                TransportConstants.KEYSTORE_PROVIDER_PROP_NAME to "JKS",
                TransportConstants.KEYSTORE_PATH_PROP_NAME to path,
                TransportConstants.KEYSTORE_PASSWORD_PROP_NAME to password,
                TransportConstants.NEED_CLIENT_AUTH_PROP_NAME to true)

        private fun CertificateStore.toTrustStoreTransportOptions(path: Path) = mapOf(
                TransportConstants.SSL_ENABLED_PROP_NAME to true,
                TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME to "JKS",
                TransportConstants.TRUSTSTORE_PATH_PROP_NAME to path,
                TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME to password,
                TransportConstants.NEED_CLIENT_AUTH_PROP_NAME to true)

        private fun ClientRpcSslOptions.toTransportOptions() = mapOf(
                TransportConstants.SSL_ENABLED_PROP_NAME to true,
                TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME to trustStoreProvider,
                TransportConstants.TRUSTSTORE_PATH_PROP_NAME to trustStorePath,
                TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME to trustStorePassword)

        private fun BrokerRpcSslOptions.toTransportOptions() = mapOf(
                TransportConstants.SSL_ENABLED_PROP_NAME to true,
                TransportConstants.KEYSTORE_PROVIDER_PROP_NAME to "JKS",
                TransportConstants.KEYSTORE_PATH_PROP_NAME to keyStorePath,
                TransportConstants.KEYSTORE_PASSWORD_PROP_NAME to keyStorePassword,
                TransportConstants.NEED_CLIENT_AUTH_PROP_NAME to false)

        internal val acceptorFactoryClassName = "org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory"
        internal val connectorFactoryClassName = NettyConnectorFactory::class.java.name

        fun p2pAcceptorTcpTransport(hostAndPort: NetworkHostAndPort, config: MutualSslConfiguration?, enableSSL: Boolean = true, lowMemoryMode: Boolean = false): TransportConfiguration {

            return p2pAcceptorTcpTransport(hostAndPort, config?.keyStore, config?.trustStore, enableSSL = enableSSL, useOpenSsl = config?.useOpenSsl ?: false, lowMemoryMode = lowMemoryMode)
        }

        fun p2pConnectorTcpTransport(hostAndPort: NetworkHostAndPort, config: MutualSslConfiguration?, enableSSL: Boolean = true, keyStoreProvider: String? = null, lowMemoryMode: Boolean = false): TransportConfiguration {

            return p2pConnectorTcpTransport(hostAndPort, config?.keyStore, config?.trustStore, enableSSL = enableSSL, useOpenSsl = config?.useOpenSsl ?: false, keyStoreProvider = keyStoreProvider, lowMemoryMode = lowMemoryMode)
        }

        fun p2pAcceptorTcpTransport(hostAndPort: NetworkHostAndPort, keyStore: FileBasedCertificateStoreSupplier?, trustStore: FileBasedCertificateStoreSupplier?, enableSSL: Boolean = true, useOpenSsl: Boolean = false, lowMemoryMode: Boolean = false): TransportConfiguration {

            val options = defaultArtemisOptions(hostAndPort).toMutableMap()
            if (enableSSL) {
                options.putAll(defaultSSLOptions)
                (keyStore to trustStore).addToTransportOptions(options)
                options[TransportConstants.SSL_PROVIDER] = if (useOpenSsl) TransportConstants.OPENSSL_PROVIDER else TransportConstants.DEFAULT_SSL_PROVIDER
            }
            options[TransportConstants.HANDSHAKE_TIMEOUT] = 0 // Suppress core.server.lambda$channelActive$0 - AMQ224088 error from load balancer type connections
            options[TransportConstants.REMOTING_THREADS_PROPNAME] = if (lowMemoryMode) ArtemisConstants.LOW_MEMORY_MODE_REMOTING_THREADS_NUMBER else ArtemisConstants.DEFAULT_REMOTING_THREADS_NUMBER_MULTIPLIER * Runtime.getRuntime().availableProcessors()
            return TransportConfiguration(acceptorFactoryClassName, options)
        }

        @Suppress("LongParameterList")
        fun p2pConnectorTcpTransport(hostAndPort: NetworkHostAndPort, keyStore: FileBasedCertificateStoreSupplier?, trustStore: FileBasedCertificateStoreSupplier?, enableSSL: Boolean = true, useOpenSsl: Boolean = false, keyStoreProvider: String? = null, lowMemoryMode: Boolean = false): TransportConfiguration {

            val options = defaultArtemisOptions(hostAndPort).toMutableMap()
            if (enableSSL) {
                options.putAll(defaultSSLOptions)
                (keyStore to trustStore).addToTransportOptions(options)
                options[TransportConstants.SSL_PROVIDER] = if (useOpenSsl) TransportConstants.OPENSSL_PROVIDER else TransportConstants.DEFAULT_SSL_PROVIDER
                keyStoreProvider?.let { options.put(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME, keyStoreProvider) }
            }
            options[TransportConstants.REMOTING_THREADS_PROPNAME] = if (lowMemoryMode) ArtemisConstants.LOW_MEMORY_MODE_REMOTING_THREADS_NUMBER else ArtemisConstants.DEFAULT_REMOTING_THREADS_NUMBER_MULTIPLIER * Runtime.getRuntime().availableProcessors()
            return TransportConfiguration(connectorFactoryClassName, options)
        }

        fun p2pConnectorTcpTransportFromList(hostAndPortList: List<NetworkHostAndPort>, config: MutualSslConfiguration?, enableSSL: Boolean = true, keyStoreProvider: String? = null, lowMemoryMode: Boolean = false): List<TransportConfiguration> = hostAndPortList.map {
            p2pConnectorTcpTransport(it, config, enableSSL, keyStoreProvider, lowMemoryMode)
        }

        fun rpcAcceptorTcpTransport(hostAndPort: NetworkHostAndPort, config: BrokerRpcSslOptions?, enableSSL: Boolean = true, lowMemoryMode: Boolean = false): TransportConfiguration {
            val options = defaultArtemisOptions(hostAndPort).toMutableMap()

            if (config != null && enableSSL) {
                config.keyStorePath.requireOnDefaultFileSystem()
                options.putAll(config.toTransportOptions())
                options.putAll(defaultSSLOptions)
            }
            options[TransportConstants.HANDSHAKE_TIMEOUT] = 0 // Suppress core.server.lambda$channelActive$0 - AMQ224088 error from load balancer type connections
            options[TransportConstants.REMOTING_THREADS_PROPNAME] = if (lowMemoryMode) ArtemisConstants.LOW_MEMORY_MODE_REMOTING_THREADS_NUMBER else ArtemisConstants.DEFAULT_REMOTING_THREADS_NUMBER_MULTIPLIER * Runtime.getRuntime().availableProcessors()
            return TransportConfiguration(acceptorFactoryClassName, options)
        }

        fun rpcConnectorTcpTransport(hostAndPort: NetworkHostAndPort, config: ClientRpcSslOptions?, enableSSL: Boolean = true, lowMemoryMode: Boolean = false): TransportConfiguration {
            val options = defaultArtemisOptions(hostAndPort).toMutableMap()

            if (config != null && enableSSL) {
                config.trustStorePath.requireOnDefaultFileSystem()
                options.putAll(config.toTransportOptions())
                options.putAll(defaultSSLOptions)
            }
            options[TransportConstants.REMOTING_THREADS_PROPNAME] = if (lowMemoryMode) ArtemisConstants.LOW_MEMORY_MODE_REMOTING_THREADS_NUMBER else ArtemisConstants.DEFAULT_REMOTING_THREADS_NUMBER_MULTIPLIER * Runtime.getRuntime().availableProcessors()
            return TransportConfiguration(connectorFactoryClassName, options)
        }

        fun rpcConnectorTcpTransportsFromList(hostAndPortList: List<NetworkHostAndPort>, config: ClientRpcSslOptions?, enableSSL: Boolean = true, lowMemoryMode: Boolean = false): List<TransportConfiguration> = hostAndPortList.map {
            rpcConnectorTcpTransport(it, config, enableSSL, lowMemoryMode)
        }

        fun rpcInternalClientTcpTransport(hostAndPort: NetworkHostAndPort, config: SslConfiguration, keyStoreProvider: String? = null): TransportConfiguration {
            return TransportConfiguration(connectorFactoryClassName, defaultArtemisOptions(hostAndPort) + defaultSSLOptions + config.toTransportOptions() + asMap(keyStoreProvider))
        }

        fun rpcInternalAcceptorTcpTransport(hostAndPort: NetworkHostAndPort, config: SslConfiguration, keyStoreProvider: String? = null): TransportConfiguration {
            return TransportConfiguration(acceptorFactoryClassName, defaultArtemisOptions(hostAndPort) + defaultSSLOptions +
                    config.toTransportOptions() + (TransportConstants.HANDSHAKE_TIMEOUT to 0) + asMap(keyStoreProvider))
        }

        private fun asMap(keyStoreProvider: String?): Map<String, String>  {
            return keyStoreProvider?.let {mutableMapOf(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME to it)} ?: emptyMap()
        }
    }
}