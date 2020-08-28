package net.corda.node.services.messaging

import net.corda.core.internal.ThreadBox
import net.corda.core.internal.div
import net.corda.core.internal.errors.AddressBindingException
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.debug
import net.corda.node.internal.artemis.*
import net.corda.node.internal.artemis.BrokerJaasLoginModule.Companion.NODE_P2P_ROLE
import net.corda.node.internal.artemis.BrokerJaasLoginModule.Companion.PEER_ROLE
import net.corda.node.services.config.NodeConfiguration
import net.corda.nodeapi.internal.AmqpMessageSizeChecksInterceptor
import net.corda.nodeapi.internal.ArtemisConstants
import net.corda.nodeapi.internal.ArtemisMessageSizeChecksInterceptor
import net.corda.nodeapi.internal.ArtemisMessagingComponent.Companion.INTERNAL_PREFIX
import net.corda.nodeapi.internal.ArtemisMessagingComponent.Companion.JOURNAL_HEADER_SIZE
import net.corda.nodeapi.internal.ArtemisMessagingComponent.Companion.NOTIFICATIONS_ADDRESS
import net.corda.nodeapi.internal.ArtemisMessagingComponent.Companion.P2P_PREFIX
import net.corda.nodeapi.internal.ArtemisMessagingComponent.Companion.SECURITY_INVALIDATION_INTERVAL
import net.corda.nodeapi.internal.ArtemisTcpTransport.Companion.p2pAcceptorTcpTransport
import net.corda.nodeapi.internal.protonwrapper.netty.RevocationConfig
import net.corda.nodeapi.internal.requireOnDefaultFileSystem
import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl
import org.apache.activemq.artemis.core.config.Configuration
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration
import org.apache.activemq.artemis.core.security.Role
import org.apache.activemq.artemis.core.server.ActiveMQServer
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager
import java.io.IOException
import java.lang.Long.max
import java.security.KeyStoreException
import javax.annotation.concurrent.ThreadSafe
import javax.security.auth.login.AppConfigurationEntry
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED

// TODO: Verify that nobody can connect to us and fiddle with our config over the socket due to the secman.
// TODO: Implement a discovery engine that can trigger builds of new connections when another node registers? (later)

/**
 * This class configures and manages an Apache Artemis message queue broker.
 *
 * Nodes communication is managed using an Artemis specific protocol, but it supports other protocols like AMQP/1.0
 * as well for interop.
 *
 * The current implementation is skeletal and lacks features like security or firewall tunnelling (that is, you must
 * be able to receive TCP connections in order to receive messages). It is good enough for local communication within
 * a fully connected network, trusted network or on localhost.
 */
@ThreadSafe
class ArtemisMessagingServer(private val config: NodeConfiguration,
                             private val messagingServerAddress: NetworkHostAndPort,
                             private val maxMessageSize: Int,
                             private val journalBufferTimeout : Int?,
                             private val lowMemoryMode: Boolean = false) : ArtemisBroker, SingletonSerializeAsToken() {
    companion object {
        private val log = contextLogger()
    }

    private class InnerState {
        var running = false
    }

    private val mutex = ThreadBox(InnerState())
    private lateinit var activeMQServer: ActiveMQServer
    override val serverControl: ActiveMQServerControl get() = activeMQServer.activeMQServerControl

    init {
        config.baseDirectory.requireOnDefaultFileSystem()
    }

    override fun start() = mutex.locked {
        if (!running) {
            configureAndStartServer()
            running = true
        }
    }

    override fun stop() = mutex.locked {
        activeMQServer.stop()
        running = false
    }

    override val addresses = config.p2pAddress.let { BrokerAddresses(it, it) }

    override val started: Boolean
        get() = activeMQServer.isStarted

    // TODO: Maybe wrap [IOException] on a key store load error so that it's clearly splitting key store loading from
    // Artemis IO errors
    @Throws(IOException::class, AddressBindingException::class, KeyStoreException::class)
    private fun configureAndStartServer() {
        val artemisConfig = createArtemisConfig()
        val securityManager = createArtemisSecurityManager()
        activeMQServer = ActiveMQServerImpl(artemisConfig, securityManager).apply {
            // Throw any exceptions which are detected during startup
            registerActivationFailureListener { exception -> throw exception }
            // Some types of queue might need special preparation on our side, like dialling back or preparing
            // a lazily initialised subsystem.
            registerPostQueueCreationCallback { log.debug { "Queue Created: $it" } }
            registerPostQueueDeletionCallback { address, qName -> log.debug { "Queue deleted: $qName for $address" } }
        }

        try {
            activeMQServer.start()
        } catch (e: IOException) {
            log.error("Unable to start message broker", e)
            if (e.isBindingError()) {
                throw AddressBindingException(config.p2pAddress)
            } else {
                throw e
            }
        }
        activeMQServer.remotingService.addIncomingInterceptor(ArtemisMessageSizeChecksInterceptor(maxMessageSize))
        activeMQServer.remotingService.addIncomingInterceptor(AmqpMessageSizeChecksInterceptor(maxMessageSize))
        // Config driven switch between legacy CORE bridges and the newer AMQP protocol bridges.
        log.info("P2P messaging server listening on $messagingServerAddress")
    }

    private fun createArtemisConfig() = SecureArtemisConfiguration().apply {
        name = "P2P"

        val artemisDir = config.baseDirectory / "artemis"
        bindingsDirectory = (artemisDir / "bindings").toString()
        journalDirectory = (artemisDir / "journal").toString()
        largeMessagesDirectory = (artemisDir / "large-messages").toString()
        pagingDirectory = (artemisDir / "paging").toString()
        // The transaction cache is configurable, and drives other cache sizes.
        globalMaxSize = max(config.transactionCacheSizeBytes, 10L * maxMessageSize)

        acceptorConfigurations = mutableSetOf(p2pAcceptorTcpTransport(NetworkHostAndPort(messagingServerAddress.host, messagingServerAddress.port), config.p2pSslOptions, lowMemoryMode = lowMemoryMode))
        // Enable built in message deduplication. Note we still have to do our own as the delayed commits
        // and our own definition of commit mean that the built in deduplication cannot remove all duplicates.
        idCacheSize = if (lowMemoryMode) ArtemisConstants.LOW_MEMORY_MODE_ID_CACHE_SIZE else ArtemisConstants.DEFAULT_ID_CACHE_SIZE // Artemis Default duplicate cache size i.e. a guess
        isPersistIDCache = true
        isPopulateValidatedUser = true
        journalBufferSize_NIO = maxMessageSize + JOURNAL_HEADER_SIZE // Artemis default is 490KiB - required to address IllegalArgumentException (when Artemis uses Java NIO): Record is too large to store.
        journalBufferSize_AIO = maxMessageSize + JOURNAL_HEADER_SIZE // Required to address IllegalArgumentException (when Artemis uses Linux Async IO): Record is too large to store.
        journalBufferTimeout_NIO = journalBufferTimeout ?: ActiveMQDefaultConfiguration.getDefaultJournalBufferTimeoutNio()
        journalBufferTimeout_AIO = journalBufferTimeout ?: ActiveMQDefaultConfiguration.getDefaultJournalBufferTimeoutAio()
        journalFileSize = maxMessageSize + JOURNAL_HEADER_SIZE// The size of each journal file in bytes. Artemis default is 10MiB.
        managementNotificationAddress = SimpleString(NOTIFICATIONS_ADDRESS)

        // JMX enablement
        if (config.jmxMonitoringHttpPort != null) {
            isJMXManagementEnabled = true
            isJMXUseBrokerName = true
        }
        // Validate user in AMQP message header against authenticated session
        registerBrokerPlugin(UserValidationPlugin())

    }.configureAddressSecurity()

    /**
     * Authenticated clients connecting to us fall in one of the following groups:
     * 1. The node itself. It is given full access to all valid queues.
     * 2. Peers on the same network as us. These are only given permission to send to our P2P inbound queue.
     * 3. RPC users. These are only given sufficient access to perform RPC with us.
     * 4. Verifiers. These are given read access to the verification request queue and write access to the response queue.
     */
    private fun ConfigurationImpl.configureAddressSecurity(): Configuration {
        val nodeInternalRole = Role(NODE_P2P_ROLE, true, true, true, true, true, true, true, true, true, true)
        securityRoles["$INTERNAL_PREFIX#"] = setOf(nodeInternalRole)  // Do not add any other roles here as it's only for the node
        securityRoles["$P2P_PREFIX#"] = setOf(nodeInternalRole, restrictedRole(PEER_ROLE, send = true))
        securityInvalidationInterval = SECURITY_INVALIDATION_INTERVAL
        return this
    }

    private fun restrictedRole(name: String, send: Boolean = false, consume: Boolean = false, createDurableQueue: Boolean = false,
                               deleteDurableQueue: Boolean = false, createNonDurableQueue: Boolean = false,
                               deleteNonDurableQueue: Boolean = false, manage: Boolean = false, browse: Boolean = false): Role {
        return Role(name, send, consume, createDurableQueue, deleteDurableQueue, createNonDurableQueue,
                deleteNonDurableQueue, manage, browse, createDurableQueue || createNonDurableQueue, deleteDurableQueue || deleteNonDurableQueue)
    }

    @Throws(IOException::class, KeyStoreException::class)
    private fun createArtemisSecurityManager(): ActiveMQJAASSecurityManager {
        val keyStore = config.p2pSslOptions.keyStore.get().value.internal
        val trustStore = config.p2pSslOptions.trustStore.get().value.internal
        val revocationMode = when {
            config.crlCheckArtemisServer && config.crlCheckSoftFail -> RevocationConfig.Mode.SOFT_FAIL
            config.crlCheckArtemisServer && !config.crlCheckSoftFail -> RevocationConfig.Mode.HARD_FAIL
            else -> RevocationConfig.Mode.OFF
        }

        val securityConfig = object : SecurityConfiguration() {
            // Override to make it work with our login module
            override fun getAppConfigurationEntry(name: String): Array<AppConfigurationEntry> {
                val options = mapOf(
                        BrokerJaasLoginModule.P2P_SECURITY_CONFIG to P2PJaasConfig(keyStore, trustStore, revocationMode),
                        BrokerJaasLoginModule.NODE_SECURITY_CONFIG to NodeJaasConfig(keyStore, trustStore)
                )
                return arrayOf(AppConfigurationEntry(name, REQUIRED, options))
            }
        }
        return ActiveMQJAASSecurityManager(BrokerJaasLoginModule::class.java.name, securityConfig)
    }
}
