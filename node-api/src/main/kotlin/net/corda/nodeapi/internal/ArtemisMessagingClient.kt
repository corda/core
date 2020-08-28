package net.corda.nodeapi.internal

import net.corda.core.serialization.internal.nodeSerializationEnv
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.loggerFor
import net.corda.nodeapi.internal.ArtemisMessagingComponent.Companion.NODE_P2P_USER
import net.corda.nodeapi.internal.ArtemisTcpTransport.Companion.p2pConnectorTcpTransport
import net.corda.nodeapi.internal.ArtemisTcpTransport.Companion.p2pConnectorTcpTransportFromList
import net.corda.nodeapi.internal.config.MessagingServerConnectionConfiguration
import net.corda.nodeapi.internal.config.MutualSslConfiguration
import org.apache.activemq.artemis.api.core.client.*
import org.apache.activemq.artemis.api.core.client.ActiveMQClient.DEFAULT_ACK_BATCH_SIZE

interface ArtemisSessionProvider {
    fun start(): ArtemisMessagingClient.Started
    fun stop()
    val started: ArtemisMessagingClient.Started?
}

class ArtemisMessagingClient(private val config: MutualSslConfiguration,
                             private val serverAddress: NetworkHostAndPort,
                             private val maxMessageSize: Int,
                             private val autoCommitSends: Boolean = true,
                             private val autoCommitAcks: Boolean = true,
                             private val confirmationWindowSize: Int = -1,
                             private val messagingServerConnectionConfig: MessagingServerConnectionConfiguration? = null,
                             private val backupServerAddressPool: List<NetworkHostAndPort> = emptyList(),
                             private val failoverCallback: ((FailoverEventType) -> Unit)? = null,
                             private val lowMemoryMode: Boolean = false
)  : ArtemisSessionProvider {
    companion object {
        private val log = loggerFor<ArtemisMessagingClient>()
        const val CORDA_ARTEMIS_CALL_TIMEOUT_PROP_NAME = "net.corda.nodeapi.artemismessagingclient.CallTimeout"
        const val CORDA_ARTEMIS_CALL_TIMEOUT_DEFAULT = 5000L
    }

    class Started(val serverLocator: ServerLocator, val sessionFactory: ClientSessionFactory, val session: ClientSession, val producer: ClientProducer)

    override var started: Started? = null
        private set

    override fun start(): Started = synchronized(this) {
        check(started == null) { "start can't be called twice" }
        val tcpTransport = p2pConnectorTcpTransport(serverAddress, config, lowMemoryMode = lowMemoryMode)
        val backupTransports = p2pConnectorTcpTransportFromList(backupServerAddressPool, config, lowMemoryMode = lowMemoryMode)

        log.info("Connecting to message broker: $serverAddress")
        if (backupTransports.isNotEmpty()) {
            log.info("Back-up message broker addresses: $backupServerAddressPool")
        }
        // If back-up artemis addresses are configured, the locator will be created using HA mode.
        @Suppress("SpreadOperator")
        val locator = ActiveMQClient.createServerLocator(backupTransports.isNotEmpty(), *(listOf(tcpTransport) + backupTransports).toTypedArray()).apply {
            // Never time out on our loopback Artemis connections. If we switch back to using the InVM transport this
            // would be the default and the two lines below can be deleted.
            connectionTTL = 60000
            clientFailureCheckPeriod = 30000
            callFailoverTimeout = java.lang.Long.getLong(CORDA_ARTEMIS_CALL_TIMEOUT_PROP_NAME, CORDA_ARTEMIS_CALL_TIMEOUT_DEFAULT)
            callTimeout = java.lang.Long.getLong(CORDA_ARTEMIS_CALL_TIMEOUT_PROP_NAME, CORDA_ARTEMIS_CALL_TIMEOUT_DEFAULT)
            minLargeMessageSize = maxMessageSize
            isUseGlobalPools = nodeSerializationEnv != null
            confirmationWindowSize = this@ArtemisMessagingClient.confirmationWindowSize
            producerWindowSize = -1
            messagingServerConnectionConfig?.let {
                connectionLoadBalancingPolicyClassName = RoundRobinConnectionPolicy::class.java.canonicalName
                reconnectAttempts = messagingServerConnectionConfig.reconnectAttempts(isHA)
                retryInterval = messagingServerConnectionConfig.retryInterval().toMillis()
                retryIntervalMultiplier = messagingServerConnectionConfig.retryIntervalMultiplier()
                maxRetryInterval = messagingServerConnectionConfig.maxRetryInterval(isHA).toMillis()
                isFailoverOnInitialConnection = messagingServerConnectionConfig.failoverOnInitialAttempt(isHA)
                initialConnectAttempts = messagingServerConnectionConfig.initialConnectAttempts(isHA)
            }
            threadPoolMaxSize = if (lowMemoryMode) ArtemisConstants.LOW_MEMORY_MODE_THREAD_POOL_MAX_SIZE else ArtemisConstants.DEFAULT_THREAD_POOL_MAX_SIZE
            addIncomingInterceptor(ArtemisMessageSizeChecksInterceptor(maxMessageSize))
        }
        val sessionFactory = locator.createSessionFactory()

        // Handle failover events if a callback method is provided
        if (failoverCallback != null) sessionFactory.addFailoverListener(failoverCallback)

        // Login using the node username. The broker will authenticate us as its node (as opposed to another peer)
        // using our TLS certificate.
        // Note that the acknowledgement of messages is not flushed to the Artermis journal until the default buffer
        // size of 1MB is acknowledged.
        val session = sessionFactory!!.createSession(NODE_P2P_USER, NODE_P2P_USER, false, autoCommitSends, autoCommitAcks, false, DEFAULT_ACK_BATCH_SIZE)
        session.start()
        // Create a general purpose producer.
        val producer = session.createProducer()
        return Started(locator, sessionFactory, session, producer).also { started = it }
    }

    override fun stop() = synchronized(this) {
        started?.run {
            producer.close()
            // Since we are leaking the session outside of this class it may well be already closed.
            if (session.stillOpen()) {
                // Ensure any trailing messages are committed to the journal
                session.commit()
            }
            // Closing the factory closes all the sessions it produced as well.
            sessionFactory.close()
            serverLocator.close()
        }
        started = null
    }
}
