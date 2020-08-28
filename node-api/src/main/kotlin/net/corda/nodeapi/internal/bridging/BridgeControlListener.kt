@file:Suppress("TooGenericExceptionCaught") // needs to catch and handle/rethrow *all* exceptions
package net.corda.nodeapi.internal.bridging

import net.corda.core.identity.CordaX500Name
import net.corda.core.serialization.SerializationDefaults
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.contextLogger
import net.corda.nodeapi.internal.ArtemisMessagingClient
import net.corda.nodeapi.internal.ArtemisMessagingComponent.Companion.BRIDGE_CONTROL
import net.corda.nodeapi.internal.ArtemisMessagingComponent.Companion.BRIDGE_NOTIFY
import net.corda.nodeapi.internal.ArtemisMessagingComponent.Companion.P2P_PREFIX
import net.corda.nodeapi.internal.ArtemisMessagingComponent.Companion.PEERS_PREFIX
import net.corda.nodeapi.internal.ArtemisSessionProvider
import net.corda.nodeapi.internal.config.CertificateStore
import net.corda.nodeapi.internal.config.MutualSslConfiguration
import net.corda.nodeapi.internal.crypto.x509
import net.corda.nodeapi.internal.protonwrapper.netty.ProxyConfig
import net.corda.nodeapi.internal.protonwrapper.netty.RevocationConfig
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException
import org.apache.activemq.artemis.api.core.ActiveMQQueueExistsException
import org.apache.activemq.artemis.api.core.RoutingType
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.activemq.artemis.api.core.client.ClientConsumer
import org.apache.activemq.artemis.api.core.client.ClientMessage
import org.apache.activemq.artemis.api.core.client.ClientSession
import rx.Observable
import rx.subjects.PublishSubject
import java.util.*

class BridgeControlListener(private val keyStore: CertificateStore,
                            trustStore: CertificateStore,
                            useOpenSSL: Boolean,
                            proxyConfig: ProxyConfig? = null,
                            maxMessageSize: Int,
                            revocationConfig: RevocationConfig,
                            enableSNI: Boolean,
                            private val artemisMessageClientFactory: () -> ArtemisSessionProvider,
                            bridgeMetricsService: BridgeMetricsService? = null,
                            trace: Boolean = false,
                            sslHandshakeTimeout: Long? = null,
                            bridgeConnectionTTLSeconds: Int = 0) : AutoCloseable {
    private val bridgeId: String = UUID.randomUUID().toString()
    private var bridgeControlQueue = "$BRIDGE_CONTROL.$bridgeId"
    private var bridgeNotifyQueue = "$BRIDGE_NOTIFY.$bridgeId"
    private val validInboundQueues = mutableSetOf<String>()
    private val bridgeManager = if (enableSNI) {
        LoopbackBridgeManager(keyStore, trustStore, useOpenSSL, proxyConfig, maxMessageSize, revocationConfig, enableSNI,
                              artemisMessageClientFactory, bridgeMetricsService, this::validateReceiveTopic, trace, sslHandshakeTimeout,
                              bridgeConnectionTTLSeconds)
    } else {
        AMQPBridgeManager(keyStore, trustStore, useOpenSSL, proxyConfig, maxMessageSize, revocationConfig, enableSNI,
                          artemisMessageClientFactory, bridgeMetricsService, trace, sslHandshakeTimeout, bridgeConnectionTTLSeconds)
    }
    private var artemis: ArtemisSessionProvider? = null
    private var controlConsumer: ClientConsumer? = null
    private var notifyConsumer: ClientConsumer? = null

    constructor(config: MutualSslConfiguration,
                p2pAddress: NetworkHostAndPort,
                maxMessageSize: Int,
                revocationConfig: RevocationConfig,
                enableSNI: Boolean,
                proxy: ProxyConfig? = null,
                lowMemoryMode: Boolean = false) : this(config.keyStore.get(), config.trustStore.get(), config.useOpenSsl, proxy, maxMessageSize, revocationConfig, enableSNI, { ArtemisMessagingClient(config, p2pAddress, maxMessageSize, lowMemoryMode = lowMemoryMode) })

    companion object {
        private val log = contextLogger()
    }

    val active: Boolean
        get() = validInboundQueues.isNotEmpty()

    private val _activeChange = PublishSubject.create<Boolean>().toSerialized()
    val activeChange: Observable<Boolean>
        get() = _activeChange

    private val _failure = PublishSubject.create<BridgeControlListener>().toSerialized()
    val failure: Observable<BridgeControlListener>
        get() = _failure

    fun start() {
        try {
            stop()

            val queueDisambiguityId = UUID.randomUUID().toString()
            bridgeControlQueue = "$BRIDGE_CONTROL.$queueDisambiguityId"
            bridgeNotifyQueue = "$BRIDGE_NOTIFY.$queueDisambiguityId"

            bridgeManager.start()
            val artemis = artemisMessageClientFactory()
            this.artemis = artemis
            artemis.start()
            val artemisClient = artemis.started!!
            val artemisSession = artemisClient.session
            registerBridgeControlListener(artemisSession)
            registerBridgeDuplicateChecker(artemisSession)
            // Attempt to read available inboxes directly from Artemis before requesting updates from connected nodes
            validInboundQueues.addAll(artemisSession.addressQuery(SimpleString("$P2P_PREFIX#")).queueNames.map { it.toString() })
            log.info("Found inboxes: $validInboundQueues")
            if (active) {
                _activeChange.onNext(true)
            }
            val startupMessage = BridgeControl.BridgeToNodeSnapshotRequest(bridgeId).serialize(context = SerializationDefaults.P2P_CONTEXT)
                    .bytes
            val bridgeRequest = artemisSession.createMessage(false)
            bridgeRequest.writeBodyBufferBytes(startupMessage)
            artemisClient.producer.send(BRIDGE_NOTIFY, bridgeRequest)
        } catch (e: Exception) {
            log.error("Failure to start BridgeControlListener", e)
            _failure.onNext(this)
        }
    }

    private fun registerBridgeControlListener(artemisSession: ClientSession) {
        try {
            artemisSession.createTemporaryQueue(BRIDGE_CONTROL, RoutingType.MULTICAST, bridgeControlQueue)
        } catch (ex: ActiveMQQueueExistsException) {
            // Ignore if there is a queue still not cleaned up
        }

        val control = artemisSession.createConsumer(bridgeControlQueue)
        controlConsumer = control
        control.setMessageHandler { msg ->
            try {
                processControlMessage(msg)
            } catch (ex: Exception) {
                log.error("Unable to process bridge control message", ex)
                _failure.onNext(this)
            }
            msg.acknowledge()
        }
    }

    private fun registerBridgeDuplicateChecker(artemisSession: ClientSession) {
        try {
            artemisSession.createTemporaryQueue(BRIDGE_NOTIFY, RoutingType.MULTICAST, bridgeNotifyQueue)
        } catch (ex: ActiveMQQueueExistsException) {
            // Ignore if there is a queue still not cleaned up
        }
        val notify = artemisSession.createConsumer(bridgeNotifyQueue)
        notifyConsumer = notify
        notify.setMessageHandler { msg ->
            try {
                val data: ByteArray = ByteArray(msg.bodySize).apply { msg.bodyBuffer.readBytes(this) }
                val notifyMessage = data.deserialize<BridgeControl.BridgeToNodeSnapshotRequest>(context = SerializationDefaults.P2P_CONTEXT)
                if (notifyMessage.bridgeIdentity != bridgeId) {
                    log.error("Fatal Error! Two bridges have been configured simultaneously! Check the enterpriseConfiguration.externalBridge status")
                    System.exit(1)
                }
            } catch (ex: Exception) {
                log.error("Unable to process bridge notification message", ex)
                _failure.onNext(this)
            }
            msg.acknowledge()
        }
    }

    fun stop() {
        try {
            if (active) {
                _activeChange.onNext(false)
            }
            validInboundQueues.clear()
            controlConsumer?.close()
            controlConsumer = null
            notifyConsumer?.close()
            notifyConsumer = null
            artemis?.apply {
                try {
                    started?.session?.deleteQueue(bridgeControlQueue)
                } catch (e: ActiveMQNonExistentQueueException) {
                    log.warn("Queue $bridgeControlQueue does not exist and it can't be deleted")
                }
                try {
                    started?.session?.deleteQueue(bridgeNotifyQueue)
                } catch (e: ActiveMQNonExistentQueueException) {
                    log.warn("Queue $bridgeNotifyQueue does not exist and it can't be deleted")
                }
                stop()
            }
            artemis = null
            bridgeManager.stop()
        } catch (e: Exception) {
            log.error("Failure to stop BridgeControlListener", e)
        }
    }

    override fun close() = stop()

    fun validateReceiveTopic(topic: String): Boolean {
        return topic in validInboundQueues
    }

    private fun validateInboxQueueName(queueName: String): Boolean {
        return queueName.startsWith(P2P_PREFIX) && artemis!!.started!!.session.queueQuery(SimpleString(queueName)).isExists
    }

    private fun validateBridgingQueueName(queueName: String): Boolean {
        return queueName.startsWith(PEERS_PREFIX) && artemis!!.started!!.session.queueQuery(SimpleString(queueName)).isExists
    }

    private fun processControlMessage(msg: ClientMessage) {
        val data: ByteArray = ByteArray(msg.bodySize).apply { msg.bodyBuffer.readBytes(this) }
        val controlMessage = data.deserialize<BridgeControl>(context = SerializationDefaults.P2P_CONTEXT)
        log.info("Received bridge control message $controlMessage")
        when (controlMessage) {
            is BridgeControl.NodeToBridgeSnapshot -> {
                if (!isConfigured(controlMessage.nodeIdentity)) {
                    log.error("Fatal error! Bridge not configured with keystore for node with legal name ${controlMessage.nodeIdentity}.")
                    System.exit(1)
                }
                if (!controlMessage.inboxQueues.all { validateInboxQueueName(it) }) {
                    log.error("Invalid queue names in control message $controlMessage")
                    return
                }
                if (!controlMessage.sendQueues.all { validateBridgingQueueName(it.queueName) }) {
                    log.error("Invalid queue names in control message $controlMessage")
                    return
                }

                val wasActive = active
                validInboundQueues.addAll(controlMessage.inboxQueues)
                for (outQueue in controlMessage.sendQueues) {
                    bridgeManager.deployBridge(controlMessage.nodeIdentity, outQueue.queueName, outQueue.targets, outQueue.legalNames.toSet())
                }
                log.info("Added inbox: ${controlMessage.inboxQueues}. Current inboxes: $validInboundQueues.")
                if (bridgeManager is LoopbackBridgeManager) {
                    // Notify loopback bridge manager inboxes has changed.
                    bridgeManager.inboxesAdded(controlMessage.inboxQueues)
                }
                if (!wasActive && active) {
                    _activeChange.onNext(true)
                }
            }
            is BridgeControl.BridgeToNodeSnapshotRequest -> {
                log.error("Message from Bridge $controlMessage detected on wrong topic!")
            }
            is BridgeControl.Create -> {
                if (!validateBridgingQueueName((controlMessage.bridgeInfo.queueName))) {
                    log.error("Invalid queue names in control message $controlMessage")
                    return
                }
                bridgeManager.deployBridge(controlMessage.nodeIdentity, controlMessage.bridgeInfo.queueName, controlMessage.bridgeInfo.targets, controlMessage.bridgeInfo.legalNames.toSet())
            }
            is BridgeControl.Delete -> {
                if (!controlMessage.bridgeInfo.queueName.startsWith(PEERS_PREFIX)) {
                    log.error("Invalid queue names in control message $controlMessage")
                    return
                }
                bridgeManager.destroyBridge(controlMessage.bridgeInfo.queueName, controlMessage.bridgeInfo.targets)
            }
            is BridgeControl.BridgeHealthCheck -> {
                log.warn("Not currently doing anything on BridgeHealthCheck")
                return
            }
        }
    }

    private fun isConfigured(sourceX500Name: String): Boolean {
        val keyStore = keyStore.value.internal
        return keyStore.aliases().toList().any { alias ->
            val x500Name = keyStore.getCertificate(alias).x509.subjectX500Principal
            val cordaX500Name = CordaX500Name.build(x500Name)
            cordaX500Name.toString() == sourceX500Name
        }
    }
}