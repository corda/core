package net.corda.node.services.rpc

import co.paralleluniverse.common.util.SameThreadExecutor
import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.RemovalListener
import com.google.common.util.concurrent.ThreadFactoryBuilder
import net.corda.client.rpc.RPCException
import net.corda.core.context.Actor
import net.corda.core.context.Actor.Id
import net.corda.core.context.InvocationContext
import net.corda.core.context.Trace
import net.corda.core.context.Trace.InvocationId
import net.corda.core.identity.CordaX500Name
import net.corda.core.internal.LifeCycle
import net.corda.core.internal.NamedCacheFactory
import net.corda.core.internal.messaging.InternalCordaRPCOps
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.RPCOps
import net.corda.core.serialization.SerializationContext
import net.corda.core.serialization.SerializationDefaults
import net.corda.core.serialization.SerializationDefaults.RPC_SERVER_CONTEXT
import net.corda.core.serialization.deserialize
import net.corda.core.utilities.Try
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.days
import net.corda.core.utilities.debug
import net.corda.core.utilities.seconds
import net.corda.core.utilities.trace
import net.corda.node.internal.security.AuthorizingSubject
import net.corda.node.internal.security.RPCSecurityManager
import net.corda.node.services.logging.pushToLoggingContext
import net.corda.nodeapi.RPCApi
import net.corda.nodeapi.RPCApi.CLASS_METHOD_DIVIDER
import net.corda.nodeapi.externalTrace
import net.corda.nodeapi.impersonatedActor
import net.corda.nodeapi.internal.DeduplicationChecker
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.nodeapi.internal.persistence.contextDatabase
import net.corda.nodeapi.internal.persistence.contextDatabaseOrNull
import net.corda.nodeapi.internal.rpc.ObservableContextInterface
import net.corda.nodeapi.internal.rpc.ObservableSubscription
import net.corda.nodeapi.internal.serialization.amqp.RpcServerObservableSerializer
import org.apache.activemq.artemis.api.core.Message
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.activemq.artemis.api.core.client.ActiveMQClient.DEFAULT_ACK_BATCH_SIZE
import org.apache.activemq.artemis.api.core.client.ClientConsumer
import org.apache.activemq.artemis.api.core.client.ClientMessage
import org.apache.activemq.artemis.api.core.client.ClientProducer
import org.apache.activemq.artemis.api.core.client.ClientSession
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory
import org.apache.activemq.artemis.api.core.client.ServerLocator
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl
import org.apache.activemq.artemis.api.core.management.CoreNotificationType
import org.apache.activemq.artemis.api.core.management.ManagementHelper
import org.slf4j.MDC
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

private typealias ObservableSubscriptionMap = Cache<InvocationId, ObservableSubscription>

data class RPCServerConfiguration(
        /** The number of threads to use for handling RPC requests */
        val rpcThreadPoolSize: Int,
        /** The interval of subscription reaping */
        val reapInterval: Duration,
        /** The cache expiry of a deduplication watermark per client. */
        val deduplicationCacheExpiry: Duration
) {
    companion object {
        val DEFAULT = RPCServerConfiguration(
                rpcThreadPoolSize = 4,
                reapInterval = 1.seconds,
                deduplicationCacheExpiry = 1.days
        )
    }
}

/**
 * The [RPCServer] implements the complement of [net.corda.client.rpc.internal.RPCClient]. When an RPC request arrives it dispatches to the
 * corresponding function in [opsList]. During serialisation of the reply (and later observations) the server subscribes to
 * each Observable it encounters and captures the client address to associate with these Observables. Later it uses this
 * address to forward observations arriving on the Observables.
 *
 * The way this is done is similar to that in [net.corda.client.rpc.internal.RPCClient], we use AMQP and add a context to stores the subscription map.
 *
 * NB: The order of elements in [opsList] matters in case of legacy RPC clients who do not specify class name of the RPC Ops they are after.
 * For Legacy RPC clients who supply method name alone, the calls are being targeted at first element in [opsList].
 */
class RPCServer(
        private val opsList: List<RPCOps>,
        private val rpcServerUsername: String,
        private val rpcServerPassword: String,
        private val serverLocator: ServerLocator,
        private val securityManager: RPCSecurityManager,
        private val nodeLegalName: CordaX500Name,
        private val rpcConfiguration: RPCServerConfiguration,
        private val cacheFactory: NamedCacheFactory
) {
    private companion object {
        private val log = contextLogger()

        private data class InvocationTarget(val method: Method, val instance: RPCOps)

        private fun ClientMessage.getOptLongProperty(propName: String): Long? {
            return if (containsProperty(propName)) {
                getLongProperty(propName)
            } else null
        }
    }

    private enum class State {
        UNSTARTED,
        STARTED,
        FINISHED
    }

    private sealed class BufferOrNone {
        data class Buffer(val container: MutableCollection<MessageAndContext>) : BufferOrNone()
        object None : BufferOrNone()
    }

    private data class MessageAndContext(val message: RPCApi.ServerToClient.RpcReply, val context: ObservableContext)

    private val lifeCycle = LifeCycle(State.UNSTARTED)
    /**
     * The method name -> InvocationTarget used for servicing the actual call.
     * NB: The key in this map can either be:
     * - FQN of the method including interface name for all the interfaces except `CordaRPCOps`;
     * - For `CordaRPCOps` interface this will be just plain method name. This is done to maintain wire compatibility with previous versions.
     */
    private val methodTable: Map<String, InvocationTarget>
    /** The observable subscription mapping. */
    private val observableMap = createObservableSubscriptionMap()
    /** A mapping from client addresses to IDs of associated Observables */
    private val clientAddressToObservables = ConcurrentHashMap<SimpleString, HashSet<InvocationId>>()
    /** The scheduled reaper handle. */
    private var reaperScheduledFuture: ScheduledFuture<*>? = null

    private var senderThread: Thread? = null
    private var rpcExecutor: ScheduledExecutorService? = null
    private var reaperExecutor: ScheduledExecutorService? = null

    private var sessionFactory: ClientSessionFactory? = null
    private var producerSession: ClientSession? = null
    private var consumerSession: ClientSession? = null
    private var rpcProducer: ClientProducer? = null
    private var rpcConsumer: ClientConsumer? = null
    private var clientBindingRemovalConsumer: ClientConsumer? = null
    private var clientBindingAdditionConsumer: ClientConsumer? = null
    private var serverControl: ActiveMQServerControl? = null

    private val responseMessageBuffer = ConcurrentHashMap<SimpleString, BufferOrNone>()
    private val sendJobQueue = LinkedBlockingQueue<RpcSendJob>()

    private val deduplicationChecker = DeduplicationChecker(rpcConfiguration.deduplicationCacheExpiry, cacheFactory = cacheFactory)
    private var deduplicationIdentity: String? = null

    constructor (
            ops: RPCOps,
            rpcServerUsername: String,
            rpcServerPassword: String,
            serverLocator: ServerLocator,
            securityManager: RPCSecurityManager,
            nodeLegalName: CordaX500Name,
            rpcConfiguration: RPCServerConfiguration,
            cacheFactory: NamedCacheFactory
    ) : this(listOf(ops), rpcServerUsername, rpcServerPassword, serverLocator, securityManager, nodeLegalName, rpcConfiguration, cacheFactory)

    init {
        val mutableMethodTable = mutableMapOf<String, InvocationTarget>()
        opsList.forEach { ops ->
            listOfApplicableInterfacesRec(ops.javaClass).toSet().forEach { interfaceClass ->
                val groupedMethods = with(interfaceClass) {
                    /*
                     * Until version 4.3, rpc calls did not include class names.
                     * Up to this version, only CordaRPCOps and InternalCordaRPCOps were supported.
                     * So, for these classes methods are registered without their class name as well to preserve backwards compatibility.
                     */
                    if(interfaceClass == CordaRPCOps::class.java || interfaceClass == InternalCordaRPCOps::class.java) {
                        methods.groupBy { it.name }
                    } else {
                        methods.groupBy { interfaceClass.name + CLASS_METHOD_DIVIDER + it.name }
                    }
                }
                groupedMethods.forEach { name, methods ->
                    if (methods.size > 1) {
                        throw IllegalArgumentException("Encountered more than one method called $name on ${interfaceClass.name}")
                    }
                }
                val interimMap = groupedMethods.mapValues { InvocationTarget(it.value.single(), ops) }
                mutableMethodTable.putAll(interimMap)
            }
        }

        // Going forward it is should be treated as immutable construct.
        methodTable = mutableMethodTable
    }

    private fun listOfApplicableInterfacesRec(clazz: Class<*>): List<Class<*>> =
        clazz.interfaces.filter { RPCOps::class.java.isAssignableFrom(it) }.flatMap {
            listOf(it) + listOfApplicableInterfacesRec(it)
        }

    private fun createObservableSubscriptionMap(): ObservableSubscriptionMap {
        val onObservableRemove = RemovalListener<InvocationId, ObservableSubscription> { key, value, cause ->
            log.debug { "Unsubscribing from Observable with id $key because of $cause" }
            value!!.subscription.unsubscribe()
        }
        return cacheFactory.buildNamed(Caffeine.newBuilder().removalListener(onObservableRemove).executor(SameThreadExecutor.getExecutor()), "RPCServer_observableSubscription")
    }

    fun start(activeMqServerControl: ActiveMQServerControl) {
        try {
            lifeCycle.requireState(State.UNSTARTED)
            log.info("Starting RPC server with configuration $rpcConfiguration")
            senderThread = startSenderThread()
            rpcExecutor = Executors.newScheduledThreadPool(
                    rpcConfiguration.rpcThreadPoolSize,
                    ThreadFactoryBuilder().setNameFormat("rpc-server-handler-pool-%d").build()
            )
            reaperExecutor = Executors.newSingleThreadScheduledExecutor(
                    ThreadFactoryBuilder().setNameFormat("rpc-server-reaper-%d").build()
            )
            reaperScheduledFuture = reaperExecutor!!.scheduleAtFixedRate(
                    this::reapSubscriptions,
                    rpcConfiguration.reapInterval.toMillis(),
                    rpcConfiguration.reapInterval.toMillis(),
                    TimeUnit.MILLISECONDS
            )

            sessionFactory = serverLocator.createSessionFactory()
            producerSession = sessionFactory!!.createSession(rpcServerUsername, rpcServerPassword, false, true, true, false, DEFAULT_ACK_BATCH_SIZE)
            createRpcProducer(producerSession!!)
            consumerSession = sessionFactory!!.createSession(rpcServerUsername, rpcServerPassword, false, true, true, false, DEFAULT_ACK_BATCH_SIZE)
            createRpcConsumer(consumerSession!!)
            createNotificationConsumers(consumerSession!!)
            serverControl = activeMqServerControl
            deduplicationIdentity = UUID.randomUUID().toString()
            lifeCycle.transition(State.UNSTARTED, State.STARTED)
            // We delay the consumer session start because Artemis starts delivering messages immediately, so we need to be
            // fully initialised.
            producerSession!!.start()
            consumerSession!!.start()
        } catch (exception: Throwable) {
            close()
            throw exception
        }
    }

    private fun createRpcProducer(producerSession: ClientSession) {
        rpcProducer = producerSession.createProducer()
    }

    private fun createRpcConsumer(consumerSession: ClientSession) {
        rpcConsumer = consumerSession.createConsumer(RPCApi.RPC_SERVER_QUEUE_NAME)
        rpcConsumer!!.setMessageHandler(this::clientArtemisMessageHandler)
    }

    private fun createNotificationConsumers(consumerSession: ClientSession) {
        clientBindingRemovalConsumer = consumerSession.createConsumer(RPCApi.RPC_CLIENT_BINDING_REMOVALS)
        clientBindingRemovalConsumer!!.setMessageHandler(this::bindingRemovalArtemisMessageHandler)
        clientBindingAdditionConsumer = consumerSession.createConsumer(RPCApi.RPC_CLIENT_BINDING_ADDITIONS)
        clientBindingAdditionConsumer!!.setMessageHandler(this::bindingAdditionArtemisMessageHandler)
    }

    private fun startSenderThread(): Thread {
        return thread(name = "rpc-server-sender", isDaemon = true) {
            var deduplicationSequenceNumber = 0L
            while (true) {
                val job = sendJobQueue.take()
                when (job) {
                    is RpcSendJob.Send -> handleSendJob(deduplicationSequenceNumber++, job)
                    RpcSendJob.Stop -> return@thread
                }
            }
        }
    }

    private fun handleSendJob(sequenceNumber: Long, job: RpcSendJob.Send) {
        try {
            val artemisMessage = producerSession!!.createMessage(false)
            if (job.database != null) {
                contextDatabase = job.database
            }
            // We must do the serialisation here as any encountered Observables may already have events, which would
            // trigger more sends. We must make sure that the root of the Observables (e.g. the RPC reply) is sent
            // before any child observations.
            job.message.writeToClientMessage(job.serializationContext, artemisMessage)
            artemisMessage.putLongProperty(RPCApi.DEDUPLICATION_SEQUENCE_NUMBER_FIELD_NAME, sequenceNumber)
            rpcProducer!!.send(job.clientAddress, artemisMessage)
            log.debug { "<- RPC <- ${job.message}" }
        } catch (throwable: Throwable) {
            log.error("Failed to send message, kicking client. Message was ${job.message}", throwable)
            serverControl!!.closeConsumerConnectionsForAddress(job.clientAddress.toString())
            invalidateClient(job.clientAddress)
            if (throwable is VirtualMachineError) throw throwable
        }
    }

    fun close(queueDrainTimeout: Duration = 5.seconds) {
        // Putting Stop message onto the queue will eventually make senderThread to stop.
        sendJobQueue.put(RpcSendJob.Stop)
        senderThread?.join(queueDrainTimeout.toMillis())
        reaperScheduledFuture?.cancel(false)
        rpcExecutor?.shutdownNow()
        reaperExecutor?.shutdownNow()
        sessionFactory?.close()
        observableMap.invalidateAll()
        reapSubscriptions()
        lifeCycle.justTransition(State.FINISHED)
    }

    private fun bindingRemovalArtemisMessageHandler(artemisMessage: ClientMessage) {
        lifeCycle.requireState(State.STARTED)
        val notificationType = artemisMessage.getStringProperty(ManagementHelper.HDR_NOTIFICATION_TYPE)
        require(notificationType == CoreNotificationType.BINDING_REMOVED.name){"Message contained notification type of $notificationType instead of expected ${CoreNotificationType.BINDING_REMOVED.name}"}
        val clientAddress = artemisMessage.getStringProperty(ManagementHelper.HDR_ROUTING_NAME)
        log.info("Detected RPC client disconnect on address $clientAddress, scheduling for reaping")
        invalidateClient(SimpleString(clientAddress))
    }

    private fun bindingAdditionArtemisMessageHandler(artemisMessage: ClientMessage) {
        lifeCycle.requireState(State.STARTED)
        val notificationType = artemisMessage.getStringProperty(ManagementHelper.HDR_NOTIFICATION_TYPE)
        require(notificationType == CoreNotificationType.BINDING_ADDED.name){"Message contained notification type of $notificationType instead of expected ${CoreNotificationType.BINDING_ADDED.name}"}
        val clientAddress = SimpleString(artemisMessage.getStringProperty(ManagementHelper.HDR_ROUTING_NAME))
        log.debug("RPC client queue created on address $clientAddress")

        val buffer = stopBuffering(clientAddress)
        buffer?.let { drainBuffer(it) }
    }

    /**
     * Disables message buffering for [clientAddress] and returns the existing buffer
     * or `null` if no requests were ever received.
     */
    private fun stopBuffering(clientAddress: SimpleString): BufferOrNone.Buffer? {
        return responseMessageBuffer.put(clientAddress, BufferOrNone.None) as? BufferOrNone.Buffer
    }

    private fun drainBuffer(buffer: BufferOrNone.Buffer) {
        buffer.container.forEach {
            it.context.sendMessage(it.message)
        }
    }

    // Note that this function operates on the *current* view of client observables. During invalidation further
    // Observables may be serialised and thus registered.
    private fun invalidateClient(clientAddress: SimpleString) {
        lifeCycle.requireState(State.STARTED)
        val observableIds = clientAddressToObservables.remove(clientAddress)
        if (observableIds != null) {
            observableMap.invalidateAll(observableIds)
        }
        responseMessageBuffer.remove(clientAddress)
    }

    private fun clientArtemisMessageHandler(artemisMessage: ClientMessage) {
        lifeCycle.requireState(State.STARTED)
        val clientToServer = RPCApi.ClientToServer.fromClientMessage(artemisMessage)
        if (log.isDebugEnabled) {
            when (clientToServer) {
                is RPCApi.ClientToServer.RpcRequest -> {
                    val username = artemisMessage.getStringProperty("_AMQ_VALIDATED_USER") ?: "(unknown)"
                    // Don't print the whole object because most of the data is useless.
                    log.debug { "-> RPC by $username -> ${clientToServer.methodName}" }
                }
                is RPCApi.ClientToServer.ObservablesClosed -> {
                    log.debug { "-> RPC observable closed -> $clientToServer"}
                }
            }
        }
        try {
            when (clientToServer) {
                is RPCApi.ClientToServer.RpcRequest -> {
                    val deduplicationSequenceNumber = artemisMessage.getOptLongProperty(RPCApi.DEDUPLICATION_SEQUENCE_NUMBER_FIELD_NAME)
                    if (deduplicationSequenceNumber != null && deduplicationChecker.checkDuplicateMessageId(
                            identity = clientToServer.clientAddress,
                            sequenceNumber = deduplicationSequenceNumber
                    )) {
                        log.info("Message duplication detected, discarding message")
                        return
                    }
                    val arguments = Try.on {
                        clientToServer.serialisedArguments.deserialize<List<Any?>>(context = RPC_SERVER_CONTEXT)
                    }
                    val context = artemisMessage.context(clientToServer.sessionId)
                    context.invocation.pushToLoggingContext()
                    val clientAddress = SimpleString(clientToServer.clientAddress.removePrefix("queue://").toString())
                    when (arguments) {
                        is Try.Success -> {
                            log.debug { "Arguments: ${arguments.value.toTypedArray().contentDeepToString()}" }
                            rpcExecutor!!.submit {
                                val result = invokeRpc(context, clientToServer.methodName, arguments.value)
                                sendReply(clientToServer.replyId, clientAddress, result)
                            }
                        }
                        is Try.Failure -> {
                            // We failed to deserialise the arguments, route back the error
                            log.warn("Inbound RPC failed", arguments.exception)
                            sendReply(clientToServer.replyId, clientAddress, arguments)
                        }
                    }
                }
                is RPCApi.ClientToServer.ObservablesClosed -> {
                    observableMap.invalidateAll(clientToServer.ids)
                }
            }
        } finally {
            artemisMessage.acknowledge()
        }
    }

    private fun invokeRpc(context: RpcAuthContext, inMethodName: String, arguments: List<Any?>): Try<Any> {
        return Try.on {
            try {
                CURRENT_RPC_CONTEXT.set(context)
                log.trace { "Calling $inMethodName" }
                val invocationTarget = methodTable[inMethodName] ?:
                        throw RPCException("Received RPC for unknown method $inMethodName - possible client/server version skew?")
                invocationTarget.method.invoke(invocationTarget.instance, *arguments.toTypedArray())
            } catch (e: InvocationTargetException) {
                throw e.cause ?: RPCException("Caught InvocationTargetException without cause")
            } catch (e: Exception) {
                log.warn("Caught exception attempting to invoke RPC $inMethodName", e)
                throw e
            } finally {
                CURRENT_RPC_CONTEXT.remove()
            }
        }
    }

    private fun sendReply(replyId: InvocationId, clientAddress: SimpleString, result: Try<Any>) {
        val reply = RPCApi.ServerToClient.RpcReply(
                id = replyId,
                result = result,
                deduplicationIdentity = deduplicationIdentity!!
        )
        val observableContext = ObservableContext(
                observableMap,
                clientAddressToObservables,
                deduplicationIdentity!!,
                clientAddress
        )

        val buffered = bufferIfQueueNotBound(clientAddress, reply, observableContext)
        if (!buffered) observableContext.sendMessage(reply)
    }

    /**
     * Buffer the message if the queue at [clientAddress] is not yet bound.
     *
     * This can happen after server restart when the client consumer session initiates failover,
     * but the client queue is not yet set up. We buffer the messages and flush the buffer only once
     * we receive a notification that the client queue bindings were added.
     */
    private fun bufferIfQueueNotBound(clientAddress: SimpleString, message: RPCApi.ServerToClient.RpcReply, context: ObservableContext): Boolean {
        val clientBuffer = responseMessageBuffer.compute(clientAddress) { _, value ->
            when (value) {
                null -> BufferOrNone.Buffer(ArrayList()).apply {
                    container.add(MessageAndContext(message, context))
                }
                is BufferOrNone.Buffer -> value.apply {
                    container.add(MessageAndContext(message, context))
                }
                is BufferOrNone.None -> value
            }
        }
        return clientBuffer is BufferOrNone.Buffer
    }

    private fun reapSubscriptions() {
        observableMap.cleanUp()
    }

    private fun ClientMessage.context(sessionId: Trace.SessionId): RpcAuthContext {
        val trace = Trace.newInstance(sessionId = sessionId)
        val externalTrace = externalTrace()
        val rpcActor = actorFrom(this)
        val impersonatedActor = impersonatedActor()
        return RpcAuthContext(InvocationContext.rpc(rpcActor.first, trace, externalTrace, impersonatedActor), rpcActor.second)
    }

    private fun actorFrom(message: ClientMessage): Pair<Actor, AuthorizingSubject> {
        val validatedUser = message.getStringProperty(Message.HDR_VALIDATED_USER) ?: throw IllegalArgumentException("Missing validated user from the Artemis message")
        val targetLegalIdentity = message.getStringProperty(RPCApi.RPC_TARGET_LEGAL_IDENTITY)?.let(CordaX500Name.Companion::parse) ?: nodeLegalName
        return Pair(Actor(Id(validatedUser), securityManager.id, targetLegalIdentity), securityManager.buildSubject(validatedUser))
    }

    /*
     * We construct an observable context on each RPC request. If subsequently a nested Observable is encountered this
     * same context is propagated by serialization context. This way all observations rooted in a single RPC will be
     * muxed correctly. Note that the context construction itself is quite cheap.
     */
    inner class ObservableContext(
            override val observableMap: ObservableSubscriptionMap,
            override val clientAddressToObservables: ConcurrentHashMap<SimpleString, HashSet<InvocationId>>,
            override val deduplicationIdentity: String,
            override val clientAddress: SimpleString
    ) : ObservableContextInterface {
        private val serializationContextWithObservableContext = RpcServerObservableSerializer.createContext(
                observableContext = this,
                serializationContext = SerializationDefaults.RPC_SERVER_CONTEXT)

        override fun sendMessage(serverToClient: RPCApi.ServerToClient) {
            sendJobQueue.put(RpcSendJob.Send(contextDatabaseOrNull, clientAddress,
                    serializationContextWithObservableContext, serverToClient))
        }
    }

    private sealed class RpcSendJob {
        data class Send(
                // TODO HACK this is because during serialisation we subscribe to observables that may use
                // DatabaseTransactionWrappingSubscriber which tries to access the current database,
                val database: CordaPersistence?,
                val clientAddress: SimpleString,
                val serializationContext: SerializationContext,
                val message: RPCApi.ServerToClient
        ) : RpcSendJob()
        object Stop : RpcSendJob()
    }
}

// TODO replace this by creating a new CordaRPCImpl for each request, passing the context, after we fix Shell and WebServer
@JvmField
internal val CURRENT_RPC_CONTEXT: ThreadLocal<RpcAuthContext> = CurrentRpcContext()

internal class CurrentRpcContext : ThreadLocal<RpcAuthContext>() {

    override fun remove() {
        super.remove()
        MDC.clear()
    }

    override fun set(context: RpcAuthContext?) {
        when {
            context != null -> {
                super.set(context)
                // this is needed here as well because the Shell sets the context without going through the RpcServer
                context.invocation.pushToLoggingContext()
            }
            else -> remove()
        }
    }
}

/**
 * Returns a context specific to the current RPC call. Note that trying to call this function outside of an RPC will
 * throw. If you'd like to use the context outside of the call (e.g. in another thread) then pass the returned reference
 * around explicitly.
 * The [InvocationContext] does not include permissions.
 */
internal fun context(): InvocationContext = rpcContext().invocation

/**
 * Returns a context specific to the current RPC call. Note that trying to call this function outside of an RPC will
 * throw. If you'd like to use the context outside of the call (e.g. in another thread) then pass the returned reference
 * around explicitly.
 * The [RpcAuthContext] includes permissions.
 */
fun rpcContext(): RpcAuthContext = CURRENT_RPC_CONTEXT.get()
