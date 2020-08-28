package net.corda.client.rpc.internal

import net.corda.client.rpc.ConnectionFailureException
import net.corda.client.rpc.CordaRPCClient
import net.corda.client.rpc.CordaRPCClientConfiguration
import net.corda.client.rpc.CordaRPCConnection
import net.corda.client.rpc.GracefulReconnect
import net.corda.client.rpc.MaxRpcRetryException
import net.corda.client.rpc.PermissionException
import net.corda.client.rpc.RPCConnection
import net.corda.client.rpc.RPCException
import net.corda.client.rpc.internal.ReconnectingCordaRPCOps.ReconnectingRPCConnection.CurrentState.CLOSED
import net.corda.client.rpc.internal.ReconnectingCordaRPCOps.ReconnectingRPCConnection.CurrentState.CONNECTED
import net.corda.client.rpc.internal.ReconnectingCordaRPCOps.ReconnectingRPCConnection.CurrentState.CONNECTING
import net.corda.client.rpc.internal.ReconnectingCordaRPCOps.ReconnectingRPCConnection.CurrentState.DIED
import net.corda.client.rpc.internal.ReconnectingCordaRPCOps.ReconnectingRPCConnection.CurrentState.UNCONNECTED
import net.corda.client.rpc.reconnect.CouldNotStartFlowException
import net.corda.core.flows.StateMachineRunId
import net.corda.core.internal.messaging.InternalCordaRPCOps
import net.corda.core.internal.min
import net.corda.core.internal.times
import net.corda.core.internal.uncheckedCast
import net.corda.core.messaging.ClientRpcSslOptions
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.DataFeed
import net.corda.core.messaging.FlowHandle
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.debug
import net.corda.core.utilities.seconds
import net.corda.nodeapi.exceptions.RejectedCommandException
import org.apache.activemq.artemis.api.core.ActiveMQConnectionTimedOutException
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException
import org.apache.activemq.artemis.api.core.ActiveMQUnBlockedException
import java.io.NotSerializableException
import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Wrapper over [CordaRPCOps] that handles exceptions when the node or the connection to the node fail.
 *
 * All operations are retried on failure, except flow start operations that die before receiving a valid [FlowHandle], in which case a
 * [CouldNotStartFlowException] is thrown.
 *
 * When calling methods that return a [DataFeed] like [CordaRPCOps.vaultTrackBy], the returned [DataFeed.updates] object will no longer
 * be a usable [rx.Observable] but an instance of [ReconnectingObservable].
 * The caller has to explicitly cast to [ReconnectingObservable] and call [ReconnectingObservable.subscribe]. If used as an [rx.Observable]
 * it will just fail.
 * The returned [DataFeed.snapshot] is the snapshot as it was when the feed was first retrieved.
 *
 * Note: There is no guarantee that observations will not be lost.
 *
 * *This class is not a stable API. Any project that wants to use it, must copy and paste it.*
 */
// TODO The executor service is not needed. All we need is a single thread that deals with reconnecting and onto which
//  ReconnectingObservables and other things can attach themselves as listeners for reconnect events.
class ReconnectingCordaRPCOps private constructor(
        val reconnectingRPCConnection: ReconnectingRPCConnection
) : InternalCordaRPCOps by proxy(reconnectingRPCConnection) {
    constructor(
            nodeHostAndPorts: List<NetworkHostAndPort>,
            username: String,
            password: String,
            rpcConfiguration: CordaRPCClientConfiguration,
            gracefulReconnect: GracefulReconnect = GracefulReconnect(),
            sslConfiguration: ClientRpcSslOptions? = null,
            classLoader: ClassLoader? = null,
            observersPool: ExecutorService,
            lowMemoryMode: Boolean = false
    ) : this(ReconnectingRPCConnection(
            nodeHostAndPorts,
            username,
            password,
            rpcConfiguration,
            sslConfiguration,
            classLoader,
            gracefulReconnect,
            observersPool,
            lowMemoryMode))
    private companion object {
        private val log = contextLogger()
        private fun proxy(reconnectingRPCConnection: ReconnectingRPCConnection): InternalCordaRPCOps {
            return Proxy.newProxyInstance(
                    this::class.java.classLoader,
                    arrayOf(InternalCordaRPCOps::class.java),
                    ErrorInterceptingHandler(reconnectingRPCConnection)) as InternalCordaRPCOps
        }
    }
    private val retryFlowsPool = Executors.newScheduledThreadPool(1)
    /**
     * This function runs a flow and retries until it completes successfully.
     *
     * [runFlow] is a function that starts a flow.
     * [hasFlowStarted] is a function that checks if the flow has actually completed by checking some side-effect, for example the vault.
     * [onFlowConfirmed] Callback when the flow is confirmed.
     * [timeout] Indicative timeout to wait until the flow would create the side-effect. Should be increased if the flow is slow. Note that
     * this timeout is calculated after the rpc client has reconnected to the node.
     *
     * Note that this method does not guarantee 100% that the flow will not be started twice.
     */
    fun runFlowWithLogicalRetry(
            runFlow: (CordaRPCOps) -> StateMachineRunId,
            hasFlowStarted: (CordaRPCOps) -> Boolean,
            onFlowConfirmed: () -> Unit = {},
            timeout: Duration = 4.seconds
    ) {
        try {
            runFlow(this)
            onFlowConfirmed()
        } catch (e: CouldNotStartFlowException) {
            log.error("Couldn't start flow: ${e.message}")
            retryFlowsPool.schedule(
                    {
                        if (!hasFlowStarted(this)) {
                            runFlowWithLogicalRetry(runFlow, hasFlowStarted, onFlowConfirmed, timeout)
                        } else {
                            onFlowConfirmed()
                        }
                    },
                    timeout.seconds, TimeUnit.SECONDS
            )
        }
    }

    /**
     * Helper class useful for reconnecting to a Node.
     */
    data class ReconnectingRPCConnection(
            val nodeHostAndPorts: List<NetworkHostAndPort>,
            val username: String,
            val password: String,
            val rpcConfiguration: CordaRPCClientConfiguration,
            val sslConfiguration: ClientRpcSslOptions? = null,
            val classLoader: ClassLoader?,
            val gracefulReconnect: GracefulReconnect = GracefulReconnect(),
            val observersPool: ExecutorService,
            val lowMemoryMode: Boolean = false
    ) : RPCConnection<CordaRPCOps> {
        private var currentRPCConnection: CordaRPCConnection? = null
        enum class CurrentState {
            UNCONNECTED, CONNECTED, CONNECTING, CLOSED, DIED
        }

        @Volatile
        private var currentState = UNCONNECTED

        init {
            current
        }
        private val current: CordaRPCConnection
            @Synchronized get() = when (currentState) {
                // The first attempt to establish a connection will try every address only once.
                UNCONNECTED ->
                    connect(nodeHostAndPorts.size) ?: throw IllegalArgumentException("The ReconnectingRPCConnection has been closed.")
                CONNECTED ->
                    currentRPCConnection!!
                CLOSED ->
                    throw IllegalArgumentException("The ReconnectingRPCConnection has been closed.")
                CONNECTING, DIED ->
                    throw IllegalArgumentException("Illegal state: $currentState ")
            }

        @Synchronized
        private fun doReconnect(e: Throwable, previousConnection: CordaRPCConnection?) {
            if (isClosed()) {
                // We don't want to reconnect if we purposely closed
                return
            }
            if (previousConnection != currentRPCConnection) {
                // We've already done this, skip
                return
            }
            // First one to get here gets to do all the reconnect logic, including calling onDisconnect and onReconnect. This makes sure
            // that they're only called once per reconnect.
            currentState = DIED
            gracefulReconnect.onDisconnect.invoke()
            //TODO - handle error cases
            log.warn("Reconnecting to ${this.nodeHostAndPorts} due to error: ${e.message}")
            log.debug("", e)
            connect(rpcConfiguration.maxReconnectAttempts)
            previousConnection?.forceClose()
            gracefulReconnect.onReconnect.invoke()
        }
        /**
         * Called on external error.
         * Will block until the connection is established again.
         */
        fun reconnectOnError(e: Throwable) {
            val previousConnection = currentRPCConnection
            doReconnect(e, previousConnection)
        }
        private fun connect(maxConnectAttempts: Int): CordaRPCConnection? {
            currentState = CONNECTING
            synchronized(this) {
                currentRPCConnection = establishConnectionWithRetry(
                        rpcConfiguration.connectionRetryInterval,
                        retries = maxConnectAttempts
                )
                // It's possible we could get closed while waiting for the connection to establish.
                if (!isClosed()) {
                    currentState = CONNECTED
                }
            }
            return currentRPCConnection
        }

        /**
         * Establishes a connection by automatically retrying if the attempt to establish a connection fails.
         *
         * @param retryInterval the interval between retries.
         * @param roundRobinIndex index of the address that will be used for the connection.
         * @param retries the number of retries remaining. A negative value implies infinite retries.
         */
        private tailrec fun establishConnectionWithRetry(
                retryInterval: Duration,
                roundRobinIndex: Int = 0,
                retries: Int = -1
        ): CordaRPCConnection? {
            if (isClosed()) {
                // We've decided to exit for some reason (maybe the client is being shutdown)
                return null
            }
            val attemptedAddress = nodeHostAndPorts[roundRobinIndex]
            log.info("Connecting to: $attemptedAddress")
            try {
                return CordaRPCClient(
                        attemptedAddress,
                        rpcConfiguration.copy(connectionMaxRetryInterval = retryInterval, maxReconnectAttempts = 1),
                        sslConfiguration,
                        classLoader,
                        lowMemoryMode
                ).start(username, password).also {
                    // Check connection is truly operational before returning it.
                    require(it.proxy.nodeInfo().legalIdentitiesAndCerts.isNotEmpty()) {
                        "Could not establish connection to $attemptedAddress."
                    }
                    log.debug { "Connection successfully established with: $attemptedAddress" }
                }
            } catch (ex: Exception) {
                when (ex) {
                    is ActiveMQSecurityException -> {
                        log.error("Failed to login to node.", ex)
                        throw ex
                    }
                    is RPCException, is ActiveMQConnectionTimedOutException, is ActiveMQUnBlockedException -> {
                        // Deliberately not logging full stack trace as it will be full of internal stacktraces.
                        log.debug { "Exception upon establishing connection: ${ex.message}" }
                    }
                    is PermissionException -> {
                        // Deliberately not logging full stack trace as it will be full of internal stacktraces.
                        log.debug { "Permission Exception establishing connection: ${ex.message}" }
                    }
                    else -> {
                        log.warn("Unknown exception [${ex.javaClass.name}] upon establishing connection.", ex)
                    }
                }
            }
            val remainingRetries = if (retries < 0) retries else (retries - 1)
            if (remainingRetries == 0) {
                throw RPCException("Cannot connect to server(s). Tried with all available servers.")
            }
            // Could not connect this time round - pause before giving another try.
            Thread.sleep(retryInterval.toMillis())
            val nextRoundRobinIndex = (roundRobinIndex + 1) % nodeHostAndPorts.size
            val nextInterval = min(
                    rpcConfiguration.connectionMaxRetryInterval,
                    retryInterval * rpcConfiguration.connectionRetryIntervalMultiplier
            )
            log.info("Could not establish connection.  Next retry interval $nextInterval")
            return establishConnectionWithRetry(nextInterval, nextRoundRobinIndex, remainingRetries)
        }

        override val proxy: CordaRPCOps
            get() = current.proxy
        override val serverProtocolVersion
            get() = current.serverProtocolVersion
        override fun notifyServerAndClose() {
            currentState = CLOSED
            synchronized(this) {
                currentRPCConnection?.notifyServerAndClose()
            }
        }
        override fun forceClose() {
            currentState = CLOSED
            synchronized(this) {
                currentRPCConnection?.forceClose()
            }
        }
        fun isClosed(): Boolean = currentState == CLOSED
    }
    private class ErrorInterceptingHandler(val reconnectingRPCConnection: ReconnectingRPCConnection) : InvocationHandler {
        private fun Method.isStartFlow() = name.startsWith("startFlow") || name.startsWith("startTrackedFlow")

        private fun checkIfIsStartFlow(method: Method, e: InvocationTargetException) {
            if (method.isStartFlow()) {
                // Don't retry flows
                throw CouldNotStartFlowException(e.targetException)
            }
        }

        /**
         * This method retries the invoked operation in a loop by re-establishing the connection when there is a problem
         * and checking if the [maxNumberOfAttempts] has been exhausted.
         *
         * A negative number for [maxNumberOfAttempts] means an unlimited number of retries will be performed.
         */
        @Suppress("ThrowsCount", "ComplexMethod")
        private fun doInvoke(method: Method, args: Array<out Any>?, maxNumberOfAttempts: Int): Any? {
            checkIfClosed()
            var remainingAttempts = maxNumberOfAttempts
            var lastException: Throwable? = null
            while (remainingAttempts != 0) {
                try {
                    log.debug { "Invoking RPC $method..." }
                    return method.invoke(reconnectingRPCConnection.proxy, *(args ?: emptyArray())).also {
                        log.debug { "RPC $method invoked successfully." }
                    }
                } catch (e: InvocationTargetException) {
                    when (e.targetException) {
                        is RejectedCommandException -> {
                            log.warn("Node is being shutdown. Operation ${method.name} rejected. Retrying when node is up...", e)
                            reconnectingRPCConnection.reconnectOnError(e)
                        }
                        is ConnectionFailureException -> {
                            log.warn("Failed to perform operation ${method.name}. Connection dropped. Retrying....", e)
                            reconnectingRPCConnection.reconnectOnError(e)
                            checkIfIsStartFlow(method, e)
                        }
                        is RPCException -> {
                            rethrowIfUnrecoverable(e.targetException as RPCException)

                            log.warn("Failed to perform operation ${method.name}. RPCException. Retrying....", e)
                            reconnectingRPCConnection.reconnectOnError(e)
                            Thread.sleep(1000) // TODO - explain why this sleep is necessary
                            checkIfIsStartFlow(method, e)
                        }
                        is PermissionException -> {
                            throw RPCException("User does not have permission to perform operation ${method.name}.", e)
                        }
                        else -> {
                            log.warn("Failed to perform operation ${method.name}.", e)
                            throw e.targetException
                        }
                    }
                    lastException = e.targetException
                    remainingAttempts--
                }
            }

            throw MaxRpcRetryException(maxNumberOfAttempts, method, lastException)
        }

        private fun checkIfClosed() {
            if (reconnectingRPCConnection.isClosed()) {
                throw RPCException("Cannot execute RPC command after client has shut down.")
            }
        }

        private fun rethrowIfUnrecoverable(e: RPCException) {
            if (e.cause is NotSerializableException) { // Do not try to reconnect when we can't serialize
                throw e
            }
        }

        override fun invoke(proxy: Any, method: Method, args: Array<out Any>?): Any? {
            return when (method.returnType) {
                DataFeed::class.java -> {
                    // Intercept the data feed methods and return a ReconnectingObservable instance
                    val initialFeed: DataFeed<Any, Any?> = uncheckedCast(doInvoke(method, args,
                            reconnectingRPCConnection.gracefulReconnect.maxAttempts))
                    val observable = ReconnectingObservable(reconnectingRPCConnection, initialFeed) {
                        // This handles reconnecting and creates new feeds.
                        uncheckedCast(this.invoke(reconnectingRPCConnection.proxy, method, args))
                    }
                    initialFeed.copy(updates = observable)
                }
                // TODO - add handlers for Observable return types.
                else -> doInvoke(method, args, reconnectingRPCConnection.gracefulReconnect.maxAttempts)
            }
        }
    }

    fun close() {
        retryFlowsPool.shutdown()
        reconnectingRPCConnection.forceClose()
    }
}
