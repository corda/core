package net.corda.client.rpc

import com.github.benmanes.caffeine.cache.Caffeine
import net.corda.client.rpc.internal.RPCClient
import net.corda.client.rpc.internal.ReconnectingCordaRPCOps
import net.corda.nodeapi.internal.rpc.client.AMQPClientSerializationScheme
import net.corda.client.rpc.reconnect.CouldNotStartFlowException
import net.corda.core.CordaInternal
import net.corda.core.context.Actor
import net.corda.core.context.Trace
import net.corda.core.identity.CordaX500Name
import net.corda.core.internal.PLATFORM_VERSION
import net.corda.core.internal.VisibleForTesting
import net.corda.core.internal.createInstancesOfClassesImplementing
import net.corda.core.internal.messaging.InternalCordaRPCOps
import net.corda.core.messaging.ClientRpcSslOptions
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.serialization.SerializationCustomSerializer
import net.corda.core.serialization.SerializationWhitelist
import net.corda.core.serialization.internal.effectiveSerializationEnv
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.days
import net.corda.core.utilities.minutes
import net.corda.core.utilities.seconds
import net.corda.nodeapi.internal.ArtemisTcpTransport.Companion.rpcConnectorTcpTransport
import net.corda.serialization.internal.AMQP_RPC_CLIENT_CONTEXT
import net.corda.serialization.internal.SerializationFactoryImpl
import net.corda.serialization.internal.amqp.SerializationFactoryCacheKey
import net.corda.serialization.internal.amqp.SerializerFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * This class is essentially just a wrapper for an RPCConnection<CordaRPCOps> and can be treated identically.
 *
 * @see RPCConnection
 */
class CordaRPCConnection private constructor(
        private val oneTimeConnection: RPCConnection<CordaRPCOps>?,
        private val observersPool: ExecutorService?,
        private val reconnectingCordaRPCOps: ReconnectingCordaRPCOps?
) : RPCConnection<CordaRPCOps> {
    internal constructor(connection: RPCConnection<CordaRPCOps>?) : this(connection, null, null)

    companion object {
        @CordaInternal
        internal fun createWithGracefulReconnection(
                username: String,
                password: String,
                addresses: List<NetworkHostAndPort>,
                rpcConfiguration: CordaRPCClientConfiguration,
                gracefulReconnect: GracefulReconnect,
                sslConfiguration: ClientRpcSslOptions? = null,
                classLoader: ClassLoader? = null,
                lowMemoryMode: Boolean = false
        ): CordaRPCConnection {
            val observersPool: ExecutorService = Executors.newCachedThreadPool()
            return CordaRPCConnection(null, observersPool, ReconnectingCordaRPCOps(
                    addresses,
                    username,
                    password,
                    rpcConfiguration,
                    gracefulReconnect,
                    sslConfiguration,
                    classLoader,
                    observersPool,
                    lowMemoryMode
            ))
        }
    }

    override val proxy: CordaRPCOps get() = reconnectingCordaRPCOps ?: oneTimeConnection!!.proxy

    private val actualConnection: RPCConnection<CordaRPCOps>
        get() = reconnectingCordaRPCOps?.reconnectingRPCConnection ?: oneTimeConnection!!

    override val serverProtocolVersion: Int get() = actualConnection.serverProtocolVersion

    override fun notifyServerAndClose() = doCloseLogic { actualConnection.notifyServerAndClose() }

    override fun forceClose() = doCloseLogic { actualConnection.forceClose() }

    private inline fun doCloseLogic(close: () -> Unit) {
        try {
            close.invoke()
        } finally {
            observersPool?.apply {
                shutdown()
                if (!awaitTermination(@Suppress("MagicNumber")30, TimeUnit.SECONDS)) {
                    shutdownNow()
                }
            }
        }
    }
}

/**
 * Can be used to configure the RPC client connection.
 */
open class CordaRPCClientConfiguration @JvmOverloads constructor(

        /**
         * The maximum retry interval for re-connections. The client will retry connections if the host is lost with
         * ever increasing spacing until the max is reached. The default is 3 minutes.
         */
        open val connectionMaxRetryInterval: Duration = 3.minutes,

        /**
         * The minimum protocol version required from the server. This is equivalent to the node's platform version
         * number. If this minimum version is not met, an exception will be thrown at startup. If you use features
         * introduced in a later version, you can bump this to match the platform version you need and get an early
         * check that runs before you do anything.
         *
         * If you leave it at the default then things will work but attempting to use an RPC added in a version later
         * than the server supports will throw [UnsupportedOperationException].
         *
         * The default value is whatever version of Corda this RPC library was shipped as a part of. Therefore if you
         * use the RPC library from Corda 4, it will by default only connect to a node of version 4 or above.
         */
        open val minimumServerProtocolVersion: Int = PLATFORM_VERSION,

        /**
         * If set to true the client will track RPC call sites (default is false). If an error occurs subsequently
         * during the RPC or in a returned Observable stream the stack trace of the originating RPC will be shown as
         * well. Note that constructing call stacks is a moderately expensive operation.
         */
        open val trackRpcCallSites: Boolean = java.lang.Boolean.getBoolean("net.corda.client.rpc.trackRpcCallSites"),

        /**
         * The interval of unused observable reaping. Leaked Observables (unused ones) are detected using weak references
         * and are cleaned up in batches in this interval. If set too large it will waste server side resources for this
         * duration. If set too low it wastes client side cycles. The default is to check once per second.
         */
        open val reapInterval: Duration = 1.seconds,

        /**
         * The number of threads to use for observations for executing [Observable.onNext]. This only has any effect
         * if [observableExecutor] is null (which is the default). The default is 4.
         */
        open val observationExecutorPoolSize: Int = 4,

        /**
         * This property is no longer used and has no effect.
         * @suppress
         */
        @Deprecated("This field is no longer used and has no effect.")
        open val cacheConcurrencyLevel: Int = 1,

        /**
         * The base retry interval for reconnection attempts. The default is 5 seconds.
         */
        open val connectionRetryInterval: Duration = 5.seconds,

        /**
         * The retry interval multiplier for exponential backoff. The default is 1.5
         */
        open val connectionRetryIntervalMultiplier: Double = 1.5,

        /**
         * Maximum reconnect attempts on failover or disconnection. The default is -1 which means unlimited.
         */
        open val maxReconnectAttempts: Int = unlimitedReconnectAttempts,

        /**
         * Maximum size of RPC responses, in bytes. Default is 10mb.
         */
        open val maxFileSize: Int = 10485760,
        // 10 MiB maximum allowed file size for attachments, including message headers.
        // TODO: acquire this value from Network Map when supported.

        /**
         * The cache expiry of a deduplication watermark per client. Default is 1 day.
         */
        open val deduplicationCacheExpiry: Duration = 1.days

) {

    companion object {

        private const val unlimitedReconnectAttempts = -1

        /** Provides an instance of this class with the parameters set to our recommended defaults. */
        @JvmField
        val DEFAULT: CordaRPCClientConfiguration = CordaRPCClientConfiguration()

    }

    /**
     * Create a new copy of a configuration object with zero or more parameters modified.
     *
     * @suppress
     */
    @Suppress("DEPRECATION")
    @JvmOverloads
    fun copy(
            connectionMaxRetryInterval: Duration = this.connectionMaxRetryInterval,
            minimumServerProtocolVersion: Int = this.minimumServerProtocolVersion,
            trackRpcCallSites: Boolean = this.trackRpcCallSites,
            reapInterval: Duration = this.reapInterval,
            observationExecutorPoolSize: Int = this.observationExecutorPoolSize,
            cacheConcurrencyLevel: Int = this.cacheConcurrencyLevel,
            connectionRetryInterval: Duration = this.connectionRetryInterval,
            connectionRetryIntervalMultiplier: Double = this.connectionRetryIntervalMultiplier,
            maxReconnectAttempts: Int = this.maxReconnectAttempts,
            maxFileSize: Int = this.maxFileSize,
            deduplicationCacheExpiry: Duration = this.deduplicationCacheExpiry
    ): CordaRPCClientConfiguration {
        return CordaRPCClientConfiguration(
                connectionMaxRetryInterval,
                minimumServerProtocolVersion,
                trackRpcCallSites,
                reapInterval,
                observationExecutorPoolSize,
                cacheConcurrencyLevel,
                connectionRetryInterval,
                connectionRetryIntervalMultiplier,
                maxReconnectAttempts,
                maxFileSize,
                deduplicationCacheExpiry
        )
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CordaRPCClientConfiguration
        if (connectionMaxRetryInterval != other.connectionMaxRetryInterval) return false
        if (minimumServerProtocolVersion != other.minimumServerProtocolVersion) return false
        if (trackRpcCallSites != other.trackRpcCallSites) return false
        if (reapInterval != other.reapInterval) return false
        if (observationExecutorPoolSize != other.observationExecutorPoolSize) return false
        @Suppress("DEPRECATION")
        if (cacheConcurrencyLevel != other.cacheConcurrencyLevel) return false
        if (connectionRetryInterval != other.connectionRetryInterval) return false
        if (connectionRetryIntervalMultiplier != other.connectionRetryIntervalMultiplier) return false
        if (maxReconnectAttempts != other.maxReconnectAttempts) return false
        if (maxFileSize != other.maxFileSize) return false
        if (deduplicationCacheExpiry != other.deduplicationCacheExpiry) return false

        return true
    }

    override fun hashCode(): Int {
        var result = minimumServerProtocolVersion
        result = 31 * result + connectionMaxRetryInterval.hashCode()
        result = 31 * result + trackRpcCallSites.hashCode()
        result = 31 * result + reapInterval.hashCode()
        result = 31 * result + observationExecutorPoolSize
        @Suppress("DEPRECATION")
        result = 31 * result + cacheConcurrencyLevel
        result = 31 * result + connectionRetryInterval.hashCode()
        result = 31 * result + connectionRetryIntervalMultiplier.hashCode()
        result = 31 * result + maxReconnectAttempts
        result = 31 * result + maxFileSize
        result = 31 * result + deduplicationCacheExpiry.hashCode()
        return result
    }

    @Suppress("DEPRECATION")
    override fun toString(): String {
        return "CordaRPCClientConfiguration(" +
                "connectionMaxRetryInterval=$connectionMaxRetryInterval, " +
                "minimumServerProtocolVersion=$minimumServerProtocolVersion, trackRpcCallSites=$trackRpcCallSites, " +
                "reapInterval=$reapInterval, observationExecutorPoolSize=$observationExecutorPoolSize, " +
                "cacheConcurrencyLevel=$cacheConcurrencyLevel, connectionRetryInterval=$connectionRetryInterval, " +
                "connectionRetryIntervalMultiplier=$connectionRetryIntervalMultiplier, " +
                "maxReconnectAttempts=$maxReconnectAttempts, maxFileSize=$maxFileSize, " +
                "deduplicationCacheExpiry=$deduplicationCacheExpiry)"
    }

    // Left in for backwards compatibility with version 3.1
    @Deprecated("Binary compatibility stub")
    operator fun component1() = connectionMaxRetryInterval

}

/**
 * GracefulReconnect provides the opportunity to perform certain logic when the RPC encounters a connection disconnect
 * during communication with the node.
 *
 * NOTE: The callbacks provided may be executed on a separate thread to that which called the RPC command.
 *
 * @param onDisconnect implement this callback to perform logic when the RPC disconnects on connection disconnect
 * @param onReconnect implement this callback to perform logic when the RPC has reconnected after connection disconnect
 * @param maxAttempts the maximum number of attempts per each individual RPC call. A negative number indicates infinite
 *                    number of retries.  The default value is 5.
 */
class GracefulReconnect(val onDisconnect: () -> Unit = {}, val onReconnect: () -> Unit = {}, val maxAttempts: Int = 5) {
    @Suppress("unused") // constructor for java
    @JvmOverloads
    constructor(onDisconnect: Runnable, onReconnect: Runnable, maxAttempts: Int = 5) :
        this(onDisconnect = { onDisconnect.run() }, onReconnect = { onReconnect.run() }, maxAttempts = maxAttempts)
}

/**
 * An RPC client connects to the specified server and allows you to make calls to the server that perform various
 * useful tasks. Please see the Client RPC section of docs.corda.net to learn more about how this API works. A brief
 * description is provided here.
 *
 * Calling [start] returns an [RPCConnection] containing a proxy that lets you invoke RPCs on the server. Calls on
 * it block, and if the server throws an exception then it will be rethrown on the client. Proxies are thread safe and
 * may be used to invoke multiple RPCs in parallel.
 *
 * RPC sends and receives are logged on the net.corda.rpc logger.
 *
 * The [CordaRPCOps] defines what client RPCs are available. If an RPC returns an [rx.Observable] anywhere in the object
 * graph returned then the server-side observable is transparently forwarded to the client side here.
 * *You are expected to use it*. The server will begin sending messages immediately that will be buffered on the
 * client, you are expected to drain by subscribing to the returned observer. You can opt-out of this by simply
 * calling the [net.corda.client.rpc.notUsed] method on it.
 *
 * You don't have to explicitly close the observable if you actually subscribe to it: it will close itself and free up
 * the server-side resources either when the client or JVM itself is shutdown, or when there are no more subscribers to
 * it. Once all the subscribers to a returned observable are unsubscribed or the observable completes successfully or
 * with an error, the observable is closed and you can't then re-subscribe again: you'll have to re-request a fresh
 * observable with another RPC.
 *
 * In case of loss of connection to the server, the client will try to reconnect using the settings provided via
 * [CordaRPCClientConfiguration]. If the client was created using a list of hosts via [haAddressPool], automatic failover will occur
 * (the servers have to be started in HA mode). While attempting failover, current and future RPC calls will throw
 * [RPCException] and previously returned observables will call onError().
 *
 * If you want to enable a more graceful form of reconnection, you can make use of the gracefulReconnect argument of the [start] method.
 * If this is set to true, then:
 * - The client will automatically reconnect, when the connection is broken regardless of whether you provided a single or
 *   multiple addresses.
 * - Simple RPC calls that return data (e.g. [CordaRPCOps.networkParameters]) will **block** and return after the connection has been
 *   re-established and the node is up.
 * - RPC calls that return [rx.Observable]s (e.g. [CordaRPCOps.vaultTrack]) will automatically reconnect and keep sending events for
 *   the subscribed [rx.Observable]s.
 *   Note: In this approach, some events might be lost during a re-connection and not sent in the subscribed [rx.Observable]s.
 * - RPC calls that invoke flows (e.g. [CordaRPCOps.startFlowDynamic]) will fail during a disconnection throwing
 *   a [CouldNotStartFlowException].
 *
 * @param hostAndPort The network address to connect to.
 * @param configuration An optional configuration used to tweak client behaviour.
 * @param sslConfiguration An optional [ClientRpcSslOptions] used to enable secure communication with the server.
 * @param haAddressPool A list of [NetworkHostAndPort] representing the addresses of servers in HA mode.
 *  The client will attempt to connect to a live server by trying each address in the list. If the servers are not in
 *  HA mode, the client will round-robin from the beginning of the list and try all servers.
 * @param classLoader a classloader, which will be used (if provided) to discover available [SerializationCustomSerializer]s
 *  and [SerializationWhitelist]s. If no classloader is provided, the classloader of the current class will be used by default
 *  for the aforementioned discovery process.
 * @param customSerializers a set of [SerializationCustomSerializer]s to be used. If this parameter is specified, then no classpath scanning
 *  will be performed for custom serializers, the provided ones will be used instead. This parameter serves as a more user-friendly option
 *  to specify your serializers and disable the classpath scanning (e.g. for performance reasons).
 */
class CordaRPCClient private constructor(
        private val hostAndPort: NetworkHostAndPort?,
        private val haAddressPool: List<NetworkHostAndPort>,
        private val configuration: CordaRPCClientConfiguration = CordaRPCClientConfiguration.DEFAULT,
        private val sslConfiguration: ClientRpcSslOptions? = null,
        private val classLoader: ClassLoader? = null,
        private val customSerializers: Set<SerializationCustomSerializer<*, *>>? = null,
        private val lowMemoryMode: Boolean
) {

    @JvmOverloads
    constructor(
            hostAndPort: NetworkHostAndPort,
            configuration: CordaRPCClientConfiguration = CordaRPCClientConfiguration.DEFAULT
    ) : this(
            hostAndPort = hostAndPort,
            haAddressPool = emptyList(),
            configuration = configuration,
            lowMemoryMode = false
    )

    constructor(
            hostAndPort: NetworkHostAndPort,
            configuration: CordaRPCClientConfiguration = CordaRPCClientConfiguration.DEFAULT,
            classLoader: ClassLoader
    ): this(
            hostAndPort,
            configuration,
            null,
            classLoader = classLoader
    )

    constructor(
            hostAndPort: NetworkHostAndPort,
            sslConfiguration: ClientRpcSslOptions? = null,
            classLoader: ClassLoader? = null
    ) : this(
            hostAndPort = hostAndPort,
            haAddressPool = emptyList(),
            sslConfiguration = sslConfiguration,
            classLoader = classLoader,
            lowMemoryMode = false
    )

    constructor(
            hostAndPort: NetworkHostAndPort,
            sslConfiguration: ClientRpcSslOptions? = null,
            classLoader: ClassLoader? = null,
            lowMemoryMode: Boolean
    ) : this(
            hostAndPort = hostAndPort,
            haAddressPool = emptyList(),
            sslConfiguration = sslConfiguration,
            classLoader = classLoader,
            lowMemoryMode = lowMemoryMode
    )

    @JvmOverloads
    constructor(
            hostAndPort: NetworkHostAndPort,
            configuration: CordaRPCClientConfiguration,
            sslConfiguration: ClientRpcSslOptions?,
            classLoader: ClassLoader? = null
    ) : this(
            hostAndPort = hostAndPort,
            haAddressPool = emptyList(),
            configuration = configuration,
            sslConfiguration = sslConfiguration,
            classLoader = classLoader,
            lowMemoryMode = false
    )

    @JvmOverloads
    constructor(
            hostAndPort: NetworkHostAndPort,
            configuration: CordaRPCClientConfiguration,
            sslConfiguration: ClientRpcSslOptions?,
            classLoader: ClassLoader? = null,
            lowMemoryMode: Boolean
    ) : this(
            hostAndPort = hostAndPort,
            haAddressPool = emptyList(),
            configuration = configuration,
            sslConfiguration = sslConfiguration,
            classLoader = classLoader,
            lowMemoryMode = lowMemoryMode
    )

    @JvmOverloads
    constructor(
            haAddressPool: List<NetworkHostAndPort>,
            configuration: CordaRPCClientConfiguration = CordaRPCClientConfiguration.DEFAULT,
            sslConfiguration: ClientRpcSslOptions? = null,
            classLoader: ClassLoader? = null
    ) : this(
            hostAndPort = null,
            haAddressPool = haAddressPool,
            configuration = configuration,
            sslConfiguration = sslConfiguration,
            classLoader = classLoader,
            lowMemoryMode = false
    )

    @JvmOverloads
    constructor(
            hostAndPort: NetworkHostAndPort,
            configuration: CordaRPCClientConfiguration = CordaRPCClientConfiguration.DEFAULT,
            sslConfiguration: ClientRpcSslOptions? = null,
            classLoader: ClassLoader? = null,
            customSerializers: Set<SerializationCustomSerializer<*, *>>?
    ) : this(
            hostAndPort = hostAndPort,
            haAddressPool = emptyList(),
            configuration = configuration,
            sslConfiguration = sslConfiguration,
            classLoader = classLoader,
            customSerializers = customSerializers,
            lowMemoryMode = false
    )

    @JvmOverloads
    constructor(
            haAddressPool: List<NetworkHostAndPort>,
            configuration: CordaRPCClientConfiguration = CordaRPCClientConfiguration.DEFAULT,
            sslConfiguration: ClientRpcSslOptions? = null,
            classLoader: ClassLoader? = null,
            customSerializers: Set<SerializationCustomSerializer<*, *>>?
    ) : this(
            hostAndPort = null,
            haAddressPool = haAddressPool,
            configuration = configuration,
            sslConfiguration = sslConfiguration,
            classLoader = classLoader,
            customSerializers = customSerializers,
            lowMemoryMode = false
    )

    // Here to keep the keep ABI compatibility happy
    companion object {}

    @CordaInternal
    @VisibleForTesting
    fun getRegisteredCustomSerializers(): List<SerializationCustomSerializer<*, *>> {
        return (effectiveSerializationEnv.serializationFactory as SerializationFactoryImpl).getRegisteredSchemes()
                .filterIsInstance<AMQPClientSerializationScheme>()
                .flatMap { it.getRegisteredCustomSerializers() }
    }

    init {
        try {
            effectiveSerializationEnv
        } catch (e: IllegalStateException) {
            try {
                val cache = Caffeine.newBuilder().maximumSize(128)
                        .build<SerializationFactoryCacheKey, SerializerFactory>().asMap()

                // If the client has explicitly provided a classloader use this one to scan for custom serializers,
                // otherwise use the current one.
                val serializationClassLoader = this.classLoader ?: this.javaClass.classLoader
                // If the client has explicitly provided a set of custom serializers, avoid performing any scanning and use these instead.
                val discoveredCustomSerializers = customSerializers ?: createInstancesOfClassesImplementing(
                        serializationClassLoader,
                        SerializationCustomSerializer::class.java,
                        lowMemoryMode = lowMemoryMode
                )

                val serializationWhitelists = ServiceLoader.load(
                        SerializationWhitelist::class.java,
                        serializationClassLoader
                ).toSet()

                AMQPClientSerializationScheme.initialiseSerialization(
                        serializationClassLoader,
                        discoveredCustomSerializers,
                        serializationWhitelists,
                        cache
                )
            } catch (e: IllegalStateException) {
                // Race e.g. two of these constructed in parallel, ignore.
            }
        }
    }

    private fun getRpcClient(): RPCClient<InternalCordaRPCOps> {
        return when {
        // Client->RPC broker
            haAddressPool.isEmpty() -> RPCClient(
                    rpcConnectorTcpTransport(hostAndPort!!, config = sslConfiguration, lowMemoryMode = lowMemoryMode),
                    configuration,
                    if (classLoader != null) AMQP_RPC_CLIENT_CONTEXT.withClassLoader(classLoader) else AMQP_RPC_CLIENT_CONTEXT,
                    lowMemoryMode = lowMemoryMode)
            else -> {
                RPCClient(haAddressPool,
                        sslConfiguration,
                        configuration,
                        if (classLoader != null) AMQP_RPC_CLIENT_CONTEXT.withClassLoader(classLoader) else AMQP_RPC_CLIENT_CONTEXT,
                        lowMemoryMode)
            }
        }
    }

    /**
     * Logs in to the target server and returns an active connection. The returned connection is a [java.io.Closeable]
     * and can be used with a try-with-resources statement. If you don't use that, you should use the
     * [RPCConnection.notifyServerAndClose] or [RPCConnection.forceClose] methods to dispose of the connection object
     * when done.
     *
     * @param username The username to authenticate with.
     * @param password The password to authenticate with.
     * @param gracefulReconnect a [GracefulReconnect] class containing callback logic when the RPC is dis/reconnected unexpectedly
     * @throws RPCException if the server version is too low or if the server isn't reachable within a reasonable timeout.
     */
    @JvmOverloads
    fun start(
            username: String,
            password: String,
            gracefulReconnect: GracefulReconnect? = null
    ): CordaRPCConnection {
        return start(username, password, null, null, gracefulReconnect)
    }

    /**
     * Logs in to the target server and returns an active connection. The returned connection is a [java.io.Closeable]
     * and can be used with a try-with-resources statement. If you don't use that, you should use the
     * [RPCConnection.notifyServerAndClose] or [RPCConnection.forceClose] methods to dispose of the connection object
     * when done.
     *
     * @param username The username to authenticate with.
     * @param password The password to authenticate with.
     * @param targetLegalIdentity in case of multi-identity RPC endpoint specific legal identity to which the calls must be addressed.
     * @param gracefulReconnect a [GracefulReconnect] class containing callback logic when the RPC is dis/reconnected unexpectedly
     * @throws RPCException if the server version is too low or if the server isn't reachable within a reasonable timeout.
     */
    @JvmOverloads
    fun start(
            username: String,
            password: String,
            targetLegalIdentity: CordaX500Name,
            gracefulReconnect: GracefulReconnect? = null
    ): CordaRPCConnection {
        return start(username, password, null, null, targetLegalIdentity, gracefulReconnect)
    }

    /**
     * Logs in to the target server and returns an active connection. The returned connection is a [java.io.Closeable]
     * and can be used with a try-with-resources statement. If you don't use that, you should use the
     * [RPCConnection.notifyServerAndClose] or [RPCConnection.forceClose] methods to dispose of the connection object
     * when done.
     *
     * @param username The username to authenticate with.
     * @param password The password to authenticate with.
     * @param externalTrace external [Trace] for correlation.
     * @param impersonatedActor the actor on behalf of which all the invocations will be made.
     * @param gracefulReconnect a [GracefulReconnect] class containing callback logic when the RPC is dis/reconnected unexpectedly
     * @throws RPCException if the server version is too low or if the server isn't reachable within a reasonable timeout.
     */
    @JvmOverloads
    fun start(
            username: String,
            password: String,
            externalTrace: Trace?,
            impersonatedActor: Actor?,
            gracefulReconnect: GracefulReconnect? = null
    ): CordaRPCConnection {
        return start(username, password, externalTrace, impersonatedActor, null, gracefulReconnect)
    }

    /**
     * Logs in to the target server and returns an active connection. The returned connection is a [java.io.Closeable]
     * and can be used with a try-with-resources statement. If you don't use that, you should use the
     * [RPCConnection.notifyServerAndClose] or [RPCConnection.forceClose] methods to dispose of the connection object
     * when done.
     *
     * @param username The username to authenticate with.
     * @param password The password to authenticate with.
     * @param externalTrace external [Trace] for correlation.
     * @param impersonatedActor the actor on behalf of which all the invocations will be made.
     * @param targetLegalIdentity in case of multi-identity RPC endpoint specific legal identity to which the calls must be addressed.
     * @param gracefulReconnect a [GracefulReconnect] class containing callback logic when the RPC is dis/reconnected unexpectedly.
     *  Note that when using graceful reconnect the values for [CordaRPCClientConfiguration.connectionMaxRetryInterval] and
     * [CordaRPCClientConfiguration.maxReconnectAttempts] will be overridden in order to mangage the reconnects.
     * @throws RPCException if the server version is too low or if the server isn't reachable within a reasonable timeout.
     */
    @JvmOverloads
    fun start(
            username: String,
            password: String,
            externalTrace: Trace?,
            impersonatedActor: Actor?,
            targetLegalIdentity: CordaX500Name?,
            gracefulReconnect: GracefulReconnect? = null
    ): CordaRPCConnection {
        val addresses = if (haAddressPool.isEmpty()) {
            listOf(hostAndPort!!)
        } else {
            haAddressPool
        }

        return if (gracefulReconnect != null) {
            CordaRPCConnection.createWithGracefulReconnection(
                    username,
                    password,
                    addresses,
                    configuration,
                    gracefulReconnect,
                    sslConfiguration,
                    classLoader,
                    lowMemoryMode
            )
        } else {
            CordaRPCConnection(getRpcClient().start(
                    InternalCordaRPCOps::class.java,
                    username,
                    password,
                    externalTrace,
                    impersonatedActor,
                    targetLegalIdentity
            ))
        }
    }

    /**
     * A helper for Kotlin users that simply closes the connection after the block has executed. Be careful not to
     * over-use this, as setting up and closing connections takes time.
     */
    inline fun <A> use(username: String, password: String, block: (CordaRPCConnection) -> A): A {
        return start(username, password).use(block)
    }
}