package net.corda.notary.experimental.raft

import com.codahale.metrics.Gauge
import com.codahale.metrics.MetricRegistry
import io.atomix.catalyst.transport.Address
import io.atomix.catalyst.transport.Transport
import io.atomix.catalyst.transport.netty.NettyTransport
import io.atomix.catalyst.transport.netty.SslProtocol
import io.atomix.copycat.client.ConnectionStrategies
import io.atomix.copycat.client.CopycatClient
import io.atomix.copycat.client.RecoveryStrategies
import io.atomix.copycat.server.CopycatServer
import io.atomix.copycat.server.cluster.Member
import io.atomix.copycat.server.storage.Storage
import io.atomix.copycat.server.storage.StorageLevel
import net.corda.core.concurrent.CordaFuture
import net.corda.core.contracts.StateRef
import net.corda.core.contracts.TimeWindow
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.NotarisationRequestSignature
import net.corda.core.identity.Party
import net.corda.core.internal.NamedCacheFactory
import net.corda.core.internal.concurrent.openFuture
import net.corda.core.internal.notary.SigningFunction
import net.corda.core.internal.notary.UniquenessProvider
import net.corda.core.serialization.SerializationDefaults
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.debug
import net.corda.node.utilities.AppendOnlyPersistentMap
import net.corda.nodeapi.internal.config.MutualSslConfiguration
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.nodeapi.internal.persistence.NODE_DATABASE_PREFIX
import net.corda.notary.experimental.raft.RaftTransactionCommitLog.Commands.CommitTransaction
import java.nio.file.Path
import java.time.Clock
import java.util.concurrent.CompletableFuture
import javax.annotation.concurrent.ThreadSafe
import javax.persistence.Column
import javax.persistence.Entity
import javax.persistence.Id
import javax.persistence.Lob
import javax.persistence.Table

/**
 * A uniqueness provider that records committed input states in a distributed collection replicated and
 * persisted in a Raft cluster, using the Copycat framework (http://atomix.io/copycat/).
 *
 * The uniqueness provider maintains both a Copycat cluster node (server) and a client through which it can submit
 * requests to the cluster. In Copycat, a client request is first sent to the server it's connected to and then redirected
 * to the cluster leader to be actioned.
 */
@ThreadSafe
class RaftUniquenessProvider(
        /** If *null* the Raft log will be stored in memory. */
        private val storagePath: Path? = null,
        private val transportConfiguration: MutualSslConfiguration,
        private val db: CordaPersistence,
        private val clock: Clock,
        private val metrics: MetricRegistry,
        private val cacheFactory: NamedCacheFactory,
        private val raftConfig: RaftConfig,
        private val signTransaction: SigningFunction
) : UniquenessProvider, SingletonSerializeAsToken() {
    companion object {
        private val log = contextLogger()
        fun createMap(cacheFactory: NamedCacheFactory): AppendOnlyPersistentMap<StateRef, Pair<Long, SecureHash>, CommittedState, String> =
                AppendOnlyPersistentMap(
                        cacheFactory = cacheFactory,
                        name = "RaftUniquenessProvider_transactions",
                        toPersistentEntityKey = { it.encoded() },
                        fromPersistentEntity = {
                            Pair(
                                    it.key.parseStateRef(),
                                    Pair(
                                            it.index,
                                            it.value.deserialize<SecureHash>(context = SerializationDefaults.STORAGE_CONTEXT)
                                    )
                            )
                        },
                        toPersistentEntity = { k: StateRef, (first, second) ->
                            CommittedState().apply {
                                key = k.encoded()
                                value = second.serialize(context = SerializationDefaults.STORAGE_CONTEXT).bytes
                                index = first
                            }

                        },
                        persistentEntityClass = CommittedState::class.java
                )

        fun StateRef.encoded() = "$txhash:$index"
        fun String.parseStateRef(): StateRef {
            val idx = lastIndexOf(':')
            require(idx != -1) {
                "Encoding error for StateRef '$this'"
            }
            return StateRef(SecureHash.create(substring(0, idx)), substring(idx + 1).toInt())
        }
    }

    @Entity
    @Table(name = "${NODE_DATABASE_PREFIX}raft_committed_states")
    class CommittedState(
            @Id
            @Column(name = "id", nullable = false)
            var key: String = "",

            @Lob
            @Column(name = "state_value", nullable = false)
            var value: ByteArray = ByteArray(0),

            @Column(name = "state_index")
            var index: Long = 0
    )

    @Suppress("MagicNumber") // database column length
    @Entity
    @Table(name = "${NODE_DATABASE_PREFIX}raft_committed_txs")
    class CommittedTransaction(
            @Id
            @Column(name = "transaction_id", nullable = false, length = 80)
            val transactionId: String
    )

    private lateinit var _clientFuture: CompletableFuture<CopycatClient>
    private lateinit var server: CopycatServer

    /**
     * Copycat clients are responsible for connecting to the cluster and submitting commands and queries that operate
     * on the cluster's replicated state machine.
     */
    private val client: CopycatClient
        get() = _clientFuture.get()

    fun start() {
        log.info("Creating Copycat server, log stored in: ${storagePath?.toAbsolutePath() ?: " memory"}")
        val stateMachineFactory = {
            RaftTransactionCommitLog(db, clock) { createMap(cacheFactory) }
        }
        val address = raftConfig.nodeAddress.let { Address(it.host, it.port) }
        val storage = buildStorage(storagePath)
        val transport = buildTransport(transportConfiguration)

        server = CopycatServer.builder(address)
                .withStateMachine(stateMachineFactory)
                .withStorage(storage)
                .withServerTransport(transport)
                .withSerializer(RaftTransactionCommitLog.serializer)
                .build()

        val serverFuture = if (raftConfig.clusterAddresses.isNotEmpty()) {
            log.info("Joining an existing Copycat cluster at ${raftConfig.clusterAddresses}")
            val cluster = raftConfig.clusterAddresses.map { Address(it.host, it.port) }
            server.join(cluster)
        } else {
            log.info("Bootstrapping a Copycat cluster at $address")
            server.bootstrap()
        }

        registerMonitoring()

        val client = CopycatClient.builder(address)
                .withTransport(transport) // TODO: use local transport for client-server communications
                .withConnectionStrategy(ConnectionStrategies.EXPONENTIAL_BACKOFF)
                .withSerializer(RaftTransactionCommitLog.serializer)
                .withRecoveryStrategy(RecoveryStrategies.RECOVER)
                .build()
        _clientFuture = serverFuture.thenCompose { client.connect(address) }
    }

    fun stop() {
        server.shutdown()
    }

    private fun buildStorage(storagePath: Path?): Storage? {
        val builder = Storage.builder()
        if (storagePath != null) {
            builder.withDirectory(storagePath.toFile()).withStorageLevel(StorageLevel.DISK)
        } else {
            builder.withStorageLevel(StorageLevel.MEMORY)
        }
        return builder.build()
    }

    private fun buildTransport(config: MutualSslConfiguration): Transport? {
        return NettyTransport.builder()
                .withSsl()
                .withSslProtocol(SslProtocol.TLSv1_2)
                .withKeyStorePath(config.keyStore.path.toString())
                .withKeyStorePassword(config.keyStore.storePassword)
                .withTrustStorePath(config.trustStore.path.toString())
                .withTrustStorePassword(config.trustStore.storePassword)
                .build()
    }

    private fun registerMonitoring() {
        metrics.register("RaftCluster.ThisServerStatus", Gauge<String> {
            server.state().name
        })
        metrics.register("RaftCluster.MembersCount", Gauge<Int> {
            server.cluster().members().size
        })
        metrics.register("RaftCluster.Members", Gauge<List<String>> {
            server.cluster().members().map { it.address().toString() }
        })

        metrics.register("RaftCluster.AvailableMembers", Gauge<List<String>> {
            server.cluster().members().filter { it.status() == Member.Status.AVAILABLE }.map { it.address().toString() }
        })

        metrics.register("RaftCluster.AvailableMembersCount", Gauge<Int> {
            server.cluster().members().filter { it.status() == Member.Status.AVAILABLE }.size
        })
    }

    override fun commit(
            states: List<StateRef>,
            txId: SecureHash,
            callerIdentity: Party,
            requestSignature: NotarisationRequestSignature,
            timeWindow: TimeWindow?,
            references: List<StateRef>
    ): CordaFuture<UniquenessProvider.Result> {
        log.debug { "Attempting to commit input states: ${states.joinToString()} for txId: $txId" }
        val commitCommand = CommitTransaction(
                states,
                txId,
                callerIdentity.name.toString(),
                requestSignature.serialize().bytes,
                timeWindow,
                references
        )
        val future = openFuture<UniquenessProvider.Result>()
        client.submit(commitCommand).thenAccept { commitError ->
            val result = if (commitError != null) {
                log.info("Error occurred while notarising $txId: $commitError")
                UniquenessProvider.Result.Failure(commitError)
            } else {
                log.info("All input states of transaction $txId have been committed")
                UniquenessProvider.Result.Success(signTransaction(txId))
            }
            future.set(result)
        }
        return future
    }
}
