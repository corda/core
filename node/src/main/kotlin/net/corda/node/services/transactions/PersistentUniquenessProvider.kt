package net.corda.node.services.transactions

import com.codahale.metrics.SlidingWindowReservoir
import net.corda.core.concurrent.CordaFuture
import net.corda.core.contracts.StateRef
import net.corda.core.contracts.TimeWindow
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.sha256
import net.corda.core.flows.NotarisationRequestSignature
import net.corda.core.flows.NotaryError
import net.corda.core.flows.StateConsumptionDetails
import net.corda.core.identity.Party
import net.corda.core.internal.NamedCacheFactory
import net.corda.core.internal.concurrent.OpenFuture
import net.corda.core.internal.concurrent.openFuture
import net.corda.core.internal.elapsedTime
import net.corda.core.internal.notary.NotaryInternalException
import net.corda.core.internal.notary.NotaryServiceFlow
import net.corda.core.internal.notary.SigningFunction
import net.corda.core.internal.notary.UniquenessProvider
import net.corda.core.internal.notary.isConsumedByTheSameTx
import net.corda.core.internal.notary.validateTimeWindow
import net.corda.core.schemas.PersistentStateRef
import net.corda.core.serialization.CordaSerializable
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.serialization.serialize
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.debug
import net.corda.node.utilities.AppendOnlyPersistentMap
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.nodeapi.internal.persistence.NODE_DATABASE_PREFIX
import net.corda.nodeapi.internal.persistence.currentDBSession
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.concurrent.ThreadSafe
import javax.persistence.Column
import javax.persistence.EmbeddedId
import javax.persistence.Entity
import javax.persistence.GeneratedValue
import javax.persistence.Id
import javax.persistence.Lob
import javax.persistence.MappedSuperclass
import kotlin.concurrent.thread

/** A RDBMS backed Uniqueness provider */
@ThreadSafe
class PersistentUniquenessProvider(val clock: Clock, val database: CordaPersistence, cacheFactory: NamedCacheFactory, val signTransaction : SigningFunction) : UniquenessProvider, SingletonSerializeAsToken() {

    @MappedSuperclass
    class BaseComittedState(
            @EmbeddedId
            val id: PersistentStateRef,

            @Column(name = "consuming_transaction_id", nullable = true)
            val consumingTxHash: String?
    )

    @Entity
    @javax.persistence.Table(name = "${NODE_DATABASE_PREFIX}notary_request_log")
    @CordaSerializable
    class Request(
            @Id
            @GeneratedValue
            @Column(nullable = true)
            val id: Int? = null,

            @Column(name = "consuming_transaction_id", nullable = true)
            val consumingTxHash: String?,

            @Column(name = "requesting_party_name", nullable = true)
            var partyName: String?,

            @Lob
            @Column(name = "request_signature", nullable = false)
            val requestSignature: ByteArray,

            @Column(name = "request_timestamp", nullable = false)
            var requestDate: Instant
    )

    @Suppress("MagicNumber") // database column length
    @Entity
    @javax.persistence.Table(name = "${NODE_DATABASE_PREFIX}notary_committed_txs")
    class CommittedTransaction(
            @Id
            @Column(name = "transaction_id", nullable = false, length = 80)
            val transactionId: String
    )

    private data class CommitRequest(
            val states: List<StateRef>,
            val txId: SecureHash,
            val callerIdentity: Party,
            val requestSignature: NotarisationRequestSignature,
            val timeWindow: TimeWindow?,
            val references: List<StateRef>,
            val future: OpenFuture<UniquenessProvider.Result>)

    @Entity
    @javax.persistence.Table(name = "${NODE_DATABASE_PREFIX}notary_committed_states")
    class CommittedState(id: PersistentStateRef, consumingTxHash: String) : BaseComittedState(id, consumingTxHash)

    private val commitLog = createMap(cacheFactory)

    private val requestQueue = LinkedBlockingQueue<CommitRequest>(requestQueueSize)
    private val nrQueuedStates = AtomicInteger(0)

    /**
     * Measured in states per minute, with a minimum of 1. We take an average of the last 100 commits.
     * Minutes was chosen to increase accuracy by 60x over seconds, given we have to use longs here.
     */
    private val throughputHistory = SlidingWindowReservoir(100)
    @Volatile
    private var throughput: Double = 0.0

    /**
     * Estimated time of request processing.
     * This uses performance metrics to gauge how long the wait time for a newly queued state will probably be.
     * It checks that there is actual traffic going on (i.e. a non-zero number of states are queued and there
     * is actual throughput) and then returns the expected wait time scaled up by a factor of 2 to give a probable
     * upper bound.
     *
     * @param numStates The number of states (input + reference) we're about to request be notarised.
     */
    override fun getEta(numStates: Int): Duration {
        val rate = throughput
        val nrStates = nrQueuedStates.getAndAdd(numStates)
        log.debug { "rate: $rate, queueSize: $nrStates" }
        if (rate > 0.0 && nrStates > 0) {
            return Duration.ofSeconds((2 * TimeUnit.MINUTES.toSeconds(1) * nrStates / rate).toLong())
        }
        return NotaryServiceFlow.defaultEstimatedWaitTime
    }

    /** A request processor thread. */
    private val processorThread = thread(name = "Notary request queue processor", isDaemon = true) {
        try {
            while (!Thread.interrupted()) {
                processRequest(requestQueue.take())
            }
        } catch (e: InterruptedException) {
        }
        log.debug { "Shutting down with ${requestQueue.size} in-flight requests unprocessed." }
    }

    companion object {
        private const val requestQueueSize = 100_000
        private val log = contextLogger()
        fun createMap(cacheFactory: NamedCacheFactory): AppendOnlyPersistentMap<StateRef, SecureHash, CommittedState, PersistentStateRef> =
                AppendOnlyPersistentMap(
                        cacheFactory = cacheFactory,
                        name = "PersistentUniquenessProvider_transactions",
                        toPersistentEntityKey = { PersistentStateRef(it.txhash.toString(), it.index) },
                        fromPersistentEntity = {
                            //TODO null check will become obsolete after making DB/JPA columns not nullable
                            val txId = it.id.txId
                            val index = it.id.index
                            Pair(
                                    StateRef(txhash = SecureHash.create(txId), index = index),
                                    SecureHash.create(it.consumingTxHash)
                            )

                        },
                        toPersistentEntity = { (txHash, index): StateRef, id: SecureHash ->
                            CommittedState(
                                    id = PersistentStateRef(txHash.toString(), index),
                                    consumingTxHash = id.toString()
                            )
                        },
                        persistentEntityClass = CommittedState::class.java
                )
    }

    /**
     * Generates and adds a [CommitRequest] to the request queue. If the request queue is full, this method will block
     * until space is available.
     *
     * Returns a future that will complete once the request is processed, containing the commit [Result].
     */
    override fun commit(
            states: List<StateRef>,
            txId: SecureHash,
            callerIdentity: Party,
            requestSignature: NotarisationRequestSignature,
            timeWindow: TimeWindow?,
            references: List<StateRef>
    ): CordaFuture<UniquenessProvider.Result> {
        val future = openFuture<UniquenessProvider.Result>()
        val request = CommitRequest(states, txId, callerIdentity, requestSignature, timeWindow, references, future)
        requestQueue.put(request)
        log.debug { "Request added to queue. TxId: $txId" }
        return future
    }

    private fun logRequest(txId: SecureHash, callerIdentity: Party, requestSignature: NotarisationRequestSignature) {
        val request = Request(
                consumingTxHash = txId.toString(),
                partyName = callerIdentity.name.toString(),
                requestSignature = requestSignature.serialize().bytes,
                requestDate = clock.instant()
        )
        val session = currentDBSession()
        session.persist(request)
    }

    private fun findAlreadyCommitted(
            states: List<StateRef>,
            references: List<StateRef>,
            commitLog: AppendOnlyPersistentMap<StateRef, SecureHash, CommittedState, PersistentStateRef>
    ): LinkedHashMap<StateRef, StateConsumptionDetails> {
        val conflictingStates = LinkedHashMap<StateRef, StateConsumptionDetails>()

        fun checkConflicts(toCheck: List<StateRef>, type: StateConsumptionDetails.ConsumedStateType) {
            return toCheck.forEach { stateRef ->
                val consumingTx = commitLog[stateRef]
                if (consumingTx != null) conflictingStates[stateRef] = StateConsumptionDetails(consumingTx.sha256(), type)
            }
        }

        checkConflicts(states, StateConsumptionDetails.ConsumedStateType.INPUT_STATE)
        checkConflicts(references, StateConsumptionDetails.ConsumedStateType.REFERENCE_INPUT_STATE)

        return conflictingStates
    }

    private fun commitOne(
            states: List<StateRef>,
            txId: SecureHash,
            callerIdentity: Party,
            requestSignature: NotarisationRequestSignature,
            timeWindow: TimeWindow?,
            references: List<StateRef>
    ) {
        database.transaction {
            logRequest(txId, callerIdentity, requestSignature)
            val conflictingStates = findAlreadyCommitted(states, references, commitLog)
            if (conflictingStates.isNotEmpty()) {
                if (states.isEmpty()) {
                    handleReferenceConflicts(txId, conflictingStates)
                } else {
                    handleConflicts(txId, conflictingStates)
                }
            } else {
                handleNoConflicts(timeWindow, states, txId, commitLog)
            }
        }
    }

    private fun previouslyCommitted(txId: SecureHash): Boolean {
        val session = currentDBSession()
        return session.find(CommittedTransaction::class.java, txId.toString()) != null
    }

    private fun handleReferenceConflicts(txId: SecureHash, conflictingStates: LinkedHashMap<StateRef, StateConsumptionDetails>) {
        if (!previouslyCommitted(txId)) {
            val conflictError = NotaryError.Conflict(txId, conflictingStates)
            log.info("Failure, input states already committed: ${conflictingStates.keys}. TxId: $txId")
            throw NotaryInternalException(conflictError)
        }
        log.info("Transaction $txId already notarised. TxId: $txId")
    }

    private fun handleConflicts(txId: SecureHash, conflictingStates: LinkedHashMap<StateRef, StateConsumptionDetails>) {
        if (isConsumedByTheSameTx(txId.sha256(), conflictingStates)) {
            log.info("Transaction $txId already notarised. TxId: $txId")
            return
        } else {
            log.info("Failure, input states already committed: ${conflictingStates.keys}. TxId: $txId")
            val conflictError = NotaryError.Conflict(txId, conflictingStates)
            throw NotaryInternalException(conflictError)
        }
    }

    private fun handleNoConflicts(timeWindow: TimeWindow?, states: List<StateRef>, txId: SecureHash, commitLog: AppendOnlyPersistentMap<StateRef, SecureHash, CommittedState, PersistentStateRef>) {
        // Skip if this is a re-notarisation of a reference-only transaction
        if (states.isEmpty() && previouslyCommitted(txId)) {
            return
        }

        val outsideTimeWindowError = validateTimeWindow(clock.instant(), timeWindow)
        if (outsideTimeWindowError == null) {
            states.forEach { stateRef ->
                commitLog[stateRef] = txId
            }
            val session = currentDBSession()
            session.persist(CommittedTransaction(txId.toString()))
            log.info("Successfully committed all input states: $states. TxId: $txId")
        } else {
            throw NotaryInternalException(outsideTimeWindowError)
        }
    }

    private fun decrementQueueSize(request: CommitRequest): Int {
        val nrStates = request.states.size + request.references.size
        nrQueuedStates.addAndGet(-nrStates)
        return nrStates
    }

    private fun processRequest(request: CommitRequest) {
        val numStates = decrementQueueSize(request)
        try {
            val duration = elapsedTime {
                commitOne(request.states, request.txId, request.callerIdentity, request.requestSignature, request.timeWindow, request.references)
            }
            val statesPerMinute = numStates.toLong() * TimeUnit.MINUTES.toNanos(1) / duration.toNanos()
            throughputHistory.update(maxOf(statesPerMinute, 1))
            throughput = throughputHistory.snapshot.median // Median deemed more stable / representative than mean.
            respondWithSuccess(request)
        } catch (e: Exception) {
            log.warn("Error processing commit request", e)
            respondWithError(request, e)
        }
    }

    private fun respondWithError(request: CommitRequest, exception: Exception) {
        if (exception is NotaryInternalException) {
            request.future.set(UniquenessProvider.Result.Failure(exception.error))
        } else {
            request.future.setException(NotaryInternalException(NotaryError.General(Exception("Internal service error."))))
        }
    }

    private fun respondWithSuccess(request: CommitRequest) {
        val signedTx = signTransaction(request.txId)
        request.future.set(UniquenessProvider.Result.Success(signedTx))
    }
}
