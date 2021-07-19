package net.corda.core.messaging

import net.corda.core.CordaInternal
import net.corda.core.concurrent.CordaFuture
import net.corda.core.context.InvocationContext
import net.corda.core.contracts.ContractState
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.FlowInitiator
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.StateMachineRunId
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.node.NetworkParameters
import net.corda.core.node.NodeDiagnosticInfo
import net.corda.core.node.NodeInfo
import net.corda.core.node.services.AttachmentId
import net.corda.core.node.services.NetworkMapCache
import net.corda.core.node.services.TransactionStorage
import net.corda.core.node.services.Vault
import net.corda.core.node.services.VaultQueryException
import net.corda.core.node.services.vault.*
import net.corda.core.serialization.CordaSerializable
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.Try
import rx.Observable
import rx.schedulers.Schedulers
import rx.subjects.PublishSubject
import java.io.IOException
import java.io.InputStream
import java.security.PublicKey
import java.time.Instant

/**
 * Represents information about a flow (the name "state machine" is legacy, Kotlin users can use the [FlowInfo] type
 * alias). You can access progress tracking, information about why the flow was started and so on.
 */
@CordaSerializable
data class StateMachineInfo @JvmOverloads constructor(
        /** A universally unique ID ([java.util.UUID]) representing this particular instance of the named flow. */
        val id: StateMachineRunId,
        /** The JVM class name of the flow code. */
        val flowLogicClassName: String,
        /**
         * An object representing information about the initiator of the flow. Note that this field is
         * superseded by the [invocationContext] property, which has more detail.
         */
        @Deprecated("There is more info available using 'invocationContext'")
        val initiator: FlowInitiator,
        /** A [DataFeed] of the current progress step as a human readable string, and updates to that string. */
        val progressTrackerStepAndUpdates: DataFeed<String, String>?,
        /** An [InvocationContext] describing why and by whom the flow was started. */
        val invocationContext: InvocationContext = initiator.invocationContext
) {
    @Suppress("DEPRECATION")
    fun copy(id: StateMachineRunId = this.id,
             flowLogicClassName: String = this.flowLogicClassName,
             initiator: FlowInitiator = this.initiator,
             progressTrackerStepAndUpdates: DataFeed<String, String>? = this.progressTrackerStepAndUpdates): StateMachineInfo {
        return copy(id = id, flowLogicClassName = flowLogicClassName, initiator = initiator, progressTrackerStepAndUpdates = progressTrackerStepAndUpdates, invocationContext = invocationContext)
    }

    override fun toString(): String = "${javaClass.simpleName}($id, $flowLogicClassName)"
}

/** An alias for [StateMachineInfo] which uses more modern terminology. */
typealias FlowInfo = StateMachineInfo

@CordaSerializable
sealed class StateMachineUpdate {
    abstract val id: StateMachineRunId

    data class Added(val stateMachineInfo: StateMachineInfo) : StateMachineUpdate() {
        override val id: StateMachineRunId get() = stateMachineInfo.id
    }

    data class Removed(override val id: StateMachineRunId, val result: Try<*>) : StateMachineUpdate()
}

// DOCSTART 1
/**
 * Data class containing information about the scheduled network parameters update. The info is emitted every time node
 * receives network map with [ParametersUpdate] which wasn't seen before. For more information see: [CordaRPCOps.networkParametersFeed]
 * and [CordaRPCOps.acceptNewNetworkParameters].
 * @property hash new [NetworkParameters] hash
 * @property parameters new [NetworkParameters] data structure
 * @property description description of the update
 * @property updateDeadline deadline for accepting this update using [CordaRPCOps.acceptNewNetworkParameters]
 */
@CordaSerializable
data class ParametersUpdateInfo(
        val hash: SecureHash,
        val parameters: NetworkParameters,
        val description: String,
        val updateDeadline: Instant
)
// DOCEND 1

@CordaSerializable
data class StateMachineTransactionMapping(val stateMachineRunId: StateMachineRunId, val transactionId: SecureHash)

/** RPC operations that the node exposes to clients. */
interface CordaRPCOps : RPCOps {
    /** Returns a list of currently in-progress state machine infos. */
    fun stateMachinesSnapshot(): List<StateMachineInfo>

    /**
     * Returns a data feed of currently in-progress state machine infos and an observable of
     * future state machine adds/removes.
     */
    @RPCReturnsObservables
    fun stateMachinesFeed(): DataFeed<List<StateMachineInfo>, StateMachineUpdate>

    /**
     * Returns a snapshot of vault states for a given query criteria (and optional order and paging specification)
     *
     * Generic vault query function which takes a [QueryCriteria] object to define filters,
     * optional [PageSpecification] and optional [Sort] modification criteria (default unsorted),
     * and returns a [Vault.Page] object containing the following:
     *  1. states as a List of <StateAndRef> (page number and size defined by [PageSpecification])
     *  2. states metadata as a List of [Vault.StateMetadata] held in the Vault States table.
     *  3. total number of results available if [PageSpecification] supplied (otherwise returns -1)
     *  4. status types used in this query: UNCONSUMED, CONSUMED, ALL
     *  5. other results (aggregate functions with/without using value groups)
     *
     * @throws VaultQueryException if the query cannot be executed for any reason
     *        (missing criteria or parsing error, paging errors, unsupported query, underlying database error)
     *
     * Notes
     *   If no [PageSpecification] is provided, a maximum of [DEFAULT_PAGE_SIZE] results will be returned.
     *   API users must specify a [PageSpecification] if they are expecting more than [DEFAULT_PAGE_SIZE] results,
     *   otherwise a [VaultQueryException] will be thrown alerting to this condition.
     *   It is the responsibility of the API user to request further pages and/or specify a more suitable [PageSpecification].
     */
    // DOCSTART VaultQueryByAPI
    @RPCReturnsObservables
    fun <T : ContractState> vaultQueryBy(criteria: QueryCriteria,
                                         paging: PageSpecification,
                                         sorting: Sort,
                                         contractStateType: Class<out T>): Vault.Page<T>
    // DOCEND VaultQueryByAPI

    // Note: cannot apply @JvmOverloads to interfaces nor interface implementations
    // Java Helpers

    // DOCSTART VaultQueryAPIHelpers
    fun <T : ContractState> vaultQuery(contractStateType: Class<out T>): Vault.Page<T>

    fun <T : ContractState> vaultQueryByCriteria(criteria: QueryCriteria, contractStateType: Class<out T>): Vault.Page<T>

    fun <T : ContractState> vaultQueryByWithPagingSpec(contractStateType: Class<out T>, criteria: QueryCriteria, paging: PageSpecification): Vault.Page<T>

    fun <T : ContractState> vaultQueryByWithSorting(contractStateType: Class<out T>, criteria: QueryCriteria, sorting: Sort): Vault.Page<T>
    // DOCEND VaultQueryAPIHelpers

    /**
     * Returns a snapshot (as per queryBy) and an observable of future updates to the vault for the given query criteria.
     *
     * Generic vault query function which takes a [QueryCriteria] object to define filters,
     * optional [PageSpecification] and optional [Sort] modification criteria (default unsorted),
     * and returns a [DataFeed] object containing
     * 1) a snapshot as a [Vault.Page] (described previously in [CordaRPCOps.vaultQueryBy])
     * 2) an [Observable] of [Vault.Update]
     *
     * Notes: the snapshot part of the query adheres to the same behaviour as the [CordaRPCOps.vaultQueryBy] function.
     *        the [QueryCriteria] applies to both snapshot and deltas (streaming updates).
     */
    // DOCSTART VaultTrackByAPI
    @RPCReturnsObservables
    fun <T : ContractState> vaultTrackBy(criteria: QueryCriteria,
                                         paging: PageSpecification,
                                         sorting: Sort,
                                         contractStateType: Class<out T>): DataFeed<Vault.Page<T>, Vault.Update<T>>
    // DOCEND VaultTrackByAPI

    // Note: cannot apply @JvmOverloads to interfaces nor interface implementations
    // Java Helpers

    // DOCSTART VaultTrackAPIHelpers
    fun <T : ContractState> vaultTrack(contractStateType: Class<out T>): DataFeed<Vault.Page<T>, Vault.Update<T>>

    fun <T : ContractState> vaultTrackByCriteria(contractStateType: Class<out T>, criteria: QueryCriteria): DataFeed<Vault.Page<T>, Vault.Update<T>>

    fun <T : ContractState> vaultTrackByWithPagingSpec(contractStateType: Class<out T>, criteria: QueryCriteria, paging: PageSpecification): DataFeed<Vault.Page<T>, Vault.Update<T>>

    fun <T : ContractState> vaultTrackByWithSorting(contractStateType: Class<out T>, criteria: QueryCriteria, sorting: Sort): DataFeed<Vault.Page<T>, Vault.Update<T>>
    // DOCEND VaultTrackAPIHelpers

    /**
     * Returns a paged snapshot of transactions ordered by timestamp (In Ascending order) for a given paging specification
     *
     *  returns a [TransactionStorage.Page] object containing the following:
     *  1. signedTransactions as a List of <TransactionStorage.RecordedTransaction> (page number and size defined by [PageSpecification])
     *  2. totalTransactionsAvailable, total number of recorded transactions
     *
     * @throws TransactionStorage.TransactionsQueryException if there are paging errors
     */
    fun transactionsSnapshotWithPagingSpec(pagingSpec: PageSpecification): TransactionStorage.Page

    /**
     * Returns a paged snapshot of transactions ordered by timestamp (In Ascending order), with paging spec., and observable for future transactions
     *
     * @throws TransactionStorage.TransactionsQueryException if there are paging errors
     */
    @RPCReturnsObservables
    fun transactionsFeedWithPagingSpec(pagingSpec: PageSpecification): DataFeed<TransactionStorage.Page, SignedTransaction>

    /**
     * @suppress Returns a list of all recorded transactions.
     *
     * TODO This method should be removed once SGX work is finalised and the design of the corresponding API using [FilteredTransaction] can be started
     */
    @Deprecated("This method is intended only for internal use and will be removed from the public API soon.")
    fun internalVerifiedTransactionsSnapshot(): List<SignedTransaction>

    /**
     * @suppress Returns the full transaction for the provided ID
     *
     * TODO This method should be removed once SGX work is finalised and the design of the corresponding API using [FilteredTransaction] can be started
     */
    @CordaInternal
    @Deprecated("This method is intended only for internal use and will be removed from the public API soon.")
    fun internalFindVerifiedTransaction(txnId: SecureHash): SignedTransaction?

    /**
     * @suppress Returns a data feed of all recorded transactions and an observable of future recorded ones.
     *
     * TODO This method should be removed once SGX work is finalised and the design of the corresponding API using [FilteredTransaction] can be started
     */
    @Deprecated("This method is intended only for internal use and will be removed from the public API soon.")
    @RPCReturnsObservables
    fun internalVerifiedTransactionsFeed(): DataFeed<List<SignedTransaction>, SignedTransaction>

    /** Returns a snapshot list of existing state machine id - recorded transaction hash mappings. */
    fun stateMachineRecordedTransactionMappingSnapshot(): List<StateMachineTransactionMapping>

    /**
     * Returns a snapshot list of existing state machine id - recorded transaction hash mappings, and a stream of future
     * such mappings as well.
     */
    @RPCReturnsObservables
    fun stateMachineRecordedTransactionMappingFeed(): DataFeed<List<StateMachineTransactionMapping>, StateMachineTransactionMapping>

    /** Returns all parties currently visible on the network with their advertised services. */
    fun networkMapSnapshot(): List<NodeInfo>

    /**
     * Returns all parties currently visible on the network with their advertised services and an observable of
     * future updates to the network.
     */
    @RPCReturnsObservables
    fun networkMapFeed(): DataFeed<List<NodeInfo>, NetworkMapCache.MapChange>

    /** Returns the network parameters the node is operating under. */
    val networkParameters: NetworkParameters

    /**
     * Returns [DataFeed] object containing information on currently scheduled parameters update (null if none are currently scheduled)
     * and observable with future update events. Any update that occurs before the deadline automatically cancels the current one.
     * Only the latest update can be accepted.
     * Note: This operation may be restricted only to node administrators.
     */
    // TODO This operation should be restricted to just node admins.
    @RPCReturnsObservables
    fun networkParametersFeed(): DataFeed<ParametersUpdateInfo?, ParametersUpdateInfo>

    /**
     * Accept network parameters with given hash, hash is obtained through [networkParametersFeed] method.
     * Information is sent back to the zone operator that the node accepted the parameters update - this process cannot be
     * undone.
     * Only parameters that are scheduled for update can be accepted, if different hash is provided this method will fail.
     * Note: This operation may be restricted only to node administrators.
     * @param parametersHash hash of network parameters to accept
     * @throws IllegalArgumentException if network map advertises update with different parameters hash then the one accepted by node's operator.
     * @throws IOException if failed to send the approval to network map
     */
    // TODO This operation should be restricted to just node admins.
    fun acceptNewNetworkParameters(parametersHash: SecureHash)

    /**
     * Start the given flow with the given arguments. [logicType] must be annotated
     * with [net.corda.core.flows.StartableByRPC].
     */
    @RPCReturnsObservables
    fun <T> startFlowDynamic(logicType: Class<out FlowLogic<T>>, vararg args: Any?): FlowHandle<T>

    /**
     * Start the given flow with the given arguments, returning an [Observable] with a single observation of the
     * result of running the flow. [logicType] must be annotated with [net.corda.core.flows.StartableByRPC].
     */
    @RPCReturnsObservables
    fun <T> startTrackedFlowDynamic(logicType: Class<out FlowLogic<T>>, vararg args: Any?): FlowProgressHandle<T>

    /**
     * Attempts to kill a flow. This is not a clean termination and should be reserved for exceptional cases such as stuck fibers.
     *
     * @return whether the flow existed and was killed.
     */
    fun killFlow(id: StateMachineRunId): Boolean

    /** Returns Node's NodeInfo, assuming this will not change while the node is running. */
    fun nodeInfo(): NodeInfo

    /**
     * Returns Node's NodeDiagnosticInfo, including the version details as well as the information about installed CorDapps.
     */
    fun nodeDiagnosticInfo(): NodeDiagnosticInfo

    /**
     * Returns network's notary identities, assuming this will not change while the node is running.
     *
     * Note that the identities are sorted based on legal name, and the ordering might change once new notaries are introduced.
     */
    fun notaryIdentities(): List<Party>

    /** Add note(s) to an existing Vault transaction. */
    fun addVaultTransactionNote(txnId: SecureHash, txnNote: String)

    /** Retrieve existing note(s) for a given Vault transaction. */
    fun getVaultTransactionNotes(txnId: SecureHash): Iterable<String>

    /** Checks whether an attachment with the given hash is stored on the node. */
    fun attachmentExists(id: SecureHash): Boolean

    /** Download an attachment JAR by ID. */
    fun openAttachment(id: SecureHash): InputStream

    /** Uploads a jar to the node, returns it's hash. */
    @Throws(java.nio.file.FileAlreadyExistsException::class)
    fun uploadAttachment(jar: InputStream): SecureHash

    /** Uploads a jar including metadata to the node, returns it's hash. */
    @Throws(java.nio.file.FileAlreadyExistsException::class)
    fun uploadAttachmentWithMetadata(jar: InputStream, uploader: String, filename: String): SecureHash

    /** Queries attachments metadata */
    fun queryAttachments(query: AttachmentQueryCriteria, sorting: AttachmentSort?): List<AttachmentId>

    /** Returns the node's current time. */
    fun currentNodeTime(): Instant

    /**
     * Returns a [CordaFuture] which completes when the node has registered wih the network map service. It can also
     * complete with an exception if it is unable to.
     */
    @RPCReturnsObservables
    fun waitUntilNetworkReady(): CordaFuture<Void?>

    // TODO These need rethinking. Instead of these direct calls we should have a way of replicating a subset of
    // the node's state locally and query that directly.
    /**
     * Returns the well known identity from an abstract party. This is intended to resolve the well known identity
     * from a confidential identity, however it transparently handles returning the well known identity back if
     * a well known identity is passed in.
     *
     * @param party identity to determine well known identity for.
     * @return well known identity, if found.
     */
    fun wellKnownPartyFromAnonymous(party: AbstractParty): Party?

    /** Returns the [Party] corresponding to the given key, if found. */
    fun partyFromKey(key: PublicKey): Party?

    /** Returns the [Party] with the X.500 principal as it's [Party.name]. */
    fun wellKnownPartyFromX500Name(x500Name: CordaX500Name): Party?

    /**
     * Get a notary identity by name.
     *
     * @return the notary identity, or null if there is no notary by that name. Note that this will return null if there
     * is a peer with that name but they are not a recognised notary service.
     */
    fun notaryPartyFromX500Name(x500Name: CordaX500Name): Party?

    /**
     * Returns a list of candidate matches for a given string, with optional fuzzy(ish) matching. Fuzzy matching may
     * get smarter with time e.g. to correct spelling errors, so you should not hard-code indexes into the results
     * but rather show them via a user interface and let the user pick the one they wanted.
     *
     * @param query The string to check against the X.500 name components
     * @param exactMatch If true, a case sensitive match is done against each component of each X.500 name.
     */
    fun partiesFromName(query: String, exactMatch: Boolean): Set<Party>

    /** Enumerates the class names of the flows that this node knows about. */
    fun registeredFlows(): List<String>

    /**
     * Returns a node's info from the network map cache, where known.
     * Notice that when there are more than one node for a given name (in case of distributed services) first service node
     * found will be returned.
     *
     * @return the node info if available.
     */
    fun nodeInfoFromParty(party: AbstractParty): NodeInfo?

    /**
     * Clear all network map data from local node cache. Notice that after invoking this method your node will lose
     * network map data and effectively won't be able to start any flow with the peers until network map is downloaded
     * again on next poll - from `additional-node-infos` directory or from network map server. It depends on the
     * polling interval when it happens. You can also use [refreshNetworkMapCache] to force next fetch from network map server
     * (not from directory - it will happen automatically).
     * If you run local test deployment and want clear view of the network, you may want to clear also `additional-node-infos`
     * directory, because cache can be repopulated from there.
     */
    fun clearNetworkMapCache()

    /**
     * Poll network map server if available for the network map. Notice that you need to have `compatibilityZone`
     * or `networkServices` configured. This is normally done automatically on the regular time interval, but you may wish to
     * have the fresh view of network earlier.
     */
    fun refreshNetworkMapCache()

    /** Sets the value of the node's flows draining mode.
     * If this mode is [enabled], the node will reject new flows through RPC, ignore scheduled flows, and do not process
     * initial session messages, meaning that P2P counterparties will not be able to initiate new flows involving the node.
     *
     * @param enabled whether the flows draining mode will be enabled.
     * */
    fun setFlowsDrainingModeEnabled(enabled: Boolean)

    /**
     * Returns whether the flows draining mode is enabled.
     *
     * @see setFlowsDrainingModeEnabled
     */
    fun isFlowsDrainingModeEnabled(): Boolean

    /**
     * Shuts the node down. Returns immediately.
     * This does not wait for flows to be completed.
     */
    fun shutdown()

    /**
     * Shuts the node down. Returns immediately.
     * @param drainPendingFlows whether the node will wait for pending flows to be completed before exiting. While draining, new flows from RPC will be rejected.
     */
    fun terminate(drainPendingFlows: Boolean = false)

    /**
     * Returns whether the node is waiting for pending flows to complete before shutting down.
     * Disabling draining mode cancels this state.
     *
     * @return whether the node will shutdown when the pending flows count reaches zero.
     */
    fun isWaitingForShutdown(): Boolean
}

/**
 * Returns a [DataFeed] of the number of pending flows. The [Observable] for the updates will complete the moment all pending flows will have terminated.
 */
@Deprecated("For automated upgrades, consider using the `gracefulShutdown` command in an SSH session instead.")
fun CordaRPCOps.pendingFlowsCount(): DataFeed<Int, Pair<Int, Int>> {
    val updates = PublishSubject.create<Pair<Int, Int>>()
    val initialPendingFlowsCount = stateMachinesFeed().let {
        var completedFlowsCount = 0
        var pendingFlowsCount = it.snapshot.size
        it.updates.observeOn(Schedulers.io()).subscribe({ update ->
            when (update) {
                is StateMachineUpdate.Added -> {
                    pendingFlowsCount++
                    updates.onNext(completedFlowsCount to pendingFlowsCount)
                }
                is StateMachineUpdate.Removed -> {
                    completedFlowsCount++
                    updates.onNext(completedFlowsCount to pendingFlowsCount)
                    if (completedFlowsCount == pendingFlowsCount) {
                        updates.onCompleted()
                    }
                }
            }
        }, updates::onError)
        if (pendingFlowsCount == 0) {
            updates.onCompleted()
        }
        pendingFlowsCount
    }
    return DataFeed(initialPendingFlowsCount, updates)
}

inline fun <reified T : ContractState> CordaRPCOps.vaultQueryBy(criteria: QueryCriteria = QueryCriteria.VaultQueryCriteria(),
                                                                paging: PageSpecification = PageSpecification(),
                                                                sorting: Sort = Sort(emptySet())): Vault.Page<T> {
    return vaultQueryBy(criteria, paging, sorting, T::class.java)
}

inline fun <reified T : ContractState> CordaRPCOps.vaultTrackBy(criteria: QueryCriteria = QueryCriteria.VaultQueryCriteria(),
                                                                paging: PageSpecification = PageSpecification(),
                                                                sorting: Sort = Sort(emptySet())): DataFeed<Vault.Page<T>, Vault.Update<T>> {
    return vaultTrackBy(criteria, paging, sorting, T::class.java)
}

// Note that the passed in constructor function is only used for unification of other type parameters and reification of
// the Class instance of the flow. This could be changed to use the constructor function directly.

inline fun <T, reified R : FlowLogic<T>> CordaRPCOps.startFlow(
        @Suppress("UNUSED_PARAMETER")
        flowConstructor: () -> R
): FlowHandle<T> = startFlowDynamic(R::class.java)

inline fun <T, A, reified R : FlowLogic<T>> CordaRPCOps.startFlow(
        @Suppress("UNUSED_PARAMETER")
        flowConstructor: (A) -> R,
        arg0: A
): FlowHandle<T> = startFlowDynamic(R::class.java, arg0)

/**
 * Extension function for type safe invocation of flows from Kotlin, for example:
 *
 * val rpc: CordaRPCOps = (..)
 * rpc.startFlow(::ResolveTransactionsFlow, setOf<SecureHash>(), aliceIdentity)
 */
inline fun <T, A, B, reified R : FlowLogic<T>> CordaRPCOps.startFlow(
        @Suppress("UNUSED_PARAMETER")
        flowConstructor: (A, B) -> R,
        arg0: A,
        arg1: B
): FlowHandle<T> = startFlowDynamic(R::class.java, arg0, arg1)

inline fun <T, A, B, C, reified R : FlowLogic<T>> CordaRPCOps.startFlow(
        @Suppress("UNUSED_PARAMETER")
        flowConstructor: (A, B, C) -> R,
        arg0: A,
        arg1: B,
        arg2: C
): FlowHandle<T> = startFlowDynamic(R::class.java, arg0, arg1, arg2)

inline fun <T, A, B, C, D, reified R : FlowLogic<T>> CordaRPCOps.startFlow(
        @Suppress("UNUSED_PARAMETER")
        flowConstructor: (A, B, C, D) -> R,
        arg0: A,
        arg1: B,
        arg2: C,
        arg3: D
): FlowHandle<T> = startFlowDynamic(R::class.java, arg0, arg1, arg2, arg3)

inline fun <T, A, B, C, D, E, reified R : FlowLogic<T>> CordaRPCOps.startFlow(
        @Suppress("UNUSED_PARAMETER")
        flowConstructor: (A, B, C, D, E) -> R,
        arg0: A,
        arg1: B,
        arg2: C,
        arg3: D,
        arg4: E
): FlowHandle<T> = startFlowDynamic(R::class.java, arg0, arg1, arg2, arg3, arg4)

inline fun <T, A, B, C, D, E, F, reified R : FlowLogic<T>> CordaRPCOps.startFlow(
        @Suppress("UNUSED_PARAMETER")
        flowConstructor: (A, B, C, D, E, F) -> R,
        arg0: A,
        arg1: B,
        arg2: C,
        arg3: D,
        arg4: E,
        arg5: F
): FlowHandle<T> = startFlowDynamic(R::class.java, arg0, arg1, arg2, arg3, arg4, arg5)

/**
 * Extension function for type safe invocation of flows from Kotlin, with progress tracking enabled.
 */
@Suppress("unused")
inline fun <T, reified R : FlowLogic<T>> CordaRPCOps.startTrackedFlow(
        @Suppress("unused_parameter")
        flowConstructor: () -> R
): FlowProgressHandle<T> = startTrackedFlowDynamic(R::class.java)

@Suppress("unused")
inline fun <T, A, reified R : FlowLogic<T>> CordaRPCOps.startTrackedFlow(
        @Suppress("unused_parameter")
        flowConstructor: (A) -> R,
        arg0: A
): FlowProgressHandle<T> = startTrackedFlowDynamic(R::class.java, arg0)

@Suppress("unused")
inline fun <T, A, B, reified R : FlowLogic<T>> CordaRPCOps.startTrackedFlow(
        @Suppress("unused_parameter")
        flowConstructor: (A, B) -> R,
        arg0: A,
        arg1: B
): FlowProgressHandle<T> = startTrackedFlowDynamic(R::class.java, arg0, arg1)

@Suppress("unused")
inline fun <T, A, B, C, reified R : FlowLogic<T>> CordaRPCOps.startTrackedFlow(
        @Suppress("unused_parameter")
        flowConstructor: (A, B, C) -> R,
        arg0: A,
        arg1: B,
        arg2: C
): FlowProgressHandle<T> = startTrackedFlowDynamic(R::class.java, arg0, arg1, arg2)

@Suppress("unused")
inline fun <T, A, B, C, D, reified R : FlowLogic<T>> CordaRPCOps.startTrackedFlow(
        @Suppress("unused_parameter")
        flowConstructor: (A, B, C, D) -> R,
        arg0: A,
        arg1: B,
        arg2: C,
        arg3: D
): FlowProgressHandle<T> = startTrackedFlowDynamic(R::class.java, arg0, arg1, arg2, arg3)

@Suppress("unused")
inline fun <T, A, B, C, D, E, reified R : FlowLogic<T>> CordaRPCOps.startTrackedFlow(
        @Suppress("unused_parameter")
        flowConstructor: (A, B, C, D, E) -> R,
        arg0: A,
        arg1: B,
        arg2: C,
        arg3: D,
        arg4: E
): FlowProgressHandle<T> = startTrackedFlowDynamic(R::class.java, arg0, arg1, arg2, arg3, arg4)

@Suppress("unused")
inline fun <T, A, B, C, D, E, F, reified R : FlowLogic<T>> CordaRPCOps.startTrackedFlow(
        @Suppress("unused_parameter")
        flowConstructor: (A, B, C, D, E, F) -> R,
        arg0: A,
        arg1: B,
        arg2: C,
        arg3: D,
        arg4: E,
        arg5: F
): FlowProgressHandle<T> = startTrackedFlowDynamic(R::class.java, arg0, arg1, arg2, arg3, arg4, arg5)

/**
 * The Data feed contains a snapshot of the requested data and an [Observable] of future updates.
 */
@CordaSerializable
data class DataFeed<out A, B>(val snapshot: A, val updates: Observable<B>)
