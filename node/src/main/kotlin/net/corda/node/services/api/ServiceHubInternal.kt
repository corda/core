package net.corda.node.services.api

import net.corda.core.concurrent.CordaFuture
import net.corda.core.context.InvocationContext
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.StateMachineRunId
import net.corda.core.internal.*
import net.corda.core.internal.concurrent.OpenFuture
import net.corda.core.messaging.DataFeed
import net.corda.core.messaging.StateMachineTransactionMapping
import net.corda.core.node.NodeInfo
import net.corda.core.node.StatesToRecord
import net.corda.core.node.services.NetworkMapCache
import net.corda.core.node.services.NetworkMapCacheBase
import net.corda.core.node.services.TransactionStorage
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.contextLogger
import net.corda.node.internal.InitiatedFlowFactory
import net.corda.node.internal.cordapp.CordappProviderInternal
import net.corda.node.services.DbTransactionsResolver
import net.corda.node.services.config.NodeConfiguration
import net.corda.node.services.messaging.MessagingService
import net.corda.node.services.persistence.AttachmentStorageInternal
import net.corda.node.services.statemachine.ExternalEvent
import net.corda.node.services.statemachine.FlowStateMachineImpl
import net.corda.nodeapi.internal.persistence.CordaPersistence
import java.security.PublicKey
import java.util.*

interface NetworkMapCacheInternal : NetworkMapCache, NetworkMapCacheBase {
    override val nodeReady: OpenFuture<Void?>

    val allNodeHashes: List<SecureHash>

    fun getNodeByHash(nodeHash: SecureHash): NodeInfo?

    /** Find nodes from the [PublicKey] toShortString representation.
     * This is used for Artemis bridge lookup process. */
    fun getNodesByOwningKeyIndex(identityKeyIndex: String): List<NodeInfo>

    /** Adds (or updates) a node to the local cache (generally only used for adding ourselves). */
    fun addOrUpdateNode(node: NodeInfo)

    /** Adds (or updates) nodes to the local cache. */
    fun addOrUpdateNodes(nodes: List<NodeInfo>)

    /** Removes a node from the local cache. */
    fun removeNode(node: NodeInfo)
}

interface ServiceHubInternal : ServiceHubCoreInternal {
    companion object {
        private val log = contextLogger()

        private fun topologicalSort(transactions: Collection<SignedTransaction>): Collection<SignedTransaction> {
            if (transactions.size == 1) return transactions
            val sort = TopologicalSort()
            for (tx in transactions) {
                sort.add(tx, tx.dependencies)
            }
            return sort.complete()
        }

        fun recordTransactions(statesToRecord: StatesToRecord,
                               txs: Collection<SignedTransaction>,
                               validatedTransactions: WritableTransactionStorage,
                               stateMachineRecordedTransactionMapping: StateMachineRecordedTransactionMappingStorage,
                               vaultService: VaultServiceInternal,
                               database: CordaPersistence) {

            database.transaction {
                require(txs.isNotEmpty()) { "No transactions passed in for recording" }

                val orderedTxs = topologicalSort(txs)
                // Divide transactions into those seen before and those that are new to this node if ALL_VISIBLE states are being recorded.
                // This allows the node to re-record transactions that have previously only been seen at the ONLY_RELEVANT level. Note that
                // for transactions being recorded at ONLY_RELEVANT, if this transaction has been seen before its outputs should already
                // have been recorded at ONLY_RELEVANT, so there shouldn't be anything to re-record here.
                val (recordedTransactions, previouslySeenTxs) = if (statesToRecord != StatesToRecord.ALL_VISIBLE) {
                    orderedTxs.filter(validatedTransactions::addTransaction) to emptyList()
                } else {
                    orderedTxs.partition(validatedTransactions::addTransaction)
                }
                val stateMachineRunId = FlowStateMachineImpl.currentStateMachine()?.id
                if (stateMachineRunId != null) {
                    recordedTransactions.forEach {
                        stateMachineRecordedTransactionMapping.addMapping(stateMachineRunId, it.id)
                    }
                } else {
                    log.warn("Transactions recorded from outside of a state machine")
                }

                // When the user has requested StatesToRecord.ALL we may end up recording and relationally mapping states
                // that do not involve us and that we cannot sign for. This will break coin selection and thus a warning
                // is present in the documentation for this feature (see the "Observer nodes" tutorial on docs.corda.net).
                //
                // The reason for this is three-fold:
                //
                // 1) We are putting in place the observer mode feature relatively quickly to meet specific customer
                //    launch target dates.
                //
                // 2) The right design for vaults which mix observations and relevant states isn't entirely clear yet.
                //
                // 3) If we get the design wrong it could create security problems and business confusions.
                //
                // Back in the bitcoinj days I did add support for "watching addresses" to the wallet code, which is the
                // Bitcoin equivalent of observer nodes:
                //
                //   https://bitcoinj.github.io/working-with-the-wallet#watching-wallets
                //
                // The ability to have a wallet containing both irrelevant and relevant states complicated everything quite
                // dramatically, even methods as basic as the getBalance() API which required additional modes to let you
                // query "balance I can spend" vs "balance I am observing". In the end it might have been better to just
                // require the user to create an entirely separate wallet for observing with.
                //
                // In Corda we don't support a single node having multiple vaults (at the time of writing), and it's not
                // clear that's the right way to go: perhaps adding an "origin" column to the VAULT_STATES table is a better
                // solution. Then you could select subsets of states depending on where the report came from.
                //
                // The risk of doing this is that apps/developers may use 'canned SQL queries' not written by us that forget
                // to add a WHERE clause for the origin column. Those queries will seem to work most of the time until
                // they're run on an observer node and mix in irrelevant data. In the worst case this may result in
                // erroneous data being reported to the user, which could cause security problems.
                //
                // Because the primary use case for recording irrelevant states is observer/regulator nodes, who are unlikely
                // to make writes to the ledger very often or at all, we choose to punt this issue for the time being.
                vaultService.notifyAll(statesToRecord, recordedTransactions.map { it.coreTransaction }, previouslySeenTxs.map { it.coreTransaction })
            }
        }
    }

    override val attachments: AttachmentStorageInternal
    override val vaultService: VaultServiceInternal
    /**
     * A map of hash->tx where tx has been signature/contract validated and the states are known to be correct.
     * The signatures aren't technically needed after that point, but we keep them around so that we can relay
     * the transaction data to other nodes that need it.
     */
    override val validatedTransactions: WritableTransactionStorage
    val stateMachineRecordedTransactionMapping: StateMachineRecordedTransactionMappingStorage
    val monitoringService: MonitoringService
    val schemaService: SchemaService
    override val networkMapCache: MembershipGroupCacheInternal
    val auditService: AuditService
    val rpcFlows: List<Class<out FlowLogic<*>>>
    val networkService: MessagingService
    val database: CordaPersistence
    val configuration: NodeConfiguration
    val nodeProperties: NodePropertiesStore
    override val cordappProvider: CordappProviderInternal

    fun getFlowFactory(initiatingFlowClass: Class<out FlowLogic<*>>): InitiatedFlowFactory<*>?
    val cacheFactory: NamedCacheFactory

    override fun recordTransactions(statesToRecord: StatesToRecord, txs: Iterable<SignedTransaction>) {
        txs.forEach { requireSupportedHashType(it) }
        recordTransactions(
                statesToRecord,
                txs as? Collection ?: txs.toList(), // We can't change txs to a Collection as it's now part of the public API
                validatedTransactions,
                stateMachineRecordedTransactionMapping,
                vaultService,
                database
        )
    }

    override fun createTransactionsResolver(flow: ResolveTransactionsFlow): TransactionsResolver = DbTransactionsResolver(flow)

    /**
     * Provides a way to topologically sort SignedTransactions. This means that given any two transactions T1 and T2 in the
     * list returned by [complete] if T1 is a dependency of T2 then T1 will occur earlier than T2.
     */
    private class TopologicalSort {
        private val forwardGraph = HashMap<SecureHash, MutableSet<SignedTransaction>>()
        private val transactions = ArrayList<SignedTransaction>()

        /**
         * Add a transaction to the to-be-sorted set of transactions.
         */
        fun add(stx: SignedTransaction, dependencies: Set<SecureHash>) {
            dependencies.forEach {
                // Note that we use a LinkedHashSet here to make the traversal deterministic (as long as the input list is).
                forwardGraph.computeIfAbsent(it) { LinkedHashSet() }.add(stx)
            }
            transactions += stx
        }

        /**
         * Return the sorted list of signed transactions.
         */
        fun complete(): List<SignedTransaction> {
            val visited = HashSet<SecureHash>(transactions.size)
            val result = ArrayList<SignedTransaction>(transactions.size)

            fun visit(transaction: SignedTransaction) {
                if (visited.add(transaction.id)) {
                    forwardGraph[transaction.id]?.forEach(::visit)
                    result += transaction
                }
            }

            transactions.forEach(::visit)

            return result.apply(Collections::reverse)
        }
    }
}

interface FlowStarter {

    /**
     * Starts an already constructed flow. Note that you must be on the server thread to call this method. This method
     * just synthesizes an [ExternalEvent.ExternalStartFlowEvent] and calls the method below.
     * @param context indicates who started the flow, see: [InvocationContext].
     */
    fun <T> startFlow(logic: FlowLogic<T>, context: InvocationContext): CordaFuture<out FlowStateMachineHandle<T>>

    /**
     * Starts a flow as described by an [ExternalEvent.ExternalStartFlowEvent].  If a transient error
     * occurs during invocation, it will re-attempt to start the flow.
     */
    fun <T> startFlow(event: ExternalEvent.ExternalStartFlowEvent<T>): CordaFuture<out FlowStateMachineHandle<T>>

    /**
     * Will check [logicType] and [args] against a whitelist and if acceptable then construct and initiate the flow.
     * Note that you must be on the server thread to call this method. [context] points how flow was started,
     * See: [InvocationContext].
     *
     * @throws net.corda.core.flows.IllegalFlowLogicException or IllegalArgumentException if there are problems with the
     * [logicType] or [args].
     */
    fun <T> invokeFlowAsync(
        logicType: Class<out FlowLogic<T>>,
        context: InvocationContext,
        vararg args: Any?
    ): CordaFuture<out FlowStateMachineHandle<T>>
}

interface StartedNodeServices : ServiceHubInternal, FlowStarter

/**
 * Thread-safe storage of transactions.
 */
interface WritableTransactionStorage : TransactionStorage {
    /**
     * Add a new *verified* transaction to the store, or convert the existing unverified transaction into a verified one.
     * @param transaction The transaction to be recorded.
     * @return true if the transaction was recorded as a *new verified* transaction, false if the transaction already exists.
     */
    // TODO: Throw an exception if trying to add a transaction with fewer signatures than an existing entry.
    fun addTransaction(transaction: SignedTransaction): Boolean

    /**
     * Add a new *unverified* transaction to the store.
     */
    fun addUnverifiedTransaction(transaction: SignedTransaction)

    /**
     * Return the transaction with the given ID from the store, and a flag of whether it's verified. Returns null if no transaction with the
     * ID exists.
     */
    fun getTransactionInternal(id: SecureHash): Pair<SignedTransaction, Boolean>?

    /**
     * Returns a future that completes with the transaction corresponding to [id] once it has been committed. Do not warn when run inside
     * a DB transaction.
     */
    fun trackTransactionWithNoWarning(id: SecureHash): CordaFuture<SignedTransaction>
}

/**
 * This is the interface to storage storing state machine -> recorded tx mappings. Any time a transaction is recorded
 * during a flow run [addMapping] should be called.
 */
interface StateMachineRecordedTransactionMappingStorage {
    fun addMapping(stateMachineRunId: StateMachineRunId, transactionId: SecureHash)
    fun track(): DataFeed<List<StateMachineTransactionMapping>, StateMachineTransactionMapping>
}
