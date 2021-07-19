package net.corda.node.services.vault

import co.paralleluniverse.fibers.Suspendable
import co.paralleluniverse.strands.Strand
import net.corda.core.CordaRuntimeException
import net.corda.core.contracts.*
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.containsAny
import net.corda.core.flows.HospitalizeFlowException
import net.corda.core.internal.*
import net.corda.core.messaging.DataFeed
import net.corda.core.node.ServicesForResolution
import net.corda.core.node.StatesToRecord
import net.corda.core.node.services.*
import net.corda.core.node.services.Vault.ConstraintInfo.Companion.constraintInfo
import net.corda.core.node.services.vault.*
import net.corda.core.observable.internal.OnResilientSubscribe
import net.corda.core.schemas.PersistentStateRef
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.transactions.*
import net.corda.core.utilities.*
import net.corda.node.services.api.SchemaService
import net.corda.node.services.api.VaultServiceInternal
import net.corda.node.services.schema.PersistentStateService
import net.corda.node.services.statemachine.FlowStateMachineImpl
import net.corda.nodeapi.internal.persistence.*
import org.hibernate.Session
import rx.Observable
import rx.exceptions.OnErrorNotImplementedException
import rx.subjects.PublishSubject
import java.security.PublicKey
import java.sql.SQLException
import java.time.Clock
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArraySet
import javax.persistence.PersistenceException
import javax.persistence.Tuple
import javax.persistence.criteria.CriteriaBuilder
import javax.persistence.criteria.CriteriaUpdate
import javax.persistence.criteria.Predicate
import javax.persistence.criteria.Root

/**
 * The vault service handles storage, retrieval and querying of states.
 *
 * This class needs database transactions to be in-flight during method calls and init, and will throw exceptions if
 * this is not the case.
 *
 * TODO: keep an audit trail with time stamps of previously unconsumed states "as of" a particular point in time.
 * TODO: have transaction storage do some caching.
 */
class NodeVaultService(
        private val clock: Clock,
        private val keyManagementService: KeyManagementService,
        private val servicesForResolution: ServicesForResolution,
        private val database: CordaPersistence,
        private val schemaService: SchemaService,
        private val appClassloader: ClassLoader
) : SingletonSerializeAsToken(), VaultServiceInternal {
    companion object {
        private val log = contextLogger()

        const val DEFAULT_SOFT_LOCKING_SQL_IN_CLAUSE_SIZE = 16

        /**
         * Establish whether a given state is relevant to a node, given the node's public keys.
         *
         * A state is relevant if any of the participants (or the owner for ownable states) has an owning key matching one of this node's
         * public keys.
         */
        fun isRelevant(state: ContractState, myKeys: Set<PublicKey>): Boolean {
            val keysToCheck = when (state) {
                is OwnableState -> listOf(state.owner.owningKey)
                else -> state.participants.map { it.owningKey }
            }
            return keysToCheck.any { it.containsAny(myKeys) }
        }
    }

    private class InnerState {
        val _updatesPublisher = PublishSubject.create<Vault.Update<ContractState>>()!!
        val _rawUpdatesPublisher = PublishSubject.create<Vault.Update<ContractState>>()!!
        val _updatesInDbTx = _updatesPublisher.wrapWithDatabaseTransaction().asObservable()!!

        // For use during publishing only.
        val updatesPublisher: rx.Observer<Vault.Update<ContractState>> get() = _updatesPublisher.bufferUntilDatabaseCommit().tee(_rawUpdatesPublisher)
    }

    private val mutex = ThreadBox(InnerState())
    private val criteriaBuilder: CriteriaBuilder by lazy { database.hibernateConfig.sessionFactoryForRegisteredSchemas.criteriaBuilder }
    private val persistentStateService = PersistentStateService(schemaService)

    /**
     * Maintain a list of contract state interfaces to concrete types stored in the vault
     * for usage in generic queries of type queryBy<LinearState> or queryBy<FungibleState<*>>
     */
    @VisibleForTesting
    internal val contractStateTypeMappings = ConcurrentHashMap<String, MutableSet<String>>()

    override fun start() {
        bootstrapContractStateTypes()
        rawUpdates.subscribe { update ->
            (update.produced + update.references).forEach {
                val concreteType = it.state.data.javaClass
                log.trace { "State update of type: $concreteType" }
                val seen = contractStateTypeMappings.any { it.value.contains(concreteType.name) }
                if (!seen) {
                    val contractTypes = deriveContractTypes(concreteType)
                    contractTypes.map {
                        val contractStateType = contractStateTypeMappings.getOrPut(it.name) { CopyOnWriteArraySet() }
                        contractStateType.add(concreteType.name)
                    }
                }
            }
        }
    }

    private fun saveStates(session: Session, states: Map<StateRef, StateAndRef<ContractState>>) {
        states.forEach { stateAndRef ->
            val stateOnly = stateAndRef.value.state.data
            // TODO: Optimise this.
            //
            // For EVERY state to be committed to the vault, this checks whether it is spendable by the recording
            // node. The behaviour is as follows:
            //
            // 1) All vault updates marked as RELEVANT will, of course, all have relevancy_status = 1 in the
            //    "vault_states" table.
            // 2) For ALL_VISIBLE updates, those which are not relevant according to the relevancy rules will have
            //    relevancy_status = 0 in the "vault_states" table.
            //
            // This is useful when it comes to querying for fungible states, when we do not want irrelevant states
            // included in the result.
            //
            // The same functionality could be obtained by passing in a list of participants to the vault query,
            // however this:
            //
            // * requires a join on the participants table which results in slow queries
            // * states may flip from being non-relevant to relevant
            // * it's more complicated for CorDapp developers
            //
            // Adding a new column in the "VaultStates" table was considered the best approach.
            val keys = stateOnly.participants.map { it.owningKey }
            val persistentStateRef = PersistentStateRef(stateAndRef.key)
            // This check is done to set the "relevancyStatus". When one performs a vault query, it is possible to return ALL states, ONLY
            // RELEVANT states or NOT relevant states.
            val isRelevant = isRelevant(stateOnly, keyManagementService.filterMyKeys(keys).toSet())
            val constraintInfo = Vault.ConstraintInfo(stateAndRef.value.state.constraint)
            // Save a row for each party in the state_party table.
            // TODO: Perhaps these can be stored in a batch?
            stateOnly.participants.groupBy { it.owningKey }.forEach { participants ->
                val persistentParty = VaultSchemaV1.PersistentParty(persistentStateRef, participants.value.first())
                session.save(persistentParty)
            }
            val stateToAdd = VaultSchemaV1.VaultStates(
                    notary = stateAndRef.value.state.notary,
                    contractStateClassName = stateAndRef.value.state.data.javaClass.name,
                    stateStatus = Vault.StateStatus.UNCONSUMED,
                    recordedTime = clock.instant(),
                    relevancyStatus = if (isRelevant) Vault.RelevancyStatus.RELEVANT else Vault.RelevancyStatus.NOT_RELEVANT,
                    constraintType = constraintInfo.type(),
                    constraintData = constraintInfo.data()
            )
            stateToAdd.stateRef = persistentStateRef
            session.save(stateToAdd)
        }
    }

    private fun recordUpdate(update: Vault.Update<ContractState>): Vault.Update<ContractState> {
        if (!update.isEmpty()) {
            val producedStateRefs = update.produced.map { it.ref }
            val producedStateRefsMap = update.produced.associateBy { it.ref }
            val consumedStateRefs = update.consumed.map { it.ref }
            val referenceStateRefsMap = update.references.associateBy { it.ref }
            log.trace { "Removing $consumedStateRefs consumed contract states and adding $producedStateRefs produced contract states to the database." }

            val session = currentDBSession()

            // Persist the outputs.
            saveStates(session, producedStateRefsMap)

            // Persist the reference states.
            saveStates(session, referenceStateRefsMap)

            // Persist the consumed inputs.
            consumedStateRefs.forEach { stateRef ->
                val state = session.get<VaultSchemaV1.VaultStates>(VaultSchemaV1.VaultStates::class.java, PersistentStateRef(stateRef))
                state?.run {
                    // Only update the state if it has not previously been consumed (this could have happened if the transaction is being
                    // re-recorded.
                    if (stateStatus != Vault.StateStatus.CONSUMED) {
                        stateStatus = Vault.StateStatus.CONSUMED
                        consumedTime = clock.instant()
                        // remove lock (if held)
                        if (lockId != null) {
                            lockId = null
                            lockUpdateTime = clock.instant()
                            log.trace("Releasing soft lock on consumed state: $stateRef")
                        }
                        session.save(state)
                    }
                }
            }

        }
        return update
    }

    override val rawUpdates: Observable<Vault.Update<ContractState>>
        get() = mutex.locked {
            FlowStateMachineImpl.currentStateMachine()?.let {
                // we are inside a flow; we cannot allow flows to subscribe Rx Observers,
                // because the Observer could reference flow's properties, essentially fiber's properties then,
                // since it does not unsubscribe on flow's/ fiber's completion,
                // it could prevent the flow/ fiber -object- get garbage collected.
                log.error(
                    "Flow ${it.logic::class.java.name} tried to access VaultService.rawUpdates " +
                            "- Rx.Observables should only be accessed outside the context of a flow " +
                            "- aborting the flow "
                )

                throw CordaRuntimeException(
                    "Flow ${it.logic::class.java.name} tried to access VaultService.rawUpdates " +
                            "- Rx.Observables should only be accessed outside the context of a flow "
                )
            }
            // we are not inside a flow, we are most likely inside a CordaService;
            // we will expose, by default, subscribing of -non unsubscribing- rx.Observers to rawUpdates.
            return _rawUpdatesPublisher.resilientOnError()
        }

    override val updates: Observable<Vault.Update<ContractState>>
        get() = mutex.locked { _updatesInDbTx }

    @VisibleForTesting
    internal val publishUpdates get() = mutex.locked { updatesPublisher }

    /** Groups adjacent transactions into batches to generate separate net updates per transaction type. */
    override fun notifyAll(statesToRecord: StatesToRecord, txns: Iterable<CoreTransaction>, previouslySeenTxns: Iterable<CoreTransaction>) {
        if (statesToRecord == StatesToRecord.NONE || (!txns.any() && !previouslySeenTxns.any())) return
        val batch = mutableListOf<CoreTransaction>()

        fun flushBatch(previouslySeen: Boolean) {
            val updates = makeUpdates(batch, statesToRecord, previouslySeen)
            processAndNotify(updates)
            batch.clear()
        }

        fun processTransactions(txs: Iterable<CoreTransaction>, previouslySeen: Boolean) {
            for (tx in txs) {
                if (batch.isNotEmpty() && tx.javaClass != batch.last().javaClass) {
                    flushBatch(previouslySeen)
                }
                batch.add(tx)
            }
            flushBatch(previouslySeen)
        }

        processTransactions(previouslySeenTxns, true)
        processTransactions(txns, false)
    }

    private fun makeUpdates(batch: Iterable<CoreTransaction>, statesToRecord: StatesToRecord, previouslySeen: Boolean): List<Vault.Update<ContractState>> {

        fun <T> withValidDeserialization(list: List<T>, txId: SecureHash): Map<Int, T> = (0 until list.size).mapNotNull { idx ->
            try {
                idx to list[idx]
            } catch (e: TransactionDeserialisationException) {
                // When resolving transaction dependencies we might encounter contracts we haven't installed locally.
                // This will cause a failure as we can't deserialize such states in the context of the `appClassloader`.
                // For now we ignore these states.
                // In the future we will use the AttachmentsClassloader to correctly deserialize and asses the relevancy.
                log.debug { "Could not deserialize state $idx from transaction $txId. Cause: $e" }
                null
            }
        }.toMap()

        // Returns only output states that can be deserialised successfully.
        fun WireTransaction.deserializableOutputStates(): Map<Int, TransactionState<ContractState>> = withValidDeserialization(this.outputs, this.id)

        // Returns only reference states that can be deserialised successfully.
        fun LedgerTransaction.deserializableRefStates(): Map<Int, StateAndRef<ContractState>> = withValidDeserialization(this.references, this.id)

        fun makeUpdate(tx: WireTransaction): Vault.Update<ContractState>? {
            val outputs: Map<Int, TransactionState<ContractState>> = tx.deserializableOutputStates()
            val ourNewStates = when (statesToRecord) {
                StatesToRecord.NONE -> throw AssertionError("Should not reach here")
                StatesToRecord.ONLY_RELEVANT -> outputs.filter { (_, value) ->
                    isRelevant(value.data, keyManagementService.filterMyKeys(outputs.values.flatMap { it.data.participants.map { it.owningKey } }).toSet())
                }
                StatesToRecord.ALL_VISIBLE -> if (previouslySeen) {
                    // For transactions being re-recorded, the node must check its vault to find out what states it has already seen. Note
                    // that some of the outputs previously seen may have been consumed in the meantime, so the check must look for all state
                    // statuses.
                    val outputRefs = tx.outRefsOfType<ContractState>().map { it.ref }
                    val seenRefs = loadStates(outputRefs).map { it.ref }
                    val unseenRefs = outputRefs - seenRefs
                    val unseenOutputIdxs = unseenRefs.map { it.index }.toSet()
                    outputs.filter { it.key in unseenOutputIdxs }
                } else {
                    outputs
                }
            }.map { (idx, _) -> tx.outRef<ContractState>(idx) }

            // Retrieve all unconsumed states for this transaction's inputs.
            val consumedStates = loadStates(tx.inputs)

            // Is transaction irrelevant? If so, then we don't care about the reference states either.
            if (consumedStates.isEmpty() && ourNewStates.isEmpty()) {
                log.trace { "tx ${tx.id} was irrelevant to this vault, ignoring" }
                return null
            }

            // This list should only contain NEW states which we have not seen before as an output in another transaction. If we can't
            // obtain the references from the vault then the reference must be a state we have not seen before, therefore we should store it
            // in the vault. If StateToRecord is set to ALL_VISIBLE or ONLY_RELEVANT then we should store all of the previously unseen
            // states in the reference list. The assumption is that we might need to inspect them at some point if they were referred to
            // in the contracts of the input or output states. If states to record is none then we shouldn't record any reference states.
            val newReferenceStateAndRefs = if (tx.references.isEmpty()) {
                emptyList()
            } else {
                when (statesToRecord) {
                    StatesToRecord.NONE -> throw AssertionError("Should not reach here")
                    StatesToRecord.ALL_VISIBLE, StatesToRecord.ONLY_RELEVANT -> {
                        val notSeenReferences = tx.references - loadStates(tx.references).map { it.ref }
                        // TODO: This is expensive - is there another way?
                        tx.toLedgerTransaction(servicesForResolution).deserializableRefStates()
                                .filter { (_, stateAndRef) -> stateAndRef.ref in notSeenReferences }
                                .values
                    }
                }
            }

            return Vault.Update(consumedStates.toSet(), ourNewStates.toSet(), references = newReferenceStateAndRefs.toSet())
        }

        fun resolveAndMakeUpdate(tx: CoreTransaction): Vault.Update<ContractState>? {
            // We need to resolve the full transaction here because outputs are calculated from inputs
            // We also can't do filtering beforehand, since for notary change transactions output encumbrance pointers
            // get recalculated based on input positions.
            val ltx: FullTransaction = when (tx) {
                is NotaryChangeWireTransaction -> tx.resolve(servicesForResolution, emptyList())
                is ContractUpgradeWireTransaction -> tx.resolve(servicesForResolution, emptyList())
                else -> throw IllegalArgumentException("Unsupported transaction type: ${tx.javaClass.name}")
            }
            val myKeys by lazy { keyManagementService.filterMyKeys(ltx.outputs.flatMap { it.data.participants.map { it.owningKey } }) }
            val (consumedStateAndRefs, producedStates) = ltx.inputs.zip(ltx.outputs).filter { (_, output) ->
                if (statesToRecord == StatesToRecord.ONLY_RELEVANT) {
                    isRelevant(output.data, myKeys.toSet())
                } else {
                    true
                }
            }.unzip()

            val producedStateAndRefs = producedStates.map { ltx.outRef<ContractState>(it.data) }
            if (consumedStateAndRefs.isEmpty() && producedStateAndRefs.isEmpty()) {
                log.trace { "tx ${tx.id} was irrelevant to this vault, ignoring" }
                return null
            }

            val referenceStateAndRefs = ltx.references

            val updateType = if (tx is ContractUpgradeWireTransaction) {
                Vault.UpdateType.CONTRACT_UPGRADE
            } else {
                Vault.UpdateType.NOTARY_CHANGE
            }
            return Vault.Update(consumedStateAndRefs.toSet(), producedStateAndRefs.toSet(), null, updateType, referenceStateAndRefs.toSet())
        }


        return batch.mapNotNull {
            if (it is WireTransaction) makeUpdate(it) else resolveAndMakeUpdate(it)
        }
    }

    private fun loadStates(refs: Collection<StateRef>): Collection<StateAndRef<ContractState>> {
        val states = mutableListOf<StateAndRef<ContractState>>()
        if (refs.isNotEmpty()) {
            val refsList = refs.toList()
            val pageSize = PageSpecification().pageSize
            (0..(refsList.size - 1) / pageSize).forEach {
                val offset = it * pageSize
                val limit = minOf(offset + pageSize, refsList.size)
                val page = queryBy<ContractState>(QueryCriteria.VaultQueryCriteria(
                        stateRefs = refsList.subList(offset, limit),
                        status = Vault.StateStatus.ALL)).states
                states.addAll(page)
            }
        }
        return states
    }

    private fun processAndNotify(updates: List<Vault.Update<ContractState>>) {
        if (updates.isEmpty()) return
        val netUpdate = updates.reduce { update1, update2 -> update1 + update2 }
        if (!netUpdate.isEmpty()) {
            recordUpdate(netUpdate)
            mutex.locked {
                // flowId was required by SoftLockManager to perform auto-registration of soft locks for new states
                val uuid = (Strand.currentStrand() as? FlowStateMachineImpl<*>)?.id?.uuid
                val vaultUpdate = if (uuid != null) netUpdate.copy(flowId = uuid) else netUpdate
                if (uuid != null) {
                    val fungible = netUpdate.produced.filter { stateAndRef ->
                        val state = stateAndRef.state.data
                        state is FungibleAsset<*> || state is FungibleState<*>
                    }
                    if (fungible.isNotEmpty()) {
                        val stateRefs = fungible.map { it.ref }.toNonEmptySet()
                        log.trace { "Reserving soft locks for flow id $uuid and states $stateRefs" }
                        softLockReserve(uuid, stateRefs)
                    }
                }
                persistentStateService.persist(vaultUpdate.produced + vaultUpdate.references)
                try {
                    updatesPublisher.onNext(vaultUpdate)
                } catch (e: Exception) {
                    // exception thrown here will cause the recording of transaction states to the vault being rolled back
                    // it could cause the ledger go into an inconsistent state, therefore we should hospitalise this flow
                    // observer code should either be fixed or ignored and have the flow retry from previous checkpoint
                    log.error(
                        "Failed to record transaction states locally " +
                                "- the node could be now in an inconsistent state with other peers and/or the notary " +
                                "- hospitalising the flow ", e
                    )

                    throw (e as? OnErrorNotImplementedException)?.let {
                        it.cause?.let { wrapped ->
                            if (wrapped is SQLException || wrapped is PersistenceException) {
                                wrapped
                            } else {
                                HospitalizeFlowException(wrapped)
                            }
                        }
                    } ?: (e as? SQLException ?: (e as? PersistenceException ?: HospitalizeFlowException(e)))
                }
            }
        }
    }

    override fun addNoteToTransaction(txnId: SecureHash, noteText: String) {
        database.transaction {
            val txnNoteEntity = VaultSchemaV1.VaultTxnNote(txnId.toString(), noteText)
            currentDBSession().save(txnNoteEntity)
        }
    }

    override fun getTransactionNotes(txnId: SecureHash): Iterable<String> {
        return database.transaction {
            val session = currentDBSession()
            val criteriaBuilder = session.criteriaBuilder
            val criteriaQuery = criteriaBuilder.createQuery(VaultSchemaV1.VaultTxnNote::class.java)
            val vaultStates = criteriaQuery.from(VaultSchemaV1.VaultTxnNote::class.java)
            val txIdPredicate = criteriaBuilder.equal(vaultStates.get<Vault.StateStatus>(VaultSchemaV1.VaultTxnNote::txId.name), txnId.toString())
            criteriaQuery.where(txIdPredicate)
            val results = session.createQuery(criteriaQuery).resultList
            results.asIterable().map { it.note ?: "" }
        }
    }

    /**
     * Whenever executed inside a [FlowStateMachineImpl], if [lockId] refers to the currently running [FlowStateMachineImpl],
     * then in that case the [FlowStateMachineImpl] instance is locking states with its [FlowStateMachineImpl.id]'s [UUID].
     * In this case alone, we keep the reserved set of [StateRef] in [FlowStateMachineImpl.softLockedStates]. This set will be then
     * used by default in [softLockRelease].
     */
    @Suppress("NestedBlockDepth", "ComplexMethod")
    @Throws(StatesNotAvailableException::class)
    override fun softLockReserve(lockId: UUID, stateRefs: NonEmptySet<StateRef>) {
        val softLockTimestamp = clock.instant()
        try {
            val session = currentDBSession()
            val criteriaBuilder = session.criteriaBuilder
            fun execute(configure: Root<*>.(CriteriaUpdate<*>, Array<Predicate>) -> Any?) = criteriaBuilder.executeUpdate(session, null) { update, _ ->
                val persistentStateRefs = stateRefs.map { PersistentStateRef(it.txhash.toString(), it.index) }
                val compositeKey = get<PersistentStateRef>(VaultSchemaV1.VaultStates::stateRef.name)
                val stateRefsPredicate = criteriaBuilder.and(compositeKey.`in`(persistentStateRefs))
                configure(update, arrayOf(stateRefsPredicate))
            }

            val updatedRows = execute { update, commonPredicates ->
                val stateStatusPredication = criteriaBuilder.equal(get<Vault.StateStatus>(VaultSchemaV1.VaultStates::stateStatus.name), Vault.StateStatus.UNCONSUMED)
                val lockIdPredicate = criteriaBuilder.or(get<String>(VaultSchemaV1.VaultStates::lockId.name).isNull,
                        criteriaBuilder.equal(get<String>(VaultSchemaV1.VaultStates::lockId.name), lockId.toString()))
                update.set(get<String>(VaultSchemaV1.VaultStates::lockId.name), lockId.toString())
                update.set(get<Instant>(VaultSchemaV1.VaultStates::lockUpdateTime.name), softLockTimestamp)
                update.where(stateStatusPredication, lockIdPredicate, *commonPredicates)
            }
            if (updatedRows > 0 && updatedRows == stateRefs.size) {
                log.trace { "Reserving soft lock states for $lockId: $stateRefs" }
                FlowStateMachineImpl.currentStateMachine()?.let {
                    if (lockId == it.id.uuid) {
                        it.softLockedStates.addAll(stateRefs)
                    }
                }
            } else {
                // revert partial soft locks
                val revertUpdatedRows = execute { update, commonPredicates ->
                    val lockIdPredicate = criteriaBuilder.equal(get<String>(VaultSchemaV1.VaultStates::lockId.name), lockId.toString())
                    val lockUpdateTime = criteriaBuilder.equal(get<Instant>(VaultSchemaV1.VaultStates::lockUpdateTime.name), softLockTimestamp)
                    update.set(get<String>(VaultSchemaV1.VaultStates::lockId.name), criteriaBuilder.nullLiteral(String::class.java))
                    update.where(lockUpdateTime, lockIdPredicate, *commonPredicates)
                }
                if (revertUpdatedRows > 0) {
                    log.trace { "Reverting $revertUpdatedRows partially soft locked states for $lockId" }
                }
                throw StatesNotAvailableException("Attempted to reserve $stateRefs for $lockId but only $updatedRows rows available")
            }
        } catch (e: Exception) {
            log.error("""soft lock update error attempting to reserve states for $lockId and $stateRefs")
                    $e.
                """)
            if (e.cause is StatesNotAvailableException) throw (e.cause as StatesNotAvailableException)
            throw e
        }
    }

    /**
     * Whenever executed inside a [FlowStateMachineImpl], if [lockId] refers to the currently running [FlowStateMachineImpl] and [stateRefs] is null,
     * then in that case the [FlowStateMachineImpl] instance will, by default, retrieve its set of [StateRef]
     * from [FlowStateMachineImpl.softLockedStates] (previously reserved from [softLockReserve]). This set will be then explicitly provided
     * to the below query which then leads to the database query optimizer use the primary key index in VAULT_STATES table, instead of lock_id_idx
     * in order to search rows to be updated. That way the query will be aligned with the rest of the queries that are following that route as well
     * (i.e. making use of the primary key), and therefore its locking order of resources within the database will be aligned
     * with the rest queries' locking orders (solving SQL deadlocks).
     *
     * If [lockId] does not refer to the currently running [FlowStateMachineImpl] and [stateRefs] is null, then it will be using only [lockId] in
     * the below query.
     */
    @Suppress("NestedBlockDepth", "ComplexMethod")
    override fun softLockRelease(lockId: UUID, stateRefs: NonEmptySet<StateRef>?) {
        val softLockTimestamp = clock.instant()
        val session = currentDBSession()
        val criteriaBuilder = session.criteriaBuilder
        fun execute(stateRefs: NonEmptySet<StateRef>?, configure: Root<*>.(CriteriaUpdate<*>, Array<Predicate>, List<PersistentStateRef>?) -> Any?) =
            criteriaBuilder.executeUpdate(session, stateRefs) { update, persistentStateRefs ->
            val stateStatusPredication = criteriaBuilder.equal(get<Vault.StateStatus>(VaultSchemaV1.VaultStates::stateStatus.name), Vault.StateStatus.UNCONSUMED)
            val lockIdPredicate = criteriaBuilder.equal(get<String>(VaultSchemaV1.VaultStates::lockId.name), lockId.toString())
            update.set<String>(get<String>(VaultSchemaV1.VaultStates::lockId.name), criteriaBuilder.nullLiteral(String::class.java))
            update.set(get<Instant>(VaultSchemaV1.VaultStates::lockUpdateTime.name), softLockTimestamp)
            configure(update, arrayOf(stateStatusPredication, lockIdPredicate), persistentStateRefs)
        }

        val stateRefsToBeReleased =
            stateRefs ?: FlowStateMachineImpl.currentStateMachine()?.let {
                // We only hold states under our flowId. For all other lockId fall back to old query mechanism, i.e. stateRefsToBeReleased = null
                if (lockId == it.id.uuid && it.softLockedStates.isNotEmpty()) {
                    NonEmptySet.copyOf(it.softLockedStates)
                } else {
                    null
                }
            }

        if (stateRefsToBeReleased == null) {
            val update = execute(null) { update, commonPredicates, _ ->
                update.where(*commonPredicates)
            }
            if (update > 0) {
                log.trace { "Releasing $update soft locked states for $lockId" }
            }
        } else {
            try {
                val updatedRows = execute(stateRefsToBeReleased) { update, commonPredicates, persistentStateRefs  ->
                    val compositeKey = get<PersistentStateRef>(VaultSchemaV1.VaultStates::stateRef.name)
                    val stateRefsPredicate = criteriaBuilder.and(compositeKey.`in`(persistentStateRefs))
                    update.where(*commonPredicates, stateRefsPredicate)
                }
                if (updatedRows > 0) {
                    FlowStateMachineImpl.currentStateMachine()?.let {
                        if (lockId == it.id.uuid) {
                            it.softLockedStates.removeAll(stateRefsToBeReleased)
                        }
                    }
                    log.trace { "Releasing $updatedRows soft locked states for $lockId and stateRefs $stateRefsToBeReleased" }
                }
            } catch (e: Exception) {
                log.error("Soft lock update error attempting to release states for $lockId and $stateRefsToBeReleased", e)
                throw e
            }
        }
    }

    @Suspendable
    @Throws(StatesNotAvailableException::class)
    override fun <T : FungibleState<*>> tryLockFungibleStatesForSpending(
            lockId: UUID,
            eligibleStatesQuery: QueryCriteria,
            amount: Amount<*>,
            contractStateType: Class<out T>
    ): List<StateAndRef<T>> {
        if (amount.quantity == 0L) {
            return emptyList()
        }

        // Helper to unwrap the token from the Issued object if one exists.
        fun unwrapIssuedAmount(amount: Amount<*>): Any {
            val token = amount.token
            return when (token) {
                is Issued<*> -> token.product
                else -> token
            }
        }

        val unwrappedToken = unwrapIssuedAmount(amount)

        // Enrich QueryCriteria with additional default attributes (such as soft locks).
        // We only want to return RELEVANT states here.
        val sortAttribute = SortAttribute.Standard(Sort.CommonStateAttribute.STATE_REF)
        val sorter = Sort(setOf(Sort.SortColumn(sortAttribute, Sort.Direction.ASC)))
        val enrichedCriteria = QueryCriteria.VaultQueryCriteria(
                contractStateTypes = setOf(contractStateType),
                softLockingCondition = QueryCriteria.SoftLockingCondition(QueryCriteria.SoftLockingType.UNLOCKED_AND_SPECIFIED, listOf(lockId)),
                relevancyStatus = Vault.RelevancyStatus.RELEVANT
        )
        val results = queryBy(contractStateType, enrichedCriteria.and(eligibleStatesQuery), sorter)

        var claimedAmount = 0L
        val claimedStates = mutableListOf<StateAndRef<T>>()
        for (state in results.states) {
            // This method handles Amount<Issued<T>> in FungibleAsset and Amount<T> in FungibleState.
            val issuedAssetToken = unwrapIssuedAmount(state.state.data.amount)

            if (issuedAssetToken == unwrappedToken) {
                claimedStates += state
                claimedAmount += state.state.data.amount.quantity
                if (claimedAmount > amount.quantity) {
                    break
                }
            }
        }
        if (claimedStates.isEmpty() || claimedAmount < amount.quantity) {
            return emptyList()
        }
        softLockReserve(lockId, claimedStates.map { it.ref }.toNonEmptySet())
        return claimedStates
    }

    @Throws(VaultQueryException::class)
    override fun <T : ContractState> _queryBy(criteria: QueryCriteria, paging: PageSpecification, sorting: Sort, contractStateType: Class<out T>): Vault.Page<T> {
        try {
            return _queryBy(criteria, paging, sorting, contractStateType, false)
        } catch (e: VaultQueryException) {
            throw e
        } catch (e: Exception) {
            throw VaultQueryException("An error occurred while attempting to query the vault: ${e.message}", e)
        }
    }

    @Throws(VaultQueryException::class)
    private fun <T : ContractState> _queryBy(criteria: QueryCriteria, paging_: PageSpecification, sorting: Sort, contractStateType: Class<out T>, skipPagingChecks: Boolean): Vault.Page<T> {
        // We decrement by one if the client requests MAX_PAGE_SIZE, assuming they can not notice this because they don't have enough memory
        // to request `MAX_PAGE_SIZE` states at once.
        val paging = if (paging_.pageSize == Integer.MAX_VALUE) {
            paging_.copy(pageSize = Integer.MAX_VALUE - 1)
        } else {
            paging_
        }
        log.debug { "Vault Query for contract type: $contractStateType, criteria: $criteria, pagination: $paging, sorting: $sorting" }
        return database.transaction {
            // calculate total results where a page specification has been defined
            var totalStates = -1L
            if (!skipPagingChecks && !paging.isDefault) {
                val count = builder { VaultSchemaV1.VaultStates::recordedTime.count() }
                val countCriteria = QueryCriteria.VaultCustomQueryCriteria(count, Vault.StateStatus.ALL)
                val results = _queryBy(criteria.and(countCriteria), PageSpecification(), Sort(emptyList()), contractStateType, true)  // only skip pagination checks for total results count query
                totalStates = results.otherResults.last() as Long
            }

            val session = getSession()

            val criteriaQuery = criteriaBuilder.createQuery(Tuple::class.java)
            val queryRootVaultStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)

            // TODO: revisit (use single instance of parser for all queries)
            val criteriaParser = HibernateQueryCriteriaParser(contractStateType, contractStateTypeMappings, criteriaBuilder, criteriaQuery, queryRootVaultStates)

            // parse criteria and build where predicates
            criteriaParser.parse(criteria, sorting)

            // prepare query for execution
            val query = session.createQuery(criteriaQuery)

            // pagination checks
            if (!skipPagingChecks && !paging.isDefault) {
                // pagination
                if (paging.pageNumber < DEFAULT_PAGE_NUM) throw VaultQueryException("Page specification: invalid page number ${paging.pageNumber} [page numbers start from $DEFAULT_PAGE_NUM]")
                if (paging.pageSize < 1) throw VaultQueryException("Page specification: invalid page size ${paging.pageSize} [minimum is 1]")
                if (paging.pageSize > MAX_PAGE_SIZE) throw VaultQueryException("Page specification: invalid page size ${paging.pageSize} [maximum is $MAX_PAGE_SIZE]")
            }

            // For both SQLServer and PostgresSQL, firstResult must be >= 0. So we set a floor at 0.
            // TODO: This is a catch-all solution. But why is the default pageNumber set to be -1 in the first place?
            // Even if we set the default pageNumber to be 1 instead, that may not cover the non-default cases.
            // So the floor may be necessary anyway.
            query.firstResult = maxOf(0, (paging.pageNumber - 1) * paging.pageSize)
            val pageSize = paging.pageSize + 1
            query.maxResults = if (pageSize > 0) pageSize else Integer.MAX_VALUE // detection too many results, protected against overflow

            // execution
            val results = query.resultList

            // final pagination check (fail-fast on too many results when no pagination specified)
            if (!skipPagingChecks && paging.isDefault && results.size > DEFAULT_PAGE_SIZE) {
                throw VaultQueryException("There are ${results.size} results, which exceeds the limit of $DEFAULT_PAGE_SIZE for queries that do not specify paging. In order to retrieve these results, provide a `PageSpecification(pageNumber, pageSize)` to the method invoked.")
            }
            val statesAndRefs: MutableList<StateAndRef<T>> = mutableListOf()
            val statesMeta: MutableList<Vault.StateMetadata> = mutableListOf()
            val otherResults: MutableList<Any> = mutableListOf()
            val stateRefs = mutableSetOf<StateRef>()

            results.asSequence()
                    .forEachIndexed { index, result ->
                        if (result[0] is VaultSchemaV1.VaultStates) {
                            if (!paging.isDefault && index == paging.pageSize) // skip last result if paged
                                return@forEachIndexed
                            val vaultState = result[0] as VaultSchemaV1.VaultStates
                            val stateRef = StateRef(SecureHash.create(vaultState.stateRef!!.txId), vaultState.stateRef!!.index)
                            stateRefs.add(stateRef)
                            statesMeta.add(Vault.StateMetadata(stateRef,
                                    vaultState.contractStateClassName,
                                    vaultState.recordedTime,
                                    vaultState.consumedTime,
                                    vaultState.stateStatus,
                                    vaultState.notary,
                                    vaultState.lockId,
                                    vaultState.lockUpdateTime,
                                    vaultState.relevancyStatus,
                                    constraintInfo(vaultState.constraintType, vaultState.constraintData)
                            ))
                        } else {
                            // TODO: improve typing of returned other results
                            log.debug { "OtherResults: ${Arrays.toString(result.toArray())}" }
                            otherResults.addAll(result.toArray().asList())
                        }
                    }
            if (stateRefs.isNotEmpty())
                statesAndRefs.addAll(uncheckedCast(servicesForResolution.loadStates(stateRefs)))

            Vault.Page(states = statesAndRefs, statesMetadata = statesMeta, stateTypes = criteriaParser.stateTypes, totalStatesAvailable = totalStates, otherResults = otherResults)
        }
    }

    /**
     * Returns a [DataFeed] containing the results of the provided query, along with the associated observable, containing any subsequent updates.
     *
     * Note that this method can be invoked concurrently with [NodeVaultService.notifyAll], which means there could be race conditions between reads
     * performed here and writes performed there. These are prevented, using the following approach:
     * - Observable updates emitted by [NodeVaultService.notifyAll] are buffered until the transaction's commit point
     *   This means that it's as if publication is performed, after the transaction is committed.
     * - Observable updates tracked by [NodeVaultService._trackBy] are buffered before the transaction (for the provided query) is open
     *   and until the client's subscription. So, it's as if the customer is subscribed to the observable before the read's transaction is open.
     *
     * The combination of the 2 conditions described above guarantee that there can be no possible interleaving, where some states are not observed in the query
     * (i.e. because read transaction opens, before write transaction is closed) and at the same time not included in the observable (i.e. because subscription
     * is done before the publication of updates). However, this guarantee cannot be provided, in cases where the client invokes [VaultService.trackBy] with an open
     * transaction.
     */
    @Throws(VaultQueryException::class)
    override fun <T : ContractState> _trackBy(criteria: QueryCriteria, paging: PageSpecification, sorting: Sort, contractStateType: Class<out T>): DataFeed<Vault.Page<T>, Vault.Update<T>> {
        return mutex.locked {
            val updates: Observable<Vault.Update<T>> = uncheckedCast(_updatesPublisher.bufferUntilSubscribed())
            if (contextTransactionOrNull != null) {
                log.warn("trackBy is called with an already existing, open DB transaction. As a result, there might be states missing from both the snapshot and observable, included in the returned data feed, because of race conditions.")
            }
            val snapshotResults = _queryBy(criteria, paging, sorting, contractStateType)
            val snapshotStatesRefs = snapshotResults.statesMetadata.map { it.ref }.toSet()
            val snapshotConsumedStatesRefs = snapshotResults.statesMetadata.filter { it.consumedTime != null }
                    .map { it.ref }.toSet()
            val filteredUpdates = updates.filter { it.containsType(contractStateType, snapshotResults.stateTypes) }
                    .map { filterContractStates(it, contractStateType) }
                    .filter { !hasBeenSeen(it, snapshotStatesRefs, snapshotConsumedStatesRefs) }

            DataFeed(snapshotResults, filteredUpdates)
        }
    }

    private fun <T : ContractState> filterContractStates(update: Vault.Update<T>, contractStateType: Class<out T>) =
            update.copy(consumed = filterByContractState(contractStateType, update.consumed),
                    produced = filterByContractState(contractStateType, update.produced))

    private fun <T : ContractState> filterByContractState(contractStateType: Class<out T>, stateAndRefs: Set<StateAndRef<T>>) =
            stateAndRefs.filter { contractStateType.isAssignableFrom(it.state.data.javaClass) }.toSet()

    /**
     * Filters out updates that have been seen, aka being reflected in the query's result snapshot.
     *
     * An update is reflected in the snapshot, if both of the following conditions hold:
     * - all the states produced by the update are included in the snapshot (regardless of whether they are consumed).
     * - all the states consumed by the update are included in the snapshot, AND they are consumed.
     *
     * Note: An update can contain multiple transactions (with netting performed on them). As a result, some of these transactions
     *       can be included in the snapshot result, while some are not. In this case, since we are not capable of reverting the netting and doing
     *       partial exclusion, we decide to return some more updates, instead of losing them completely (not returning them either in
     *       the snapshot or in the observable).
     */
    private fun <T: ContractState> hasBeenSeen(update: Vault.Update<T>, snapshotStatesRefs: Set<StateRef>, snapshotConsumedStatesRefs: Set<StateRef>): Boolean {
        val updateProducedStatesRefs = update.produced.map { it.ref }.toSet()
        val updateConsumedStatesRefs = update.consumed.map { it.ref }.toSet()

        return snapshotStatesRefs.containsAll(updateProducedStatesRefs) && snapshotConsumedStatesRefs.containsAll(updateConsumedStatesRefs)
    }

    private fun getSession() = database.currentOrNew().session
    /**
     * Derive list from existing vault states and then incrementally update using vault observables
     */
    private fun bootstrapContractStateTypes() {
        val criteria = criteriaBuilder.createQuery(String::class.java)
        val vaultStates = criteria.from(VaultSchemaV1.VaultStates::class.java)
        criteria.select(vaultStates.get("contractStateClassName")).distinct(true)
        val session = getSession()

        val query = session.createQuery(criteria)
        val results = query.resultList
        val distinctTypes = results.map { it }

        val unknownTypes = mutableSetOf<String>()
        distinctTypes.forEach { type ->
            val concreteType: Class<ContractState>? = try {
                uncheckedCast(Class.forName(type, true, appClassloader))
            } catch (e: ClassNotFoundException) {
                unknownTypes += type
                null
            }
            concreteType?.let {
                val contractTypes = deriveContractTypes(it)
                contractTypes.map {
                    val contractStateType = contractStateTypeMappings.getOrPut(it.name) { CopyOnWriteArraySet() }
                    contractStateType.add(concreteType.name)
                }
            }
        }
        if (unknownTypes.isNotEmpty()) {
            log.warn("There are unknown contract state types in the vault, which will prevent these states from being used. The relevant CorDapps must be loaded for these states to be used. The types not on the classpath are ${unknownTypes.joinToString(", ", "[", "]")}.")
        }
    }

    private fun <T : ContractState> deriveContractTypes(clazz: Class<T>): Set<Class<T>> {
        val myTypes : MutableSet<Class<T>> = mutableSetOf()
        clazz.superclass?.let {
            if (!it.isInstance(Any::class)) {
                myTypes.add(uncheckedCast(it))
                myTypes.addAll(deriveContractTypes(uncheckedCast(it)))
            }
        }
        clazz.interfaces.forEach {
            if (it != ContractState::class.java) {
                myTypes.add(uncheckedCast(it))
                myTypes.addAll(deriveContractTypes(uncheckedCast(it)))
            }
        }
        return myTypes
    }
}

private fun CriteriaBuilder.executeUpdate(
    session: Session,
    stateRefs: NonEmptySet<StateRef>?,
    configure: Root<*>.(CriteriaUpdate<*>, List<PersistentStateRef>?) -> Any?
): Int {
    fun doUpdate(persistentStateRefs: List<PersistentStateRef>?): Int {
        createCriteriaUpdate(VaultSchemaV1.VaultStates::class.java).let { update ->
            update.from(VaultSchemaV1.VaultStates::class.java).run { configure(update, persistentStateRefs) }
            return session.createQuery(update).executeUpdate()
        }
    }
    return stateRefs?.let {
        // Increase SQL server performance by, processing updates in chunks allowing the database's optimizer to make use of the index.
        var updatedRows = 0
        it.asSequence()
            .map { stateRef -> PersistentStateRef(stateRef.txhash.toString(), stateRef.index) }
            .chunked(NodeVaultService.DEFAULT_SOFT_LOCKING_SQL_IN_CLAUSE_SIZE)
            .forEach { persistentStateRefs ->
                updatedRows += doUpdate(persistentStateRefs)
            }
        updatedRows
    } ?: doUpdate(null)
}

/** The Observable returned allows subscribing with custom SafeSubscribers to source [Observable]. */
internal fun<T> Observable<T>.resilientOnError(): Observable<T> = Observable.unsafeCreate(OnResilientSubscribe(this, false))