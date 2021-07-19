package net.corda.core.node.services

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.DeleteForDJVM
import net.corda.core.DoNotImplement
import net.corda.core.concurrent.CordaFuture
import net.corda.core.contracts.*
import net.corda.core.crypto.Crypto
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.FlowException
import net.corda.core.flows.FlowLogic
import net.corda.core.identity.AbstractParty
import net.corda.core.internal.MAX_NUMBER_OF_KEYS_IN_SIGNATURE_CONSTRAINT
import net.corda.core.internal.concurrent.doneFuture
import net.corda.core.messaging.DataFeed
import net.corda.core.node.services.Vault.RelevancyStatus.*
import net.corda.core.node.services.Vault.StateStatus
import net.corda.core.node.services.vault.*
import net.corda.core.serialization.CordaSerializable
import net.corda.core.toFuture
import net.corda.core.transactions.LedgerTransaction
import net.corda.core.utilities.NonEmptySet
import net.corda.core.utilities.toHexString
import rx.Observable
import java.time.Instant
import java.util.*

/**
 * A vault (name may be temporary) wraps a set of states that are useful for us to keep track of, for instance,
 * because we own them. This class represents an immutable, stable state of a vault: it is guaranteed not to
 * change out from underneath you, even though the canonical currently-best-known vault may change as we learn
 * about new transactions from our peers and generate new transactions that consume states ourselves.
 *
 * This abstract class has no references to Cash contracts.
 *
 * [states] Holds a [VaultService] queried subset of states that are *active* and *relevant*.
 *   Active means they haven't been consumed yet (or we don't know about it).
 *   Relevant means they contain at least one of our pubkeys.
 */
@CordaSerializable
class Vault<out T : ContractState>(val states: Iterable<StateAndRef<T>>) {
    /**
     * Represents an update observed by the vault that will be notified to observers.  Include the [StateRef]s of
     * transaction outputs that were consumed (inputs) and the [ContractState]s produced (outputs) to/by the transaction
     * or transactions observed and the vault.
     *
     * If the vault observes multiple transactions simultaneously, where some transactions consume the outputs of some of the
     * other transactions observed, then the changes are observed "net" of those.
     */
    @CordaSerializable
    data class Update<U : ContractState> @JvmOverloads constructor(
            val consumed: Set<StateAndRef<U>>,
            val produced: Set<StateAndRef<U>>,
            val flowId: UUID? = null,
            /**
             * Specifies the type of update, currently supported types are general and, contract upgrade and notary change.
             * Notary change transactions only modify the notary field on states, and potentially need to be handled
             * differently.
             */
            val type: UpdateType = UpdateType.GENERAL,
            val references: Set<StateAndRef<U>> = emptySet()
    ) {
        /** Checks whether the update contains a state of the specified type. */
        inline fun <reified T : ContractState> containsType() = consumed.any { it.state.data is T } || produced.any { it.state.data is T } || references.any { it.state.data is T }

        /** Checks whether the update contains a state of the specified type and state status */
        fun <T : ContractState> containsType(clazz: Class<T>, status: StateStatus) =
                when (status) {
                    StateStatus.UNCONSUMED -> produced.any { clazz.isAssignableFrom(it.state.data.javaClass) }
                    StateStatus.CONSUMED -> consumed.any { clazz.isAssignableFrom(it.state.data.javaClass) }
                    else -> consumed.any { clazz.isAssignableFrom(it.state.data.javaClass) }
                            || produced.any { clazz.isAssignableFrom(it.state.data.javaClass) }
                }

        fun isEmpty() = consumed.isEmpty() && produced.isEmpty()

        /**
         * Combine two updates into a single update with the combined inputs and outputs of the two updates but net
         * any outputs of the left-hand-side (this) that are consumed by the inputs of the right-hand-side (rhs).
         *
         * i.e. the net effect in terms of state live-ness of receiving the combined update is the same as receiving this followed by rhs.
         */
        operator fun plus(rhs: Update<U>): Update<U> {
            require(rhs.type == type) { "Cannot combine updates of different types" }
            val combinedConsumed = consumed + (rhs.consumed - produced)
            // The ordering below matters to preserve ordering of consumed/produced Sets when they are insertion order dependent implementations.
            val combinedProduced = produced.filter { it !in rhs.consumed }.toSet() + rhs.produced
            return copy(consumed = combinedConsumed, produced = combinedProduced, references = references + rhs.references)
        }

        override fun toString(): String {
            val sb = StringBuilder()
            sb.appendln("${consumed.size} consumed, ${produced.size} produced")
            sb.appendln("")
            sb.appendln("Consumed:")
            consumed.forEach {
                sb.appendln("${it.ref}: ${it.state}")
            }
            sb.appendln("")
            sb.appendln("Produced:")
            produced.forEach {
                sb.appendln("${it.ref}: ${it.state}")
            }
            sb.appendln("References:")
            references.forEach {
                sb.appendln("${it.ref}: ${it.state}")
            }
            return sb.toString()
        }

        /** Additional copy method to maintain backwards compatibility. */
        fun copy(
                consumed: Set<StateAndRef<U>>,
                produced: Set<StateAndRef<U>>,
                flowId: UUID? = null,
                type: UpdateType = UpdateType.GENERAL
        ): Update<U> {
            return Update(consumed, produced, flowId, type, references)
        }

    }

    @CordaSerializable
    enum class StateStatus {
        UNCONSUMED, CONSUMED, ALL
    }

    /**
     * If the querying node is a participant in a state then it is classed as [RELEVANT].
     *
     * If the querying node is not a participant in a state then it is classed as [NOT_RELEVANT]. These types of
     * states can still be recorded in the vault if the transaction containing them was recorded with the
     * [StatesToRecord.ALL_VISIBLE] flag. This will typically happen for things like reference data which can be
     * referenced in transactions as a [ReferencedStateAndRef] but cannot be modified by any party but the maintainer.
     *
     * If both [RELEVANT] and [NOT_RELEVANT] states are required to be returned from a query, then the [ALL] flag
     * can be used.
     *
     * NOTE: Default behaviour is for ALL STATES to be returned as this is how Corda behaved before the introduction of
     * this query criterion.
     */
    @CordaSerializable
    enum class RelevancyStatus {
        RELEVANT, NOT_RELEVANT, ALL
    }

    /**
     *  Contract constraint information associated with a [ContractState].
     *  See [AttachmentConstraint]
     */
    @CordaSerializable
    data class ConstraintInfo(val constraint: AttachmentConstraint) {
        @CordaSerializable
        enum class Type {
            ALWAYS_ACCEPT, HASH, CZ_WHITELISTED, SIGNATURE
        }
        fun type(): Type {
            return when (constraint::class.java) {
                AlwaysAcceptAttachmentConstraint::class.java -> Type.ALWAYS_ACCEPT
                HashAttachmentConstraint::class.java -> Type.HASH
                WhitelistedByZoneAttachmentConstraint::class.java -> Type.CZ_WHITELISTED
                SignatureAttachmentConstraint::class.java -> Type.SIGNATURE
                else -> throw IllegalArgumentException("Invalid constraint type: $constraint")
            }
        }
        fun data(): ByteArray? {
            return when (type()) {
                Type.HASH -> (constraint as HashAttachmentConstraint).attachmentId.bytes
                Type.SIGNATURE -> (constraint as SignatureAttachmentConstraint).key.encoded
                else -> null
            }
        }
        companion object {
            fun constraintInfo(type: Type, data: ByteArray?): ConstraintInfo {
                return when (type) {
                    Type.ALWAYS_ACCEPT -> ConstraintInfo(AlwaysAcceptAttachmentConstraint)
                    Type.HASH -> ConstraintInfo(HashAttachmentConstraint(SecureHash.create(data!!.toHexString())))
                    Type.CZ_WHITELISTED -> ConstraintInfo(WhitelistedByZoneAttachmentConstraint)
                    Type.SIGNATURE -> ConstraintInfo(SignatureAttachmentConstraint(Crypto.decodePublicKey(data!!)))
                }
            }
        }
    }

    @CordaSerializable
    enum class UpdateType {
        GENERAL, NOTARY_CHANGE, CONTRACT_UPGRADE
    }

    /**
     * Returned in queries [VaultService.queryBy] and [VaultService.trackBy].
     * A Page contains:
     *  1) a [List] of actual [StateAndRef] requested by the specified [QueryCriteria] to a maximum of [MAX_PAGE_SIZE].
     *  2) a [List] of associated [Vault.StateMetadata], one per [StateAndRef] result.
     *  3) a total number of states that met the given [QueryCriteria] if a [PageSpecification] was provided,
     *     otherwise it defaults to -1.
     *  4) Status types used in this query: [StateStatus.UNCONSUMED], [StateStatus.CONSUMED], [StateStatus.ALL].
     *  5) Other results as a [List] of any type (eg. aggregate function results with/without group by).
     *
     *  Note: currently otherResults are used only for Aggregate Functions (in which case, the states and statesMetadata
     *  results will be empty).
     */
    @CordaSerializable
    data class Page<out T : ContractState>(val states: List<StateAndRef<T>>,
                                           val statesMetadata: List<StateMetadata>,
                                           val totalStatesAvailable: Long,
                                           val stateTypes: StateStatus,
                                           val otherResults: List<Any>)

    @CordaSerializable
    data class StateMetadata @JvmOverloads constructor(
            val ref: StateRef,
            val contractStateClassName: String,
            val recordedTime: Instant,
            val consumedTime: Instant?,
            val status: Vault.StateStatus,
            val notary: AbstractParty?,
            val lockId: String?,
            val lockUpdateTime: Instant?,
            val relevancyStatus: Vault.RelevancyStatus? = null,
            val constraintInfo: ConstraintInfo? = null
    ) {
        fun copy(
                ref: StateRef = this.ref,
                contractStateClassName: String = this.contractStateClassName,
                recordedTime: Instant = this.recordedTime,
                consumedTime: Instant? = this.consumedTime,
                status: Vault.StateStatus = this.status,
                notary: AbstractParty? = this.notary,
                lockId: String? = this.lockId,
                lockUpdateTime: Instant? = this.lockUpdateTime
        ): StateMetadata {
            return StateMetadata(ref, contractStateClassName, recordedTime, consumedTime, status, notary, lockId, lockUpdateTime, null)
        }
        fun copy(
                ref: StateRef = this.ref,
                contractStateClassName: String = this.contractStateClassName,
                recordedTime: Instant = this.recordedTime,
                consumedTime: Instant? = this.consumedTime,
                status: Vault.StateStatus = this.status,
                notary: AbstractParty? = this.notary,
                lockId: String? = this.lockId,
                lockUpdateTime: Instant? = this.lockUpdateTime,
                relevancyStatus: Vault.RelevancyStatus?
        ): StateMetadata {
            return StateMetadata(ref, contractStateClassName, recordedTime, consumedTime, status, notary, lockId, lockUpdateTime, relevancyStatus, ConstraintInfo(AlwaysAcceptAttachmentConstraint))
        }
    }

    companion object {
        @Deprecated("No longer used. The vault does not emit empty updates")
        val NoUpdate = Update(emptySet(), emptySet(), type = Vault.UpdateType.GENERAL, references = emptySet())
        @Deprecated("No longer used. The vault does not emit empty updates")
        val NoNotaryUpdate = Vault.Update(emptySet(), emptySet(), type = Vault.UpdateType.NOTARY_CHANGE, references = emptySet())
    }
}

/**
 * The maximum permissible size of contract constraint type data (for storage in vault states database table).
 *
 * This value establishes an upper limit of a CompositeKey with up to [MAX_NUMBER_OF_KEYS_IN_SIGNATURE_CONSTRAINT] keys stored in.
 * However, note this assumes a rather conservative upper bound per key.
 * For reference, measurements have shown the following numbers for each algorithm:
 * - 2048-bit RSA keys: 1 key -> 294 bytes, 2 keys -> 655 bytes, 3 keys -> 961 bytes
 * - 256-bit ECDSA (k1) keys: 1 key -> 88 bytes, 2 keys -> 231 bytes, 3 keys -> 331 bytes
 * - 256-bit EDDSA keys: 1 key -> 44 bytes, 2 keys -> 140 bytes, 3 keys -> 195 bytes
 */
const val MAX_CONSTRAINT_DATA_SIZE = 1_000 * MAX_NUMBER_OF_KEYS_IN_SIGNATURE_CONSTRAINT

/**
 * A [VaultService] is responsible for securely and safely persisting the current state of a vault to storage. The
 * vault service vends immutable snapshots of the current vault for working with: if you build a transaction based
 * on a vault that isn't current, be aware that it may end up being invalid if the states that were used have been
 * consumed by someone else first!
 *
 * Note that transactions we've seen are held by the storage service, not the vault.
 */
@DoNotImplement
interface VaultService {
    /**
     * Prefer the use of [updates] unless you know why you want to use this instead.
     *
     * Get a synchronous [Observable] of updates.  When observations are pushed to the Observer, the [Vault] will already
     * incorporate the update, and the database transaction associated with the update will still be open and current.
     * If for some reason the processing crosses outside of the database transaction (for example, the update is pushed
     * outside the current JVM or across to another [Thread], which is executing in a different database transaction),
     * then the [Vault] may not incorporate the update due to racing with committing the current database transaction.
     */
    val rawUpdates: Observable<Vault.Update<ContractState>>

    /**
     * Get a synchronous [Observable] of updates.  When observations are pushed to the Observer, the [Vault] will
     * already incorporate the update and the database transaction associated with the update will have been committed
     * and closed.
     */
    val updates: Observable<Vault.Update<ContractState>>

    /**
     * Provide a [CordaFuture] for when a [StateRef] is consumed, which can be very useful in building tests.
     */
    @DeleteForDJVM
    fun whenConsumed(ref: StateRef): CordaFuture<Vault.Update<ContractState>> {
        val query = QueryCriteria.VaultQueryCriteria(
                stateRefs = listOf(ref),
                status = Vault.StateStatus.CONSUMED
        )
        val result = trackBy<ContractState>(query)
        val snapshot = result.snapshot.states
        return if (snapshot.isNotEmpty()) {
            doneFuture(Vault.Update(consumed = setOf(snapshot.single()), produced = emptySet(), references = emptySet()))
        } else {
            result.updates.toFuture()
        }
    }

    /**
     *  Add a note to an existing [LedgerTransaction] given by its unique [SecureHash] id.
     *  Multiple notes may be attached to the same [LedgerTransaction].
     *  These are additively and immutably persisted within the node local vault database in a single textual field.
     *  using a semi-colon separator.
     */
    fun addNoteToTransaction(txnId: SecureHash, noteText: String)

    fun getTransactionNotes(txnId: SecureHash): Iterable<String>

    // DOCEND VaultStatesQuery

    /**
     * Soft locking is used to prevent multiple transactions trying to use the same states simultaneously.
     * Violation of a soft lock would result in a double spend being created and rejected by the notary.
     */

    // DOCSTART SoftLockAPI

    /**
     * Reserve a set of [StateRef] for a given [UUID] unique identifier.
     * Typically, the unique identifier will refer to a [FlowLogic.runId]'s [UUID] associated with an in-flight flow.
     * In this case if the flow terminates the locks will automatically be freed, even if there is an error.
     * However, the user can specify their own [UUID] and manage this manually, possibly across the lifetime of multiple
     * flows, or from other thread contexts e.g. [CordaService] instances.
     * In the case of coin selection, soft locks are automatically taken upon gathering relevant unconsumed input refs.
     *
     * @throws [StatesNotAvailableException] when not possible to soft-lock all of requested [StateRef].
     */
    @Throws(StatesNotAvailableException::class)
    fun softLockReserve(lockId: UUID, stateRefs: NonEmptySet<StateRef>)

    /**
     * Release all or an explicitly specified set of [StateRef] for a given [UUID] unique identifier.
     * A [Vault] soft-lock manager is automatically notified from flows that are terminated, such that any soft locked
     * states may be released.
     * In the case of coin selection, soft-locks are automatically released once previously gathered unconsumed
     * input refs are consumed as part of cash spending.
     */
    fun softLockRelease(lockId: UUID, stateRefs: NonEmptySet<StateRef>? = null)
    // DOCEND SoftLockAPI

    /**
     * Helper function to determine spendable states and soft locking them.
     * Currently performance will be worse than for the hand optimised version in
     * [Cash.unconsumedCashStatesForSpending]. However, this is fully generic and can operate with custom [FungibleState]
     * and [FungibleAsset] states.
     * @param lockId The [FlowLogic.runId]'s [UUID] of the current flow used to soft lock the states.
     * @param eligibleStatesQuery A custom query object that selects down to the appropriate subset of all states of the
     * [contractStateType]. e.g. by selecting on account, issuer, etc. The query is internally augmented with the
     * [StateStatus.UNCONSUMED], soft lock and contract type requirements.
     * @param amount The required amount of the asset. It is assumed that compatible issuer states will be filtered out
     * by the [eligibleStatesQuery]. This method accepts both Amount<Issued<*>> and Amount<*>. Amount<Issued<*>> is
     * automatically unwrapped to Amount<*>.
     * @param contractStateType class type of the result set.
     * @return Returns a locked subset of the [eligibleStatesQuery] sufficient to satisfy the requested amount,
     * or else an empty list and no change in the stored lock states when their are insufficient resources available.
     */
    @Suspendable
    @Throws(StatesNotAvailableException::class)
    fun <T : FungibleState<*>> tryLockFungibleStatesForSpending(lockId: UUID,
                                                                eligibleStatesQuery: QueryCriteria,
                                                                amount: Amount<*>,
                                                                contractStateType: Class<out T>): List<StateAndRef<T>>

    // DOCSTART VaultQueryAPI
    /**
     * Generic vault query function which takes a [QueryCriteria] object to define filters,
     * optional [PageSpecification] and optional [Sort] modification criteria (default unsorted),
     * and returns a [Vault.Page] object containing the following:
     *  1. states as a List of <StateAndRef> (page number and size defined by [PageSpecification])
     *  2. states metadata as a List of [Vault.StateMetadata] held in the Vault States table.
     *  3. total number of results available if [PageSpecification] supplied (otherwise returns -1).
     *  4. status types used in this query: [StateStatus.UNCONSUMED], [StateStatus.CONSUMED], [StateStatus.ALL].
     *  5. other results (aggregate functions with/without using value groups).
     *
     * @throws VaultQueryException if the query cannot be executed for any reason
     *        (missing criteria or parsing error, paging errors, unsupported query, underlying database error).
     *
     * Notes
     *   If no [PageSpecification] is provided, a maximum of [DEFAULT_PAGE_SIZE] results will be returned.
     *   API users must specify a [PageSpecification] if they are expecting more than [DEFAULT_PAGE_SIZE] results,
     *   otherwise a [VaultQueryException] will be thrown alerting to this condition.
     *   It is the responsibility of the API user to request further pages and/or specify a more suitable [PageSpecification].
     */
    @Throws(VaultQueryException::class)
    fun <T : ContractState> _queryBy(criteria: QueryCriteria,
                                     paging: PageSpecification,
                                     sorting: Sort,
                                     contractStateType: Class<out T>): Vault.Page<T>

    /**
     * Generic vault query function which takes a [QueryCriteria] object to define filters,
     * optional [PageSpecification] and optional [Sort] modification criteria (default unsorted),
     * and returns a [DataFeed] object containing:
     * 1) a snapshot as a [Vault.Page] (described previously in [queryBy]).
     * 2) an [Observable] of [Vault.Update].
     *
     * @throws VaultQueryException if the query cannot be executed for any reason.
     *
     * Notes:
     *    - The snapshot part of the query adheres to the same behaviour as the [queryBy] function.
     *    - The update part of the query currently only supports query criteria filtering by contract
     *      type(s) and state status(es). CID-731 <https://r3-cev.atlassian.net/browse/CID-731> proposes
     *      adding the complete set of [QueryCriteria] filtering.
     */
    @Throws(VaultQueryException::class)
    fun <T : ContractState> _trackBy(criteria: QueryCriteria,
                                     paging: PageSpecification,
                                     sorting: Sort,
                                     contractStateType: Class<out T>): DataFeed<Vault.Page<T>, Vault.Update<T>>
    // DOCEND VaultQueryAPI

    // Note: cannot apply @JvmOverloads to interfaces nor interface implementations.
    // Java Helpers.
    fun <T : ContractState> queryBy(contractStateType: Class<out T>): Vault.Page<T> {
        return _queryBy(QueryCriteria.VaultQueryCriteria(), PageSpecification(), Sort(emptySet()), contractStateType)
    }

    fun <T : ContractState> queryBy(contractStateType: Class<out T>, criteria: QueryCriteria): Vault.Page<T> {
        return _queryBy(criteria, PageSpecification(), Sort(emptySet()), contractStateType)
    }

    fun <T : ContractState> queryBy(contractStateType: Class<out T>, paging: PageSpecification): Vault.Page<T> {
        return _queryBy(QueryCriteria.VaultQueryCriteria(), paging, Sort(emptySet()), contractStateType)
    }

    fun <T : ContractState> queryBy(contractStateType: Class<out T>, criteria: QueryCriteria, paging: PageSpecification): Vault.Page<T> {
        return _queryBy(criteria, paging, Sort(emptySet()), contractStateType)
    }

    fun <T : ContractState> queryBy(contractStateType: Class<out T>, criteria: QueryCriteria, sorting: Sort): Vault.Page<T> {
        return _queryBy(criteria, PageSpecification(), sorting, contractStateType)
    }

    fun <T : ContractState> queryBy(contractStateType: Class<out T>, criteria: QueryCriteria, paging: PageSpecification, sorting: Sort): Vault.Page<T> {
        return _queryBy(criteria, paging, sorting, contractStateType)
    }

    fun <T : ContractState> trackBy(contractStateType: Class<out T>): DataFeed<Vault.Page<T>, Vault.Update<T>> {
        return _trackBy(QueryCriteria.VaultQueryCriteria(), PageSpecification(), Sort(emptySet()), contractStateType)
    }

    fun <T : ContractState> trackBy(contractStateType: Class<out T>, criteria: QueryCriteria): DataFeed<Vault.Page<T>, Vault.Update<T>> {
        return _trackBy(criteria, PageSpecification(), Sort(emptySet()), contractStateType)
    }

    fun <T : ContractState> trackBy(contractStateType: Class<out T>, paging: PageSpecification): DataFeed<Vault.Page<T>, Vault.Update<T>> {
        return _trackBy(QueryCriteria.VaultQueryCriteria(), paging, Sort(emptySet()), contractStateType)
    }

    fun <T : ContractState> trackBy(contractStateType: Class<out T>, criteria: QueryCriteria, paging: PageSpecification): DataFeed<Vault.Page<T>, Vault.Update<T>> {
        return _trackBy(criteria, paging, Sort(emptySet()), contractStateType)
    }

    fun <T : ContractState> trackBy(contractStateType: Class<out T>, criteria: QueryCriteria, sorting: Sort): DataFeed<Vault.Page<T>, Vault.Update<T>> {
        return _trackBy(criteria, PageSpecification(), sorting, contractStateType)
    }

    fun <T : ContractState> trackBy(contractStateType: Class<out T>, criteria: QueryCriteria, paging: PageSpecification, sorting: Sort): DataFeed<Vault.Page<T>, Vault.Update<T>> {
        return _trackBy(criteria, paging, sorting, contractStateType)
    }
}

inline fun <reified T : ContractState> VaultService.queryBy(): Vault.Page<T> {
    return _queryBy(QueryCriteria.VaultQueryCriteria(), PageSpecification(), Sort(emptySet()), T::class.java)
}

inline fun <reified T : ContractState> VaultService.queryBy(criteria: QueryCriteria): Vault.Page<T> {
    return _queryBy(criteria, PageSpecification(), Sort(emptySet()), T::class.java)
}

inline fun <reified T : ContractState> VaultService.queryBy(paging: PageSpecification): Vault.Page<T> {
    return _queryBy(QueryCriteria.VaultQueryCriteria(), paging, Sort(emptySet()), T::class.java)
}

inline fun <reified T : ContractState> VaultService.queryBy(criteria: QueryCriteria, paging: PageSpecification): Vault.Page<T> {
    return _queryBy(criteria, paging, Sort(emptySet()), T::class.java)
}

inline fun <reified T : ContractState> VaultService.queryBy(criteria: QueryCriteria, sorting: Sort): Vault.Page<T> {
    return _queryBy(criteria, PageSpecification(), sorting, T::class.java)
}

inline fun <reified T : ContractState> VaultService.queryBy(criteria: QueryCriteria, paging: PageSpecification, sorting: Sort): Vault.Page<T> {
    return _queryBy(criteria, paging, sorting, T::class.java)
}

inline fun <reified T : ContractState> VaultService.trackBy(): DataFeed<Vault.Page<T>, Vault.Update<T>> {
    return _trackBy(QueryCriteria.VaultQueryCriteria(), PageSpecification(), Sort(emptySet()), T::class.java)
}

inline fun <reified T : ContractState> VaultService.trackBy(paging: PageSpecification): DataFeed<Vault.Page<T>, Vault.Update<T>> {
    return _trackBy(QueryCriteria.VaultQueryCriteria(), paging, Sort(emptySet()), T::class.java)
}

inline fun <reified T : ContractState> VaultService.trackBy(criteria: QueryCriteria): DataFeed<Vault.Page<T>, Vault.Update<T>> {
    return _trackBy(criteria, PageSpecification(), Sort(emptySet()), T::class.java)
}

inline fun <reified T : ContractState> VaultService.trackBy(criteria: QueryCriteria, paging: PageSpecification): DataFeed<Vault.Page<T>, Vault.Update<T>> {
    return _trackBy(criteria, paging, Sort(emptySet()), T::class.java)
}

inline fun <reified T : ContractState> VaultService.trackBy(criteria: QueryCriteria, sorting: Sort): DataFeed<Vault.Page<T>, Vault.Update<T>> {
    return _trackBy(criteria, PageSpecification(), sorting, T::class.java)
}

inline fun <reified T : ContractState> VaultService.trackBy(criteria: QueryCriteria, paging: PageSpecification, sorting: Sort): DataFeed<Vault.Page<T>, Vault.Update<T>> {
    return _trackBy(criteria, paging, sorting, T::class.java)
}

class VaultQueryException(description: String, cause: Exception? = null) : FlowException(description, cause) {
    constructor(description: String) : this(description, null)
}

class StatesNotAvailableException(override val message: String?, override val cause: Throwable? = null) : FlowException(message, cause) {
    override fun toString() = "Soft locking error: $message"
}