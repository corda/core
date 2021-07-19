@file:JvmName("Structures")
@file:KeepForDJVM
package net.corda.core.contracts

import net.corda.core.DeleteForDJVM
import net.corda.core.KeepForDJVM
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.secureRandomBytes
import net.corda.core.crypto.toStringShort
import net.corda.core.flows.FlowLogicRef
import net.corda.core.flows.FlowLogicRefFactory
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.Party
import net.corda.core.serialization.CordaSerializable
import net.corda.core.serialization.serialize
import net.corda.core.transactions.LedgerTransaction
import net.corda.core.utilities.OpaqueBytes
import java.security.PublicKey
import java.time.Instant

// DOCSTART 1
/** Implemented by anything that can be named by a secure hash value (e.g. transactions, attachments). */
interface NamedByHash {
    val id: SecureHash
}
// DOCEND 1

/**
 * The [Issued] data class holds the details of an on ledger digital asset.
 * In particular it gives the public credentials of the entity that created these digital tokens
 * and the particular product represented.
 *
 * @param P the class type of product underlying the definition, for example [java.util.Currency].
 * @property issuer The [AbstractParty] details of the entity which issued the asset
 * and a reference blob, which can contain other details related to the token creation e.g. serial number,
 * warehouse location, etc.
 * The issuer is the gatekeeper for creating, or destroying the tokens on the digital ledger and
 * only their [PrivateKey] signature can authorise transactions that do not conserve the total number
 * of tokens on the ledger.
 * Other identities may own the tokens, but they can only create transactions that conserve the total token count.
 * Typically the issuer is also a well know organisation that can convert digital tokens to external assets
 * and thus underwrites the digital tokens.
 * Different issuer values may coexist for a particular product, but these cannot be merged.
 * @property product The details of the specific product represented by these digital tokens. The value
 * of product may differentiate different kinds of asset within the same logical class e.g the currency, or
 * it may just be a type marker for a single custom asset.
 */
@KeepForDJVM
@CordaSerializable
data class Issued<out P : Any>(val issuer: PartyAndReference, val product: P) {
    init {
        require(issuer.reference.size <= MAX_ISSUER_REF_SIZE) { "Maximum issuer reference size is $MAX_ISSUER_REF_SIZE." }
    }

    override fun toString() = "$product issued by $issuer"
}

/**
 * The maximum permissible size of an issuer reference.
 */
const val MAX_ISSUER_REF_SIZE = 512

/**
 * Strips the issuer and returns an [Amount] of the raw token directly. This is useful when you are mixing code that
 * cares about specific issuers with code that will accept any, or which is imposing issuer constraints via some
 * other mechanism and the additional type safety is not wanted.
 */
fun <T : Any> Amount<Issued<T>>.withoutIssuer(): Amount<T> = Amount(quantity, displayTokenSize, token.product)

// DOCSTART 3

/**
 * Return structure for [OwnableState.withNewOwner]
 */
@KeepForDJVM
data class CommandAndState(val command: CommandData, val ownableState: OwnableState)

/**
 * A contract state that can have a single owner.
 */
@KeepForDJVM
interface OwnableState : ContractState {
    /** There must be a MoveCommand signed by this key to claim the amount. */
    val owner: AbstractParty

    /** Copies the underlying data structure, replacing the owner field with this new value and leaving the rest alone. */
    fun withNewOwner(newOwner: AbstractParty): CommandAndState
}
// DOCEND 3

/** Something which is scheduled to happen at a point in time. */
@KeepForDJVM
interface Scheduled {
    val scheduledAt: Instant
}

/**
 * Represents a contract state (unconsumed output) of type [LinearState] and a point in time that a lifecycle event is
 * expected to take place for that contract state.
 *
 * This is effectively the input to a scheduler, which wakes up at that point in time and asks the contract state what
 * lifecycle processing needs to take place.  e.g. a fixing or a late payment etc.
 */
@CordaSerializable
@KeepForDJVM
data class ScheduledStateRef(val ref: StateRef, override val scheduledAt: Instant) : Scheduled

/**
 * This class represents the lifecycle activity that a contract state of type [LinearState] would like to perform at a
 * given point in time. e.g. run a fixing flow.
 *
 * Note the use of [FlowLogicRef] to represent a safe way to transport a [net.corda.core.flows.FlowLogic] out of the
 * contract sandbox.
 *
 * Currently we support only flow based activities as we expect there to be a transaction generated off the back of
 * the activity, otherwise we have to start tracking secondary state on the platform of which scheduled activities
 * for a particular [ContractState] have been processed/fired etc.  If the activity is not "on ledger" then the
 * scheduled activity shouldn't be either.
 */
@KeepForDJVM
data class ScheduledActivity(val logicRef: FlowLogicRef, override val scheduledAt: Instant) : Scheduled

// DOCSTART 2
/**
 * A state that evolves by superseding itself, all of which share the common "linearId".
 *
 * This simplifies the job of tracking the current version of certain types of state in e.g. a vault.
 */
@KeepForDJVM
interface LinearState : ContractState {
    /**
     * Unique id shared by all LinearState states throughout history within the vaults of all parties.
     * Verify methods should check that one input and one output share the id in a transaction,
     * except at issuance/termination.
     */
    val linearId: UniqueIdentifier
}
// DOCEND 2
@KeepForDJVM
interface SchedulableState : ContractState {
    /**
     * Indicate whether there is some activity to be performed at some future point in time with respect to this
     * [ContractState], what that activity is and at what point in time it should be initiated.
     * This can be used to implement deadlines for payment or processing of financial instruments according to a schedule.
     *
     * The state has no reference to it's own StateRef, so supply that for use as input to any FlowLogic constructed.
     *
     * @return null if there is no activity to schedule.
     */
    fun nextScheduledActivity(thisStateRef: StateRef, flowLogicRefFactory: FlowLogicRefFactory): ScheduledActivity?
}

/** Returns the SHA-256 hash of the serialised contents of this state (not cached!) */
fun ContractState.hash(): SecureHash = SecureHash.sha256(serialize().bytes)

/** Returns the hash of the serialised contents of this state (not cached!) */
fun ContractState.hash(algorithm: String): SecureHash = SecureHash.hashAs(algorithm, serialize().bytes)

/**
 * A stateref is a pointer (reference) to a state, this is an equivalent of an "outpoint" in Bitcoin. It records which
 * transaction defined the state and where in that transaction it was.
 */
@KeepForDJVM
@CordaSerializable
// DOCSTART 8
data class StateRef(val txhash: SecureHash, val index: Int) {
    override fun toString() = "$txhash($index)"
}
// DOCEND 8

/** A StateAndRef is simply a (state, ref) pair. For instance, a vault (which holds available assets) contains these. */
@KeepForDJVM
@CordaSerializable
// DOCSTART 7
data class StateAndRef<out T : ContractState>(val state: TransactionState<T>, val ref: StateRef) {
    /** For adding [StateAndRef]s as references to a [TransactionBuilder]. */
    fun referenced() = ReferencedStateAndRef(this)
}
// DOCEND 7

/** A wrapper for a [StateAndRef] indicating that it should be added to a transaction as a reference input state. */
@KeepForDJVM
data class ReferencedStateAndRef<out T : ContractState>(val stateAndRef: StateAndRef<T>)

/** Filters a list of [StateAndRef] objects according to the type of the states */
inline fun <reified T : ContractState> Iterable<StateAndRef<ContractState>>.filterStatesOfType(): List<StateAndRef<T>> {
    return mapNotNull { if (it.state.data is T) StateAndRef(TransactionState(it.state.data, it.state.contract, it.state.notary), it.ref) else null }
}

/**
 * Reference to something being stored or issued by a party e.g. in a vault or (more likely) on their normal
 * ledger. The reference is intended to be encrypted so it's meaningless to anyone other than the party.
 */
@KeepForDJVM
@CordaSerializable
data class PartyAndReference(val party: AbstractParty, val reference: OpaqueBytes) {
    override fun toString() = "$party$reference"
}

/** Marker interface for classes that represent commands */
@CordaSerializable
interface CommandData

/** Commands that inherit from this are intended to have no data items: it's only their presence that matters. */
@KeepForDJVM
abstract class TypeOnlyCommandData : CommandData {
    override fun equals(other: Any?) = other?.javaClass == javaClass
    override fun hashCode() = javaClass.name.hashCode()
}

/** Command data/content plus pubkey pair: the signature is stored at the end of the serialized bytes */
@KeepForDJVM
@CordaSerializable
data class Command<T : CommandData>(val value: T, val signers: List<PublicKey>) {
    // TODO Introduce NonEmptyList?
    init {
        require(signers.isNotEmpty()) { "The list of signers cannot be empty" }
    }

    constructor(data: T, key: PublicKey) : this(data, listOf(key))

    private fun commandDataToString() = value.toString().let { if (it.contains("@")) it.replace('$', '.').split("@")[0] else it }
    override fun toString() = "${commandDataToString()} with pubkeys ${signers.joinToString { it.toStringShort() }}"
}

/** A common move command for contract states which can change owner. */
@KeepForDJVM
interface MoveCommand : CommandData {
    /**
     * Contract code the moved state(s) are for the attention of, for example to indicate that the states are moved in
     * order to settle an obligation contract's state object(s).
     */
    // TODO: Replace Class here with a general contract constraints object
    val contract: Class<out Contract>?
}

// DOCSTART 6
/** A [Command] where the signing parties have been looked up if they have a well known/recognised institutional key. */
@KeepForDJVM
@CordaSerializable
data class CommandWithParties<out T : CommandData>(
        val signers: List<PublicKey>,
        /** If any public keys were recognised, the looked up institutions are available here */
        @Deprecated("Should not be used in contract verification code as it is non-deterministic, will be disabled for some future target platform version onwards and will take effect only for CorDapps targeting those versions.")
        val signingParties: List<Party>,
        val value: T
)
// DOCEND 6

// DOCSTART 5
/**
 * Implemented by a program that implements business logic on the shared ledger. All participants run this code for
 * every [net.corda.core.transactions.LedgerTransaction] they see on the network, for every input and output state. All
 * contracts must accept the transaction for it to be accepted: failure of any aborts the entire thing. The time is taken
 * from a trusted time-window attached to the transaction itself i.e. it is NOT necessarily the current time.
 *
 * TODO: Contract serialization is likely to change, so the annotation is likely temporary.
 */
@KeepForDJVM
@CordaSerializable
interface Contract {
    /**
     * Takes an object that represents a state transition, and ensures the inputs/outputs/commands make sense.
     * Must throw an exception if there's a problem that should prevent state transition. Takes a single object
     * rather than an argument so that additional data can be added without breaking binary compatibility with
     * existing contract code.
     */
    @Throws(IllegalArgumentException::class)
    fun verify(tx: LedgerTransaction)
}
// DOCEND 5

/** The annotated [Contract] implements the legal prose identified by the given URI. */
@Target(AnnotationTarget.CLASS)
@MustBeDocumented
annotation class LegalProseReference(val uri: String)

/**
 * Interface which can upgrade state objects issued by a contract to a new state object issued by a different contract.
 * The upgraded contract should specify the legacy contract class name, and provide an upgrade function that will convert
 * legacy contract states into states defined by this contract.
 *
 * In addition to the legacy contract class name, you can also specify the legacy contract constraint by implementing
 * [UpgradedContractWithLegacyConstraint] instead. Otherwise, the default [WhitelistedByZoneAttachmentConstraint] will
 * be used for verifying the validity of an upgrade transaction.
 *
 * @param OldState the old contract state (can be [ContractState] or other common supertype if this supports upgrading
 * more than one state).
 * @param NewState the upgraded contract state.
 */
@KeepForDJVM
interface UpgradedContract<in OldState : ContractState, out NewState : ContractState> : Contract {
    /**
     * Name of the contract this is an upgraded version of, used as part of verification of upgrade transactions.
     */
    val legacyContract: ContractClassName
    /**
     * Upgrade contract's state object to a new state object.
     *
     * @throws IllegalArgumentException if the given state object is not one that can be upgraded. This can be either
     * that the class is incompatible, or that the data inside the state object cannot be upgraded for some reason.
     */
    fun upgrade(state: OldState): NewState
}

/**
 * This interface allows specifying a custom legacy contract constraint for upgraded contracts. The default for [UpgradedContract]
 * is [WhitelistedByZoneAttachmentConstraint].
 */
@KeepForDJVM
interface UpgradedContractWithLegacyConstraint<in OldState : ContractState, out NewState : ContractState> : UpgradedContract<OldState, NewState> {
    /**
     * A validator for the legacy (pre-upgrade) contract attachments on the transaction.
     */
    val legacyContractConstraint: AttachmentConstraint
}

/**
 * A privacy salt is required to compute nonces per transaction component in order to ensure that an adversary cannot
 * use brute force techniques and reveal the content of a Merkle-leaf hashed value.
 * Because this salt serves the role of the seed to compute nonces, its size and entropy should be equal to the
 * underlying hash function used for Merkle tree generation, currently [SecureHash.SHA256], which has an output of 32 bytes.
 * There are two constructors, one that generates a new 32-bytes random salt, and another that takes a [ByteArray] input.
 * The latter is required in cases where the salt value needs to be pre-generated (agreed between transacting parties),
 * but it is highlighted that one should always ensure it has sufficient entropy.
 */
@CordaSerializable
@KeepForDJVM
class PrivacySalt(bytes: ByteArray) : OpaqueBytes(bytes) {
    /** Constructs a salt with a randomly-generated saltLength byte value. */
    @DeleteForDJVM
    constructor(saltLength: Int) : this(secureRandomBytes(saltLength))

    /** Constructs a salt with a randomly-generated 32 byte value. */
    @DeleteForDJVM
    constructor() : this(MINIMUM_SIZE)

    init {
        require(bytes.any { it != 0.toByte() }) { "Privacy salt should not be all zeros." }
        require(bytes.size >= MINIMUM_SIZE) { "Privacy salt should be at least $MINIMUM_SIZE bytes." }
    }

    fun validateFor(algorithm: String) {
        val digestLength = SecureHash.digestLengthFor(algorithm)
        require(bytes.size >= digestLength) { "Privacy salt should be at least $digestLength bytes for $algorithm." }
    }

    companion object {
        private const val MINIMUM_SIZE = 32

        @DeleteForDJVM
        @JvmStatic
        fun createFor(algorithm: String): PrivacySalt {
            return PrivacySalt(SecureHash.digestLengthFor(algorithm))
        }
    }
}

/**
 * A convenience class for passing around a state and it's contract
 *
 * @property state A state
 * @property contract The contract that should verify the state
 */
@KeepForDJVM
data class StateAndContract(val state: ContractState, val contract: ContractClassName)
