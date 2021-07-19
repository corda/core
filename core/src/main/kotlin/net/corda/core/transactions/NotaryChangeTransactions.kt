package net.corda.core.transactions

import net.corda.core.CordaInternal
import net.corda.core.DeleteForDJVM
import net.corda.core.KeepForDJVM
import net.corda.core.contracts.*
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.SecureHash.Companion.SHA2_256
import net.corda.core.crypto.TransactionSignature
import net.corda.core.crypto.hashAs
import net.corda.core.identity.Party
import net.corda.core.node.NetworkParameters
import net.corda.core.node.ServiceHub
import net.corda.core.node.ServicesForResolution
import net.corda.core.serialization.CordaSerializable
import net.corda.core.serialization.DeprecatedConstructorForDeserialization
import net.corda.core.serialization.SerializedBytes
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.transactions.NotaryChangeWireTransaction.Component.*
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.toBase58String
import java.security.PublicKey

/**
 * A special transaction for changing the notary of a state. It only needs specifying the state(s) as input(s),
 * old and new notaries. Output states can be computed by applying the notary modification to corresponding inputs
 * on the fly.
 */
@CordaSerializable
@KeepForDJVM
data class NotaryChangeWireTransaction(
        /**
         * Contains all of the transaction components in serialized form.
         * This is used for calculating the transaction id in a deterministic fashion, since re-serializing properties
         * may result in a different byte sequence depending on the serialization context.
         */
        val serializedComponents: List<OpaqueBytes>,
        val hashAlgorithm: String
) : CoreTransaction() {
    @DeprecatedConstructorForDeserialization(1)
    constructor(serializedComponents: List<OpaqueBytes>) : this(serializedComponents, SHA2_256)

    fun copy(serializedComponents: List<OpaqueBytes>): NotaryChangeWireTransaction {
        return NotaryChangeWireTransaction(serializedComponents, hashAlgorithm)
    }

    override val inputs: List<StateRef> = serializedComponents[INPUTS.ordinal].deserialize()
    override val references: List<StateRef> = emptyList()
    override val notary: Party = serializedComponents[NOTARY.ordinal].deserialize()

    /** Identity of the notary service to reassign the states to.*/
    val newNotary: Party = serializedComponents[NEW_NOTARY.ordinal].deserialize()

    override val networkParametersHash: SecureHash? by lazy {
        if (serializedComponents.size >= PARAMETERS_HASH.ordinal + 1) {
            serializedComponents[PARAMETERS_HASH.ordinal].deserialize<SecureHash>()
        } else null
    }

    /**
     * This transaction does not contain any output states, outputs can be obtained by resolving a
     * [NotaryChangeLedgerTransaction] and applying the notary modification to inputs.
     */
    override val outputs: List<TransactionState<ContractState>>
        get() = throw UnsupportedOperationException("NotaryChangeWireTransaction does not contain output states, " +
                "outputs can only be obtained from a resolved NotaryChangeLedgerTransaction")

    init {
        check(inputs.isNotEmpty()) { "A notary change transaction must have inputs" }
        check(notary != newNotary) { "The old and new notaries must be different – $newNotary" }
        checkBaseInvariants()
    }

    /**
     * A privacy salt is not really required in this case, because we already used nonces in normal transactions and
     * thus input state refs will always be unique. Also, filtering doesn't apply on this type of transactions.
     */
    override val id: SecureHash by lazy {
        serializedComponents.map { component ->
            component.bytes.hashAs(hashAlgorithm)
        }.reduce { combinedHash, componentHash ->
            combinedHash.concatenate(componentHash)
        }
    }

    /** Resolves input states and network parameters and builds a [NotaryChangeLedgerTransaction]. */
    @DeleteForDJVM
    fun resolve(services: ServicesForResolution, sigs: List<TransactionSignature>): NotaryChangeLedgerTransaction {
        val resolvedInputs = services.loadStates(inputs.toSet()).toList()
        val hashToResolve = networkParametersHash ?: services.networkParametersService.defaultHash
        val resolvedNetworkParameters = services.networkParametersService.lookup(hashToResolve)
                ?: throw TransactionResolutionException(id)
        return NotaryChangeLedgerTransaction.create(resolvedInputs, notary, newNotary, id, sigs, resolvedNetworkParameters)
    }

    /** Resolves input states and builds a [NotaryChangeLedgerTransaction]. */
    @DeleteForDJVM
    fun resolve(services: ServiceHub, sigs: List<TransactionSignature>) = resolve(services as ServicesForResolution, sigs)

    /**
     * This should return a serialized virtual output state, that will be used to verify spending transactions.
     * The binary output should not depend on the classpath of the node that is verifying the transaction.
     *
     * Ideally the serialization engine would support partial deserialization so that only the Notary ( and the encumbrance can be replaced from the binary input state)
     *
     *
     * TODO - currently this uses the main classloader.
     */
    @CordaInternal
    internal fun resolveOutputComponent(
            services: ServicesForResolution,
            stateRef: StateRef,
            @Suppress("UNUSED_PARAMETER") params: NetworkParameters
    ): SerializedBytes<TransactionState<ContractState>> {
        return services.loadState(stateRef).serialize()
    }

    enum class Component {
        INPUTS, NOTARY, NEW_NOTARY, PARAMETERS_HASH
    }

    @Deprecated("Required only for backwards compatibility purposes. This type of transaction should not be constructed outside Corda code.", ReplaceWith("NotaryChangeTransactionBuilder"), DeprecationLevel.WARNING)
    constructor(inputs: List<StateRef>, notary: Party, newNotary: Party) : this(listOf(inputs, notary, newNotary).map { it.serialize() })
}

/**
 * A notary change transaction with fully resolved inputs and signatures. In contrast with a regular transaction,
 * signatures are checked against the signers specified by input states' *participants* fields, so full resolution is
 * needed for signature verification.
 */
@KeepForDJVM
class NotaryChangeLedgerTransaction
private constructor(
        override val inputs: List<StateAndRef<ContractState>>,
        override val notary: Party,
        val newNotary: Party,
        override val id: SecureHash,
        override val sigs: List<TransactionSignature>,
        override val networkParameters: NetworkParameters?
) : FullTransaction(), TransactionWithSignatures {
    companion object {
        @CordaInternal
        internal fun create(inputs: List<StateAndRef<ContractState>>,
                            notary: Party,
                            newNotary: Party,
                            id: SecureHash,
                            sigs: List<TransactionSignature>,
                            networkParameters: NetworkParameters): NotaryChangeLedgerTransaction {
            return NotaryChangeLedgerTransaction(inputs, notary, newNotary, id, sigs, networkParameters)
        }
    }

    init {
        checkEncumbrances()
        checkNewNotaryWhitelisted()
    }

    /**
     * Check that the output notary is whitelisted.
     *
     * Note that for this transaction type we do not require the input notary to be whitelisted to support network merging.
     * For all other transaction types this is enforced.
     */
    private fun checkNewNotaryWhitelisted() {
        networkParameters?.let { parameters ->
            val notaryWhitelist = parameters.notaries.map { it.identity }
            check(newNotary in notaryWhitelist) {
                "The output notary $newNotary is not whitelisted in the attached network parameters."
            }
        }
    }

    override val references: List<StateAndRef<ContractState>> = emptyList()

    /** We compute the outputs on demand by applying the notary field modification to the inputs. */
    override val outputs: List<TransactionState<ContractState>>
        get() = computeOutputs()

    private fun computeOutputs(): List<TransactionState<ContractState>> {
        val inputPositionIndex: Map<StateRef, Int> = inputs.mapIndexed { index, stateAndRef -> stateAndRef.ref to index }.toMap()
        return inputs.map { (state, ref) ->
            if (state.encumbrance != null) {
                val encumbranceStateRef = StateRef(ref.txhash, state.encumbrance)
                val encumbrancePosition = inputPositionIndex[encumbranceStateRef]
                        ?: throw IllegalStateException("Unable to generate output states – transaction not constructed correctly.")
                state.copy(notary = newNotary, encumbrance = encumbrancePosition)
            } else state.copy(notary = newNotary)
        }
    }

    override val requiredSigningKeys: Set<PublicKey>
        get() = inputs.flatMap { it.state.data.participants }.map { it.owningKey }.toSet() + notary.owningKey

    override fun getKeyDescriptions(keys: Set<PublicKey>): List<String> {
        return keys.map { it.toBase58String() }
    }

    /**
     * Check that encumbrances have been included in the inputs.
     */
    private fun checkEncumbrances() {
        val encumberedStates = inputs.asSequence().filter { it.state.encumbrance != null }.associateBy { it.ref }
        if (encumberedStates.isNotEmpty()) {
            inputs.forEach { (state, ref) ->
                if (StateRef(ref.txhash, state.encumbrance!!) !in encumberedStates) {
                    throw TransactionVerificationException.TransactionMissingEncumbranceException(
                            id,
                            state.encumbrance,
                            TransactionVerificationException.Direction.INPUT)
                }
            }
        }
    }

    operator fun component1(): List<StateAndRef<ContractState>> = inputs
    operator fun component2(): Party = notary
    operator fun component3(): Party = newNotary
    operator fun component4(): SecureHash = id
    operator fun component5(): List<TransactionSignature> = sigs
    operator fun component6(): NetworkParameters? = networkParameters

    override fun equals(other: Any?): Boolean = this === other || other is NotaryChangeLedgerTransaction && this.id == other.id

    override fun hashCode(): Int = id.hashCode()

    override fun toString(): String {
        return """NotaryChangeLedgerTransaction(
            |    id=$id
            |    inputs=$inputs
            |    notary=$notary
            |    newNotary=$newNotary
            |    sigs=$sigs
            |    networkParameters=$networkParameters
            |)""".trimMargin()
    }

    // Things that we can't remove after `data class` removal from this class, so it is deprecated instead.
    //
    @Deprecated("NotaryChangeLedgerTransaction should not be created directly, use NotaryChangeWireTransaction.resolve instead.")
    constructor(
            inputs: List<StateAndRef<ContractState>>,
            notary: Party,
            newNotary: Party,
            id: SecureHash,
            sigs: List<TransactionSignature>
    ) : this(inputs, notary, newNotary, id, sigs, null)

    @Deprecated("NotaryChangeLedgerTransaction should not be created directly, use NotaryChangeWireTransaction.resolve instead.")
    fun copy(inputs: List<StateAndRef<ContractState>> = this.inputs,
             notary: Party = this.notary,
             newNotary: Party = this.newNotary,
             id: SecureHash = this.id,
             sigs: List<TransactionSignature> = this.sigs
    ): NotaryChangeLedgerTransaction {
        return NotaryChangeLedgerTransaction(
                inputs,
                notary,
                newNotary,
                id,
                sigs,
                this.networkParameters
        )
    }
}
