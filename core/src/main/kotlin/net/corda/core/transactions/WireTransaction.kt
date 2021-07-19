package net.corda.core.transactions

import net.corda.core.CordaInternal
import net.corda.core.DeleteForDJVM
import net.corda.core.KeepForDJVM
import net.corda.core.contracts.*
import net.corda.core.contracts.ComponentGroupEnum.COMMANDS_GROUP
import net.corda.core.contracts.ComponentGroupEnum.OUTPUTS_GROUP
import net.corda.core.crypto.*
import net.corda.core.crypto.SecureHash.Companion.SHA2_256
import net.corda.core.identity.Party
import net.corda.core.internal.*
import net.corda.core.node.NetworkParameters
import net.corda.core.node.ServiceHub
import net.corda.core.node.ServicesForResolution
import net.corda.core.node.services.AttachmentId
import net.corda.core.serialization.CordaSerializable
import net.corda.core.serialization.DeprecatedConstructorForDeserialization
import net.corda.core.serialization.SerializedBytes
import net.corda.core.serialization.internal.AttachmentsClassLoaderCache
import net.corda.core.serialization.serialize
import net.corda.core.utilities.OpaqueBytes
import java.security.PublicKey
import java.security.SignatureException
import java.util.function.Predicate

/**
 * A transaction ready for serialisation, without any signatures attached. A WireTransaction is usually wrapped
 * by a [SignedTransaction] that carries the signatures over this payload.
 * The identity of the transaction is the Merkle tree root of its components (see [MerkleTree]).
 *
 * For privacy purposes, each part of a transaction should be accompanied by a nonce.
 * To avoid storing a random number (nonce) per component, an initial [privacySalt] is the sole value utilised,
 * so that all component nonces are deterministically computed.
 *
 * A few notes about backwards compatibility:
 * A wire transaction can be backwards compatible, in the sense that if an old client receives a [componentGroups] with
 * more elements than expected, it will normally deserialise the required objects and omit any checks in the optional
 * new fields. Moreover, because the Merkle tree is constructed from the received list of [ComponentGroup], which internally
 * deals with bytes, any client can compute the Merkle tree and on the same time relay a [WireTransaction] object even
 * if she is unable to read some of the "optional" component types. We stress that practically, a new type of
 * [WireTransaction] should only be considered compatible if and only if the following rules apply:
 * <p><ul>
 * <li>Component-type ordering is fixed (eg. inputs, then outputs, then commands etc, see [ComponentGroupEnum] for the actual ordering).
 * <li>Removing a component-type that existed in older wire transaction types is not allowed, because it will affect the Merkle tree structure.
 * <li>Changing the order of existing component types is also not allowed, for the same reason.
 * <li>New component types must be added at the end of the list of [ComponentGroup] and update the [ComponentGroupEnum] with the new type. After a component is added, its ordinal must never change.
 * <li>A new component type should always be an "optional value", in the sense that lack of its visibility does not change the transaction and contract logic and details. An example of "optional" components could be a transaction summary or some statistics.
 * </ul></p>
 */
@CordaSerializable
@KeepForDJVM
class WireTransaction(componentGroups: List<ComponentGroup>, val privacySalt: PrivacySalt, val hashAlgorithm: String) : TraversableTransaction(componentGroups) {
    @DeprecatedConstructorForDeserialization(1)
    constructor(componentGroups: List<ComponentGroup>, privacySalt: PrivacySalt = PrivacySalt()) : this(componentGroups, privacySalt, SHA2_256)

    @DeleteForDJVM
    constructor(componentGroups: List<ComponentGroup>) : this(componentGroups, PrivacySalt())

    @Deprecated("Required only in some unit-tests and for backwards compatibility purposes.",
            ReplaceWith("WireTransaction(val componentGroups: List<ComponentGroup>, override val privacySalt: PrivacySalt)"), DeprecationLevel.WARNING)
    @DeleteForDJVM
    @JvmOverloads
    constructor(
            inputs: List<StateRef>,
            attachments: List<SecureHash>,
            outputs: List<TransactionState<ContractState>>,
            commands: List<Command<*>>,
            notary: Party?,
            timeWindow: TimeWindow?,
            privacySalt: PrivacySalt = PrivacySalt()
    ) : this(createComponentGroups(inputs, outputs, commands, attachments, notary, timeWindow, emptyList(), null), privacySalt)

    init {
        check(componentGroups.all { it.components.isNotEmpty() }) { "Empty component groups are not allowed" }
        check(componentGroups.map { it.groupIndex }.toSet().size == componentGroups.size) { "Duplicated component groups detected" }
        checkBaseInvariants()
        check(inputs.isNotEmpty() || outputs.isNotEmpty()) { "A transaction must contain at least one input or output state" }
        check(commands.isNotEmpty()) { "A transaction must contain at least one command" }
        if (timeWindow != null) check(notary != null) { "Transactions with time-windows must be notarised" }
        privacySalt.validateFor(hashAlgorithm)
    }

    /** The transaction id is represented by the root hash of Merkle tree over the transaction components. */
    override val id: SecureHash get() = merkleTree.hash

    /** Public keys that need to be fulfilled by signatures in order for the transaction to be valid. */
    val requiredSigningKeys: Set<PublicKey>
        get() {
            val commandKeys = commands.flatMap { it.signers }.toSet()
            // TODO: prevent notary field from being set if there are no inputs and no time-window.
            return if (notary != null && (inputs.isNotEmpty() || references.isNotEmpty() || timeWindow != null)) {
                commandKeys + notary.owningKey
            } else {
                commandKeys
            }
        }

    /**
     * Looks up identities and attachments from storage to generate a [LedgerTransaction]. A transaction is expected to
     * have been fully resolved using the resolution flow by this point.
     *
     * @throws AttachmentResolutionException if a required attachment was not found in storage.
     * @throws TransactionResolutionException if an input points to a transaction not found in storage.
     */
    @Throws(AttachmentResolutionException::class, TransactionResolutionException::class)
    @DeleteForDJVM
    fun toLedgerTransaction(services: ServicesForResolution): LedgerTransaction {
        return services.specialise(
            toLedgerTransactionInternal(
                resolveIdentity = { services.identityService.partyFromKey(it) },
                resolveAttachment = { services.attachments.openAttachment(it) },
                resolveStateRefAsSerialized = { resolveStateRefBinaryComponent(it, services) },
                resolveParameters = {
                    val hashToResolve = it ?: services.networkParametersService.defaultHash
                    services.networkParametersService.lookup(hashToResolve)
                },
                // `as?` is used due to [MockServices] not implementing [ServiceHubCoreInternal]
                isAttachmentTrusted = { (services as? ServiceHubCoreInternal)?.attachmentTrustCalculator?.calculate(it) ?: true },
                attachmentsClassLoaderCache = (services as? ServiceHubCoreInternal)?.attachmentsClassLoaderCache
            )
        )
    }

    // Helper for deprecated toLedgerTransaction
    // TODO: revisit once Deterministic JVM code updated
    @Suppress("UNUSED") // not sure if this field can be removed safely??
    private val missingAttachment: Attachment by lazy {
        object : AbstractAttachment({ byteArrayOf() }, DEPLOYED_CORDAPP_UPLOADER ) {
            override val id: SecureHash get() = throw UnsupportedOperationException()
        }
    }

    /**
     * Looks up identities, attachments and dependent input states using the provided lookup functions in order to
     * construct a [LedgerTransaction]. Note that identity lookup failure does *not* cause an exception to be thrown.
     * This invocation doesn't check various rules like no-downgrade or package namespace ownership.
     *
     * @throws AttachmentResolutionException if a required attachment was not found using [resolveAttachment].
     * @throws TransactionResolutionException if an input was not found not using [resolveStateRef].
     */
    @Deprecated("Use toLedgerTransaction(ServicesForResolution) instead")
    @Throws(AttachmentResolutionException::class, TransactionResolutionException::class)
    fun toLedgerTransaction(
            resolveIdentity: (PublicKey) -> Party?,
            resolveAttachment: (SecureHash) -> Attachment?,
            resolveStateRef: (StateRef) -> TransactionState<*>?,
            @Suppress("UNUSED_PARAMETER") resolveContractAttachment: (TransactionState<ContractState>) -> AttachmentId?
    ): LedgerTransaction {
        // This reverts to serializing the resolved transaction state.
        return toLedgerTransactionInternal(
                resolveIdentity,
                resolveAttachment,
                { stateRef -> resolveStateRef(stateRef)?.serialize() },
                { null },
                { it.isUploaderTrusted() },
                null
        )
    }

    // Especially crafted for TransactionVerificationRequest
    @CordaInternal
    internal fun toLtxDjvmInternalBridge(
            resolveAttachment: (SecureHash) -> Attachment?,
            resolveStateRef: (StateRef) -> TransactionState<*>?,
            resolveParameters: (SecureHash?) -> NetworkParameters?
    ): LedgerTransaction {
        return toLedgerTransactionInternal(
                { null },
                resolveAttachment,
                { stateRef -> resolveStateRef(stateRef)?.serialize() },
                resolveParameters,
                { true }, // Any attachment loaded through the DJVM should be trusted
                null
        )
    }

    @Suppress("LongParameterList", "ThrowsCount")
    private fun toLedgerTransactionInternal(
            resolveIdentity: (PublicKey) -> Party?,
            resolveAttachment: (SecureHash) -> Attachment?,
            resolveStateRefAsSerialized: (StateRef) -> SerializedBytes<TransactionState<ContractState>>?,
            resolveParameters: (SecureHash?) -> NetworkParameters?,
            isAttachmentTrusted: (Attachment) -> Boolean,
            attachmentsClassLoaderCache: AttachmentsClassLoaderCache?
    ): LedgerTransaction {
        // Look up public keys to authenticated identities.
        val authenticatedCommands = commands.lazyMapped { cmd, _ ->
            val parties = cmd.signers.mapNotNull { pk -> resolveIdentity(pk) }
            CommandWithParties(cmd.signers, parties, cmd.value)
        }

        val serializedResolvedInputs = inputs.map { ref ->
            SerializedStateAndRef(resolveStateRefAsSerialized(ref) ?: throw TransactionResolutionException(ref.txhash), ref)
        }
        val resolvedInputs = serializedResolvedInputs.lazyMapped { star, _ -> star.toStateAndRef() }

        val serializedResolvedReferences = references.map { ref ->
            SerializedStateAndRef(resolveStateRefAsSerialized(ref) ?: throw TransactionResolutionException(ref.txhash), ref)
        }
        val resolvedReferences = serializedResolvedReferences.lazyMapped { star, _ -> star.toStateAndRef() }

        val resolvedAttachments = attachments.lazyMapped { att, _ -> resolveAttachment(att) ?: throw AttachmentResolutionException(att) }

        val resolvedNetworkParameters = resolveParameters(networkParametersHash) ?: throw TransactionResolutionException.UnknownParametersException(id, networkParametersHash!!)

        val ltx = LedgerTransaction.create(
                resolvedInputs,
                outputs,
                authenticatedCommands,
                resolvedAttachments,
                id,
                notary,
                timeWindow,
                privacySalt,
                resolvedNetworkParameters,
                resolvedReferences,
                componentGroups,
                serializedResolvedInputs,
                serializedResolvedReferences,
                isAttachmentTrusted,
                attachmentsClassLoaderCache
        )

        checkTransactionSize(ltx, resolvedNetworkParameters.maxTransactionSize, serializedResolvedInputs, serializedResolvedReferences)

        return ltx
    }

    /**
     * Deterministic function that checks if the transaction is below the maximum allowed size.
     * It uses the binary representation of transactions.
     */
    private fun checkTransactionSize(ltx: LedgerTransaction,
                                     maxTransactionSize: Int,
                                     resolvedSerializedInputs: List<SerializedStateAndRef>,
                                     resolvedSerializedReferences: List<SerializedStateAndRef>) {
        var remainingTransactionSize = maxTransactionSize

        fun minus(size: Int) {
            require(remainingTransactionSize > size) { "Transaction exceeded network's maximum transaction size limit : $maxTransactionSize bytes." }
            remainingTransactionSize -= size
        }

        // This calculates a value that is slightly lower than the actual re-serialized version. But it is stable and does not depend on the classloader.
        fun componentGroupSize(componentGroup: ComponentGroupEnum): Int {
            return this.componentGroups.firstOrNull { it.groupIndex == componentGroup.ordinal }?.let { cg -> cg.components.sumBy { it.size } + 4 } ?: 0
        }

        // Check attachments size first as they are most likely to go over the limit. With ContractAttachment instances
        // it's likely that the same underlying Attachment CorDapp will occur more than once so we dedup on the attachment id.
        ltx.attachments.distinctBy { it.id }.forEach { minus(it.size) }

        minus(resolvedSerializedInputs.sumBy { it.serializedState.size })
        minus(resolvedSerializedReferences.sumBy { it.serializedState.size })

        // For Commands and outputs we can use the component groups as they are already serialized.
        minus(componentGroupSize(COMMANDS_GROUP))
        minus(componentGroupSize(OUTPUTS_GROUP))
    }

    /**
     * Build filtered transaction using provided filtering functions.
     */
    fun buildFilteredTransaction(filtering: Predicate<Any>): FilteredTransaction =
            FilteredTransaction.buildFilteredTransaction(this, filtering)

    /**
     * Builds whole Merkle tree for a transaction.
     * Briefly, each component group has its own sub Merkle tree and all of the roots of these trees are used as leaves
     * in a top level Merkle tree.
     * Note that ordering of elements inside a [ComponentGroup] matters when computing the Merkle root.
     * On the other hand, insertion group ordering does not affect the top level Merkle tree construction, as it is
     * actually an ordered Merkle tree, where its leaves are ordered based on the group ordinal in [ComponentGroupEnum].
     * If any of the groups is an empty list or a null object, then [SecureHash.allOnesHash] is used as its hash.
     * Also, [privacySalt] is not a Merkle tree leaf, because it is already "inherently" included via the component nonces.
     */
    val merkleTree: MerkleTree by lazy { MerkleTree.getMerkleTree(groupHashes) }

    /**
     * The leaves (group hashes) of the top level Merkle tree.
     * If a group's Merkle root is allOnesHash, it is a flag that denotes this group is empty (if list) or null (if single object)
     * in the wire transaction.
     */
    internal val groupHashes: List<SecureHash> by lazy {
        val listOfLeaves = mutableListOf<SecureHash>()
        // Even if empty and not used, we should at least send oneHashes for each known
        // or received but unknown (thus, bigger than known ordinal) component groups.
        val allOnesHash = SecureHash.allOnesHashFor(hashAlgorithm)
        for (i in 0..componentGroups.map { it.groupIndex }.max()!!) {
            val root = groupsMerkleRoots[i] ?: allOnesHash
            listOfLeaves.add(root)
        }
        listOfLeaves
    }

    /**
     * Calculate the hashes of the existing component groups, that are used to build the transaction's Merkle tree.
     * Each group has its own sub Merkle tree and the hash of the root of this sub tree works as a leaf of the top
     * level Merkle tree. The root of the latter is the transaction identifier.
     *
     * The tree structure is helpful for preserving privacy, please
     * see the user-guide section "Transaction tear-offs" to learn more about this topic.
     */
    internal val groupsMerkleRoots: Map<Int, SecureHash> by lazy {
        availableComponentHashes.entries.associate { it.key to MerkleTree.getMerkleTree(it.value).hash }
    }

    /**
     * Calculate nonces for every transaction component, including new fields (due to backwards compatibility support) we cannot process.
     * Nonce are computed in the following way:
     * nonce1 = H(salt || path_for_1st_component)
     * nonce2 = H(salt || path_for_2nd_component)
     * etc.
     * Thus, all of the nonces are "independent" in the sense that knowing one or some of them, you can learn
     * nothing about the rest.
     */
    internal val availableComponentNonces: Map<Int, List<SecureHash>> by lazy {
        componentGroups.associate { it.groupIndex to it.components.mapIndexed { internalIndex, internalIt -> componentHash(hashAlgorithm, internalIt, privacySalt, it.groupIndex, internalIndex) } }
    }

    /**
     * Calculate hashes for every transaction component. These will be used to build the full Merkle tree.
     * The root of the tree is the transaction identifier. The tree structure is helpful for privacy, please
     * see the user-guide section "Transaction tear-offs" to learn more about this topic.
     */
    internal val availableComponentHashes: Map<Int, List<SecureHash>> by lazy {
        componentGroups.associate { it.groupIndex to it.components.mapIndexed { internalIndex, internalIt -> componentHash(availableComponentNonces[it.groupIndex]!![internalIndex], internalIt) } }
    }

    /**
     * Checks that the given signature matches one of the commands and that it is a correct signature over the tx.
     *
     * @throws [SignatureException] if the signature didn't match the transaction contents.
     * @throws IllegalArgumentException if the signature key doesn't appear in any command.
     */
    fun checkSignature(sig: TransactionSignature) {
        require(commands.any { it.signers.any { sig.by in it.keys } }) { "Signature key doesn't match any command" }
        sig.verify(id)
    }

    companion object {
        @CordaInternal
        @Deprecated("Do not use, this is internal API")
        fun createComponentGroups(inputs: List<StateRef>,
                                  outputs: List<TransactionState<ContractState>>,
                                  commands: List<Command<*>>,
                                  attachments: List<SecureHash>,
                                  notary: Party?,
                                  timeWindow: TimeWindow?): List<ComponentGroup> {
            return createComponentGroups(inputs, outputs, commands, attachments, notary, timeWindow, emptyList(), null)
        }

        /**
         * This is the main logic that knows how to retrieve the binary representation of [StateRef]s.
         *
         * For [ContractUpgradeWireTransaction] or [NotaryChangeWireTransaction] it knows how to recreate the output state in the
         * correct classloader independent of the node's classpath.
         */
        @CordaInternal
        fun resolveStateRefBinaryComponent(stateRef: StateRef, services: ServicesForResolution): SerializedBytes<TransactionState<ContractState>>? {
            return if (services is ServiceHub) {
                val coreTransaction = services.validatedTransactions.getTransaction(stateRef.txhash)?.coreTransaction
                        ?: throw TransactionResolutionException(stateRef.txhash)
                // Get the network parameters from the tx or whatever the default params are.
                val paramsHash = coreTransaction.networkParametersHash ?: services.networkParametersService.defaultHash
                val params = services.networkParametersService.lookup(paramsHash)
                        ?: throw IllegalStateException("Should have been able to fetch parameters by this point: $paramsHash")
                @Suppress("UNCHECKED_CAST")
                when (coreTransaction) {
                    is WireTransaction -> coreTransaction.componentGroups
                            .firstOrNull { it.groupIndex == OUTPUTS_GROUP.ordinal }
                            ?.components
                            ?.get(stateRef.index) as SerializedBytes<TransactionState<ContractState>>?
                    is ContractUpgradeWireTransaction -> coreTransaction.resolveOutputComponent(services, stateRef, params)
                    is NotaryChangeWireTransaction -> coreTransaction.resolveOutputComponent(services, stateRef, params)
                    else -> throw UnsupportedOperationException("Attempting to resolve input ${stateRef.index} of a ${coreTransaction.javaClass} transaction. This is not supported.")
                }
            } else {
                // For backwards compatibility revert to using the node classloader.
                services.loadState(stateRef).serialize()
            }
        }
    }

    @DeleteForDJVM
    override fun toString(): String {
        val buf = StringBuilder()
        buf.appendln("Transaction:")
        for (reference in references) {
            val emoji = Emoji.rightArrow
            buf.appendln("${emoji}REFS:       $reference")
        }
        for (input in inputs) {
            val emoji = Emoji.rightArrow
            buf.appendln("${emoji}INPUT:      $input")
        }
        for ((data) in outputs) {
            val emoji = Emoji.leftArrow
            buf.appendln("${emoji}OUTPUT:     $data")
        }
        for (command in commands) {
            val emoji = Emoji.diamond
            buf.appendln("${emoji}COMMAND:    $command")
        }
        for (attachment in attachments) {
            val emoji = Emoji.paperclip
            buf.appendln("${emoji}ATTACHMENT: $attachment")
        }
        if (networkParametersHash != null) {
            buf.appendln("PARAMETERS HASH:  $networkParametersHash")
        }
        return buf.toString()
    }

    override fun equals(other: Any?): Boolean {
        if (other is WireTransaction) {
            return (this.id == other.id)
        }
        return false
    }

    override fun hashCode(): Int = id.hashCode()
}

/**
 * A ComponentGroup is used to store the full list of transaction components of the same type in serialised form.
 * Practically, a group per component type of a transaction is required; thus, there will be a group for input states,
 * a group for all attachments (if there are any) etc.
 */
@CordaSerializable
open class ComponentGroup(open val groupIndex: Int, open val components: List<OpaqueBytes>)
