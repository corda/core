package net.corda.core.transactions

import net.corda.core.contracts.*
import net.corda.core.crypto.*
import net.corda.core.identity.Party
import net.corda.core.internal.Emoji
import net.corda.core.node.ServicesForResolution
import net.corda.core.utilities.OpaqueBytes
import java.security.PublicKey
import java.security.SignatureException
import java.util.function.Predicate

/**
 * A transaction ready for serialisation, without any signatures attached. A WireTransaction is usually wrapped
 * by a [SignedTransaction] that carries the signatures over this payload.
 * The identity of the transaction is the Merkle tree root of its components (see [MerkleTree]).
 */
data class WireTransaction(
        /** Pointers to the input states on the ledger, identified by (tx identity hash, output index). */
        override val inputs: List<StateRef>,
        /** Hashes of the ZIP/JAR files that are needed to interpret the contents of this wire transaction. */
        override val attachments: List<SecureHash>,
        override val outputs: List<TransactionState<ContractState>>,
        /** Ordered list of ([CommandData], [PublicKey]) pairs that instruct the contracts what to do. */
        override val commands: List<Command<*>>,
        override val notary: Party?,
        // TODO: remove type
        override val type: TransactionType,
        override val timeWindow: TimeWindow?,
        /**
         * For privacy purposes, each part of a transaction should be accompanied by a nonce.
         * To avoid storing a random number (nonce) per component, an initial "salt" is the sole value utilised,
         * so that all component nonces are deterministically computed in the following way:
         * nonce1 = H(salt || 1)
         * nonce2 = H(salt || 2)
         *
         * Thus, all of the nonces are "independent" in the sense that knowing one or some of them, you can learn
         * nothing about the rest.
         */
        override val privacySalt: PrivacySalt = PrivacySalt(secureRandomBytes(32))
) : CoreTransaction(), TraversableTransaction {
    init {
        checkBaseInvariants()
        if (timeWindow != null) check(notary != null) { "Transactions with time-windows must be notarised" }
        check(availableComponents.isNotEmpty()) { "A WireTransaction cannot be empty" }
    }

    /** The transaction id is represented by the root hash of Merkle tree over the transaction components. */
    override val id: SecureHash get() = merkleTree.hash

    override val availableComponents: List<Any>
        get() = listOf(inputs, attachments, outputs, commands).flatten() + listOf(notary, type, timeWindow).filterNotNull()

    /** Public keys that need to be fulfilled by signatures in order for the transaction to be valid. */
    val requiredSigningKeys: Set<PublicKey> get() {
        val commandKeys = commands.flatMap { it.signers }.toSet()
        // TODO: prevent notary field from being set if there are no inputs and no timestamp
        return if (notary != null && (inputs.isNotEmpty() || timeWindow != null)) {
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
    fun toLedgerTransaction(services: ServicesForResolution): LedgerTransaction {
        return toLedgerTransaction(
                resolveIdentity = { services.identityService.partyFromKey(it) },
                resolveAttachment = { services.attachments.openAttachment(it) },
                resolveStateRef = { services.loadState(it) }
        )
    }

    /**
     * Looks up identities, attachments and dependent input states using the provided lookup functions in order to
     * construct a [LedgerTransaction]. Note that identity lookup failure does *not* cause an exception to be thrown.
     *
     * @throws AttachmentResolutionException if a required attachment was not found using [resolveAttachment].
     * @throws TransactionResolutionException if an input was not found not using [resolveStateRef].
     */
    @Throws(AttachmentResolutionException::class, TransactionResolutionException::class)
    fun toLedgerTransaction(
            resolveIdentity: (PublicKey) -> Party?,
            resolveAttachment: (SecureHash) -> Attachment?,
            resolveStateRef: (StateRef) -> TransactionState<*>?
    ): LedgerTransaction {
        // Look up public keys to authenticated identities. This is just a stub placeholder and will all change in future.
        val authenticatedArgs = commands.map {
            val parties = it.signers.mapNotNull { pk -> resolveIdentity(pk) }
            AuthenticatedObject(it.signers, parties, it.value)
        }
        // Open attachments specified in this transaction. If we haven't downloaded them, we fail.
        val attachments = attachments.map { resolveAttachment(it) ?: throw AttachmentResolutionException(it) }
        val resolvedInputs = inputs.map { ref ->
            resolveStateRef(ref)?.let { StateAndRef(it, ref) } ?: throw TransactionResolutionException(ref.txhash)
        }
        return LedgerTransaction(resolvedInputs, outputs, authenticatedArgs, attachments, id, notary, timeWindow, type, privacySalt)
    }

    /**
     * Build filtered transaction using provided filtering functions.
     */
    fun buildFilteredTransaction(filtering: Predicate<Any>): FilteredTransaction {
        return FilteredTransaction.buildMerkleTransaction(this, filtering)
    }

    /**
     * Builds whole Merkle tree for a transaction.
     */
    val merkleTree: MerkleTree by lazy { MerkleTree.getMerkleTree(availableComponentHashes) }

    /**
     * Construction of partial transaction from WireTransaction based on filtering.
     * Note that list of nonces to be sent is updated on the fly, based on the index of the filtered tx component.
     * @param filtering filtering over the whole WireTransaction
     * @returns FilteredLeaves used in PartialMerkleTree calculation and verification.
     */
    fun filterWithFun(filtering: Predicate<Any>): FilteredLeaves {
        val nonces: MutableList<SecureHash> = mutableListOf()

        fun notNullFalseAndNonceUpdate(elem: Any?, index: Int, nonces: MutableList<SecureHash>): Any? {
            return if (elem == null || !filtering.test(elem))
                null
            else {
                nonces.add(computeNonce(privacySalt, index))
                elem
            }
        }

        fun notNullFalse(elem: Any?): Any? = if (elem == null || !filtering.test(elem)) null else elem

        fun<T : Any> filterAndNoncesUpdate(filtering: Predicate<Any>, t: T, index: Int, nonces: MutableList<SecureHash>): Boolean {
            return if (filtering.test(t)) {
                nonces.add(computeNonce(privacySalt, index))
                true
            } else
                false
        }

        return FilteredLeaves(
                inputs.filterIndexed { index, it -> filterAndNoncesUpdate(filtering, it, index, nonces) },
                attachments.filterIndexed { index, it -> filterAndNoncesUpdate(filtering, it, index + offsets[0], nonces) },
                outputs.filterIndexed { index, it -> filterAndNoncesUpdate(filtering, it, index + offsets[1], nonces) },
                commands.filterIndexed { index, it -> filterAndNoncesUpdate(filtering, it, index + offsets[2], nonces) },
                notNullFalseAndNonceUpdate(notary, offsets[3], nonces) as Party?,
                notNullFalseAndNonceUpdate(type, offsets[4], nonces) as TransactionType?,
                notNullFalseAndNonceUpdate(timeWindow, offsets[5], nonces) as TimeWindow?,
                notNullFalse(privacySalt) as PrivacySalt?, // PrivacySalt doesn't need an accompanied nonce.
                nonces
        )
    }

    private fun indexOffsets(): List<Int> {
        // No need to add zero index for inputs, thus offsets[0] corresponds to attachments and offsets[1] to outputs.
        val offsets = mutableListOf(inputs.size, inputs.size + attachments.size)
        offsets.add(offsets.last() + outputs.size)
        offsets.add(offsets.last() + commands.size)
        if (notary != null)
            offsets.add(offsets.last() + 1)
        else
            offsets.add(offsets.last())
        offsets.add(offsets.last() + 1) // For tx type.
        if (timeWindow != null)
            offsets.add(offsets.last() + 1)
        else
            offsets.add(offsets.last())
        // No need to add offset for privacySalt as it doesn't require a nonce.
        return offsets
    }

    val offsets: List<Int> by lazy { indexOffsets() }

    /**
     * Checks that the given signature matches one of the commands and that it is a correct signature over the tx.
     *
     * @throws SignatureException if the signature didn't match the transaction contents.
     * @throws IllegalArgumentException if the signature key doesn't appear in any command.
     */
    fun checkSignature(sig: DigitalSignature.WithKey) {
        require(commands.any { it.signers.any { sig.by in it.keys } }) { "Signature key doesn't match any command" }
        sig.verify(id)
    }

    override fun toString(): String {
        val buf = StringBuilder()
        buf.appendln("Transaction:")
        for (input in inputs) buf.appendln("${Emoji.rightArrow}INPUT:      $input")
        for ((data) in outputs) buf.appendln("${Emoji.leftArrow}OUTPUT:     $data")
        for (command in commands) buf.appendln("${Emoji.diamond}COMMAND:    $command")
        for (attachment in attachments) buf.appendln("${Emoji.paperclip}ATTACHMENT: $attachment")
        return buf.toString()
    }
}
