package net.corda.core.transactions

import net.corda.core.DoNotImplement
import net.corda.core.KeepForDJVM
import net.corda.core.contracts.NamedByHash
import net.corda.core.crypto.TransactionSignature
import net.corda.core.crypto.isFulfilledBy
import net.corda.core.transactions.SignedTransaction.SignaturesMissingException
import net.corda.core.utilities.toNonEmptySet
import java.security.InvalidKeyException
import java.security.PublicKey
import java.security.SignatureException
import java.util.*

/** An interface for transactions containing signatures, with logic for signature verification. */
@KeepForDJVM
@DoNotImplement
interface TransactionWithSignatures : NamedByHash {
    /**
     * List of signatures on this transaction.
     * @see checkSignaturesAreValid
     * @see verifyRequiredSignatures
     */
    val sigs: List<TransactionSignature>

    /** Specifies all the public keys that require signatures for the transaction to be valid. */
    val requiredSigningKeys: Set<PublicKey>

    /**
     * Verifies the signatures on this transaction and throws if any are missing. In this context, "verifying" means
     * checking they are valid signatures and that their public keys are in the [requiredSigningKeys] set.
     *
     * @throws SignatureException if any signatures are invalid or unrecognised.
     * @throws SignaturesMissingException if any signatures should have been present but were not.
     */
    @JvmDefault
    @Throws(SignatureException::class)
    fun verifyRequiredSignatures() = verifySignaturesExcept(emptySet())

    /**
     * Verifies the signatures on this transaction and throws if any are missing which aren't passed as parameters.
     * In this context, "verifying" means checking they are valid signatures and that their public keys are in
     * the [requiredSigningKeys] set.
     *
     * Normally you would not provide any keys to this function, but if you're in the process of building a partial
     * transaction and you want to access the contents before you've signed it, you can specify your own keys here
     * to bypass that check.
     *
     * @throws SignatureException if any signatures are invalid or unrecognised.
     * @throws SignaturesMissingException if any signatures should have been present but were not.
     */
    @JvmDefault
    @Throws(SignatureException::class)
    fun verifySignaturesExcept(vararg allowedToBeMissing: PublicKey) {
        verifySignaturesExcept(Arrays.asList(*allowedToBeMissing))
    }

    /**
     * Verifies the signatures on this transaction and throws if any are missing which aren't passed as parameters.
     * In this context, "verifying" means checking they are valid signatures and that their public keys are in
     * the [requiredSigningKeys] set.
     *
     * Normally you would not provide any keys to this function, but if you're in the process of building a partial
     * transaction and you want to access the contents before you've signed it, you can specify your own keys here
     * to bypass that check.
     *
     * @throws SignatureException if any signatures are invalid or unrecognised.
     * @throws SignaturesMissingException if any signatures should have been present but were not.
     */
    @JvmDefault
    @Throws(SignatureException::class)
    fun verifySignaturesExcept(allowedToBeMissing: Collection<PublicKey>) {
        val needed = getMissingSigners() - allowedToBeMissing
        if (needed.isNotEmpty())
            throw SignaturesMissingException(needed.toNonEmptySet(), getKeyDescriptions(needed), id)
        checkSignaturesAreValid()
    }

    /**
     * Mathematically validates the signatures that are present on this transaction. This does not imply that
     * the signatures are by the right keys, or that there are sufficient signatures, just that they aren't
     * corrupt. If you use this function directly you'll need to do the other checks yourself. Probably you
     * want [verifyRequiredSignatures] instead.
     *
     * @throws InvalidKeyException if the key on a signature is invalid.
     * @throws SignatureException if a signature fails to verify.
     */
    @JvmDefault
    @Throws(InvalidKeyException::class, SignatureException::class)
    fun checkSignaturesAreValid() {
        for (transactionSignature in sigs.parallelStream()) {
            transactionSignature.verify(id)
        }
    }

    /**
     * Get a human readable description of where signatures are required from, and are missing, to assist in debugging
     * the underlying cause.
     *
     * Note that the results should not be serialised, parsed or expected to remain stable between Corda versions.
     */
    fun getKeyDescriptions(keys: Set<PublicKey>): List<String>

    /**
     * Return the [PublicKey]s for which we still need signatures.
     */
    @JvmDefault
    fun getMissingSigners(): Set<PublicKey> {
        val sigKeys = sigs.map { it.by }.toSet()
        // TODO Problem is that we can get single PublicKey wrapped as CompositeKey in allowedToBeMissing/mustSign
        //  equals on CompositeKey won't catch this case (do we want to single PublicKey be equal to the same key wrapped in CompositeKey with threshold 1?)
        return requiredSigningKeys.filter { !it.isFulfilledBy(sigKeys) }.toSet()
    }
}