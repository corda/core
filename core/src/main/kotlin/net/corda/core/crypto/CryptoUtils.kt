@file:Suppress("MatchingDeclarationName")
@file:KeepForDJVM
@file:JvmName("CryptoUtils")

package net.corda.core.crypto

import net.corda.core.DeleteForDJVM
import net.corda.core.KeepForDJVM
import net.corda.core.contracts.PrivacySalt
import net.corda.core.crypto.internal.platformSecureRandomFactory
import net.corda.core.serialization.SerializationDefaults
import net.corda.core.serialization.serialize
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.toBase58
import net.corda.core.utilities.toSHA256Bytes
import java.math.BigInteger
import java.nio.ByteBuffer
import java.security.InvalidKeyException
import java.security.KeyPair
import java.security.NoSuchAlgorithmException
import java.security.PrivateKey
import java.security.PublicKey
import java.security.SecureRandom
import java.security.SecureRandomSpi
import java.security.SignatureException

/**
 * Utility to simplify the act of signing a byte array.
 * @param bytesToSign the data/message to be signed in [ByteArray] form (usually the Merkle root).
 * @return the [DigitalSignature] object on the input message.
 * @throws IllegalArgumentException if the signature scheme is not supported for this private key.
 * @throws InvalidKeyException if the private key is invalid.
 * @throws SignatureException if signing is not possible due to malformed data or private key.
 */
@DeleteForDJVM
@Throws(InvalidKeyException::class, SignatureException::class)
fun PrivateKey.sign(bytesToSign: ByteArray): DigitalSignature = DigitalSignature(Crypto.doSign(this, bytesToSign))

/**
 * Utility to simplify the act of signing a byte array and return a [DigitalSignature.WithKey] object.
 * Note that there is no check if the public key matches with the signing private key.
 * @param bytesToSign the data/message to be signed in [ByteArray] form (usually the Merkle root).
 * @return the [DigitalSignature.WithKey] object on the input message [bytesToSign] and [publicKey].
 * @throws IllegalArgumentException if the signature scheme is not supported for this private key.
 * @throws InvalidKeyException if the private key is invalid.
 * @throws SignatureException if signing is not possible due to malformed data or private key.
 */
@DeleteForDJVM
@Throws(InvalidKeyException::class, SignatureException::class)
fun PrivateKey.sign(bytesToSign: ByteArray, publicKey: PublicKey): DigitalSignature.WithKey {
    return DigitalSignature.WithKey(publicKey, this.sign(bytesToSign).bytes)
}

/**
 * Helper function to sign with a key pair.
 * @param bytesToSign the data/message to be signed in [ByteArray] form (usually the Merkle root).
 * @return the digital signature (in [ByteArray]) on the input message.
 * @throws IllegalArgumentException if the signature scheme is not supported for this private key.
 * @throws InvalidKeyException if the private key is invalid.
 * @throws SignatureException if signing is not possible due to malformed data or private key.
 */
@DeleteForDJVM
@Throws(InvalidKeyException::class, SignatureException::class)
fun KeyPair.sign(bytesToSign: ByteArray): DigitalSignature.WithKey = private.sign(bytesToSign, public)

/** Helper function to sign the bytes of [bytesToSign] with a key pair. */
@DeleteForDJVM
@Throws(InvalidKeyException::class, SignatureException::class)
fun KeyPair.sign(bytesToSign: OpaqueBytes): DigitalSignature.WithKey = sign(bytesToSign.bytes)

/**
 * Helper function for signing a [SignableData] object.
 * @param signableData the object to be signed.
 * @return a [TransactionSignature] object.
 * @throws IllegalArgumentException if the signature scheme is not supported for this private key.
 * @throws InvalidKeyException if the private key is invalid.
 * @throws SignatureException if signing is not possible due to malformed data or private key.
 */
@DeleteForDJVM
@Throws(InvalidKeyException::class, SignatureException::class)
fun KeyPair.sign(signableData: SignableData): TransactionSignature = Crypto.doSign(this, signableData)

/**
 * Utility to simplify the act of verifying a signature.
 *
 * @throws InvalidKeyException if the key to verify the signature with is not valid (i.e. wrong key type for the
 * signature).
 * @throws SignatureException if the signature is invalid (i.e. damaged), or does not match the key (incorrect).
 * @throws IllegalArgumentException if the signature scheme is not supported or if any of the clear or signature data is empty.
 */
// TODO: SignatureException should be used only for a damaged signature, as per `java.security.Signature.verify()`.
@Throws(SignatureException::class, InvalidKeyException::class)
fun PublicKey.verify(content: ByteArray, signature: DigitalSignature) = Crypto.doVerify(this, signature.bytes, content)

/**
 * Utility to simplify the act of verifying a signature. In comparison to [verify] if the key and signature
 * do not match it returns false rather than throwing an exception. Normally you should use the function which throws,
 * as it avoids the risk of failing to test the result, but this is for uses such as [java.security.Signature.verify]
 * implementations.
 *
 * @throws InvalidKeyException if the key to verify the signature with is not valid (i.e. wrong key type for the
 * signature).
 * @throws SignatureException if the signature is invalid (i.e. damaged).
 * @throws IllegalArgumentException if the signature scheme is not supported or if any of the clear or signature data is empty.
 * @throws IllegalStateException if this is a [CompositeKey], because verification of composite key signatures is not supported.
 * @return whether the signature is correct for this key.
 */
@Throws(SignatureException::class, InvalidKeyException::class)
fun PublicKey.isValid(content: ByteArray, signature: DigitalSignature): Boolean {
    if (this is CompositeKey)
        throw IllegalStateException("Verification of CompositeKey signatures currently not supported.") // TODO CompositeSignature verification.
    return Crypto.isValid(this, signature.bytes, content)
}

/** Render a public key to its hash (in Base58) of its serialised form using the DL prefix. */
fun PublicKey.toStringShort(): String = "DL" + this.toSHA256Bytes().toBase58()

/**
 * Return a [Set] of the contained leaf keys if this is a [CompositeKey].
 * Otherwise, return a [Set] with a single element (this [PublicKey]).
 * <i>Note that leaf keys cannot be of type [CompositeKey].</i>
 */
val PublicKey.keys: Set<PublicKey> get() = (this as? CompositeKey)?.leafKeys ?: setOf(this)

/** Return true if [otherKey] fulfils the requirements of this [PublicKey]. */
fun PublicKey.isFulfilledBy(otherKey: PublicKey): Boolean = isFulfilledBy(setOf(otherKey))

/** Return true if [otherKeys] fulfil the requirements of this [PublicKey]. */
fun PublicKey.isFulfilledBy(otherKeys: Iterable<PublicKey>): Boolean = (this as? CompositeKey)?.isFulfilledBy(otherKeys) ?: (this in otherKeys)

/**
 * Checks whether any of the given [keys] matches a leaf on the [CompositeKey] tree or a single [PublicKey].
 *
 * <i>Note that this function checks against leaves, which cannot be of type [CompositeKey]. Due to that, if any of the
 * [otherKeys] is a [CompositeKey], this function will not find a match.</i>
 */
fun PublicKey.containsAny(otherKeys: Iterable<PublicKey>): Boolean {
    return if (this is CompositeKey) keys.intersect(otherKeys).isNotEmpty()
    else this in otherKeys
}

/** Returns the set of all [PublicKey]s of the signatures. */
fun Iterable<TransactionSignature>.byKeys() = map { it.by }.toSet()

// Allow Kotlin destructuring:
// val (private, public) = keyPair
/* The [PrivateKey] of this [KeyPair]. */
operator fun KeyPair.component1(): PrivateKey = this.private

/* The [PublicKey] of this [KeyPair]. */
operator fun KeyPair.component2(): PublicKey = this.public

/** A simple wrapper that will make it easier to swap out the signature algorithm we use in future. */
@DeleteForDJVM
fun generateKeyPair(): KeyPair = Crypto.generateKeyPair()

/**
 * Returns a key pair derived from the given private key entropy. This is useful for unit tests and other cases where
 * you want hard-coded private keys.
 * @param entropy a [BigInteger] value.
 * @return a deterministically generated [KeyPair] for the [Crypto.DEFAULT_SIGNATURE_SCHEME].
 */
fun entropyToKeyPair(entropy: BigInteger): KeyPair = Crypto.deriveKeyPairFromEntropy(entropy)

/**
 * Helper function to verify a signature.
 * @param signatureData the signature on a message.
 * @param clearData the clear data/message that was signed (usually the Merkle root).
 * @throws InvalidKeyException if the key is invalid.
 * @throws SignatureException if this signatureData object is not initialized properly,
 * the passed-in signatureData is improperly encoded or of the wrong type,
 * if this signatureData algorithm is unable to process the input data provided, etc.
 * @throws IllegalArgumentException if the signature scheme is not supported for this private key or if any of the clear or signature data is empty.
 */
@Throws(InvalidKeyException::class, SignatureException::class)
fun PublicKey.verify(signatureData: ByteArray, clearData: ByteArray): Boolean = Crypto.doVerify(this, signatureData, clearData)

/**
 * Helper function for the signers to verify their own signature.
 * @param signatureData the signature on a message.
 * @param clearData the clear data/message that was signed (usually the Merkle root).
 * @throws InvalidKeyException if the key is invalid.
 * @throws SignatureException if this signatureData object is not initialized properly,
 * the passed-in signatureData is improperly encoded or of the wrong type,
 * if this signatureData algorithm is unable to process the input data provided, etc.
 * @throws IllegalArgumentException if the signature scheme is not supported for this private key or if any of the clear or signature data is empty.
 */
@Throws(InvalidKeyException::class, SignatureException::class)
fun KeyPair.verify(signatureData: ByteArray, clearData: ByteArray): Boolean = Crypto.doVerify(this.public, signatureData, clearData)

/**
 * Generate a securely random [ByteArray] of requested number of bytes. Usually used for seeds, nonces and keys.
 * @param numOfBytes how many random bytes to output.
 * @return a random [ByteArray].
 * @throws NoSuchAlgorithmException thrown if "NativePRNGNonBlocking" is not supported on the JVM
 * or if no strong [SecureRandom] implementations are available or if Security.getProperty("securerandom.strongAlgorithms") is null or empty,
 * which should never happen and suggests an unusual JVM or non-standard Java library.
 */
@DeleteForDJVM
@Throws(NoSuchAlgorithmException::class)
fun secureRandomBytes(numOfBytes: Int): ByteArray = ByteArray(numOfBytes).apply { newSecureRandom().nextBytes(this) }

/**
 * This is a hack added because during deserialisation when no-param constructors are called sometimes default values
 * generate random numbers, which fail in SGX.
 * TODO remove this once deserialisation is figured out.
 */
private class DummySecureRandomSpi : SecureRandomSpi() {
    override fun engineSetSeed(bytes: ByteArray?) {
        Exception("DummySecureRandomSpi.engineSetSeed called").printStackTrace(System.out)
    }

    override fun engineNextBytes(bytes: ByteArray?) {
        Exception("DummySecureRandomSpi.engineNextBytes called").printStackTrace(System.out)
        bytes?.fill(0)
    }

    override fun engineGenerateSeed(numberOfBytes: Int): ByteArray {
        Exception("DummySecureRandomSpi.engineGenerateSeed called").printStackTrace(System.out)
        return ByteArray(numberOfBytes)
    }
}

object DummySecureRandom : SecureRandom(DummySecureRandomSpi(), null)

/**
 * Get an instance of [SecureRandom] to avoid blocking, due to waiting for additional entropy, when possible.
 * In this version, the NativePRNGNonBlocking is exclusively used on Linux OS to utilize dev/urandom because in high traffic
 * /dev/random may wait for a certain amount of "noise" to be generated on the host machine before returning a result.
 *
 * On Solaris, Linux, and OS X, if the entropy gathering device in java.security is set to file:/dev/urandom
 * or file:/dev/random, then NativePRNG is preferred to SHA1PRNG. Otherwise, SHA1PRNG is preferred.
 * @see <a href="https://docs.oracle.com/javase/8/docs/technotes/guides/security/SunProviders.html#SecureRandomImp">SecureRandom Implementation</a>.
 *
 * If both dev/random and dev/urandom are available, then dev/random is only preferred over dev/urandom during VM boot
 * where it may be possible that OS didn't yet collect enough entropy to fill the randomness pool for the 1st time.
 * @see <a href="http://www.2uo.de/myths-about-urandom/">Myths about urandom</a> for a more descriptive explanation on /dev/random Vs /dev/urandom.
 * TODO: check default settings per OS and random/urandom availability.
 * @return a [SecureRandom] object.
 * @throws NoSuchAlgorithmException thrown if "NativePRNGNonBlocking" is not supported on the JVM
 * or if no strong SecureRandom implementations are available or if Security.getProperty("securerandom.strongAlgorithms") is null or empty,
 * which should never happen and suggests an unusual JVM or non-standard Java library.
 */
@DeleteForDJVM
@Throws(NoSuchAlgorithmException::class)
fun newSecureRandom(): SecureRandom = platformSecureRandomFactory()

/**
 * Returns a random positive non-zero long generated using a secure RNG. This function sacrifies a bit of entropy in order
 * to avoid potential bugs where the value is used in a context where negative numbers or zero are not expected.
 */
@DeleteForDJVM
fun random63BitValue(): Long {
    while (true) {
        val candidate = Math.abs(newSecureRandom().nextLong())
        // No need to check for -0L
        if (candidate != 0L && candidate != Long.MIN_VALUE) {
            return candidate
        }
    }
}

/**
 * Compute the hash of each serialised component so as to be used as Merkle tree leaf. The resultant output (leaf) is
 * calculated using the SHA256d algorithm, thus SHA256(SHA256(nonce || serializedComponent)), where nonce is computed
 * from [computeNonce].
 */
fun componentHash(opaqueBytes: OpaqueBytes, privacySalt: PrivacySalt, componentGroupIndex: Int, internalIndex: Int): SecureHash =
        componentHash(computeNonce(privacySalt, componentGroupIndex, internalIndex), opaqueBytes)

/**
 * Compute the hash of each serialised component so as to be used as Merkle tree leaf. The resultant output (leaf) is
 * calculated using the SHA256d algorithm, thus SHA256(SHA256(nonce || serializedComponent)), where nonce is computed
 * from [computeNonce].
 */
fun componentHash(algorithm: String, opaqueBytes: OpaqueBytes, privacySalt: PrivacySalt, componentGroupIndex: Int, internalIndex: Int): SecureHash =
        componentHash(computeNonce(algorithm, privacySalt, componentGroupIndex, internalIndex), opaqueBytes)

/** Return the HASH(HASH(nonce || serializedComponent)). */
fun componentHash(nonce: SecureHash, opaqueBytes: OpaqueBytes): SecureHash = SecureHash.hashTwiceAs(nonce.algorithm, nonce.bytes + opaqueBytes.bytes)

/**
 * Serialise the object and return the hash of the serialized bytes. Note that the resulting hash may not be deterministic
 * across platform versions: serialization can produce different values if any of the types being serialized have changed,
 * or if the version of serialization specified by the context changes.
 */
fun <T : Any> serializedHash(x: T): SecureHash = x.serialize(context = SerializationDefaults.P2P_CONTEXT.withoutReferences()).bytes.sha256()

/**
 * Method to compute a nonce based on privacySalt, component group index and component internal index.
 * SHA256d (double SHA256) is used to prevent length extension attacks.
 * @param privacySalt a [PrivacySalt].
 * @param groupIndex the fixed index (ordinal) of this component group.
 * @param internalIndex the internal index of this object in its corresponding components list.
 * @return SHA256(SHA256(privacySalt || groupIndex || internalIndex))
 */
fun computeNonce(privacySalt: PrivacySalt, groupIndex: Int, internalIndex: Int) = SecureHash.sha256Twice(privacySalt.bytes + ByteBuffer.allocate(8).putInt(groupIndex).putInt(internalIndex).array())

/**
 * Method to compute a nonce based on privacySalt, component group index and component internal index.
 * Double [algorithm] is used to prevent length extension attacks.
 * @param algorithm The [java.security.MessageDigest] algorithm to use.
 * @param privacySalt a [PrivacySalt].
 * @param groupIndex the fixed index (ordinal) of this component group.
 * @param internalIndex the internal index of this object in its corresponding components list.
 * @return SHA256(SHA256(privacySalt || groupIndex || internalIndex))
 */
fun computeNonce(algorithm: String, privacySalt: PrivacySalt, groupIndex: Int, internalIndex: Int) = SecureHash.hashTwiceAs(algorithm, privacySalt.bytes + ByteBuffer.allocate(8).putInt(groupIndex).putInt(internalIndex).array())

