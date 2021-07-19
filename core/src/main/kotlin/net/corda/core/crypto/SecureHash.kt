@file:Suppress("TooManyFunctions", "MagicNumber")
@file:KeepForDJVM
package net.corda.core.crypto

import io.netty.util.concurrent.FastThreadLocal
import net.corda.core.DeleteForDJVM
import net.corda.core.KeepForDJVM
import net.corda.core.serialization.CordaSerializable
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.parseAsHex
import net.corda.core.utilities.toHexString
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.util.Collections.unmodifiableSet
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.function.Supplier

/**
 * Container for a cryptographically secure hash value.
 * Provides utilities for generating a cryptographic hash using different algorithms (currently only SHA-256 supported).
 */
@KeepForDJVM
@CordaSerializable
sealed class SecureHash constructor(val algorithm: String, bytes: ByteArray) : OpaqueBytes(bytes) {
    constructor(bytes: ByteArray): this(SHA2_256, bytes)

    /** SHA-256 is part of the SHA-2 hash function family. Generated hash is fixed size, 256-bits (32-bytes). */
    class SHA256(bytes: ByteArray) : SecureHash(bytes) {
        init {
            require(bytes.size == 32) { "Invalid hash size, must be 32 bytes" }
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is SHA256) return false
            if (!super.equals(other)) return false
            return true
        }

        // This is an efficient hashCode, because there is no point in performing a hash calculation on a cryptographic hash.
        // It just takes the first 4 bytes and transforms them into an Int.
        override fun hashCode() = ByteBuffer.wrap(bytes).int

        /**
         * Convert the hash value to an uppercase hexadecimal [String].
         */
        override fun toString() = toHexString()

        override fun generate(data: ByteArray): SecureHash {
            return data.sha256()
        }
    }

    class HASH(algorithm: String, bytes: ByteArray) : SecureHash(algorithm, bytes) {
        override fun equals(other: Any?): Boolean {
            return when {
                this === other -> true
                other !is HASH -> false
                else -> algorithm == other.algorithm && super.equals(other)
            }
        }

        override fun hashCode() = ByteBuffer.wrap(bytes).int

        override fun generate(data: ByteArray): SecureHash {
            return HASH(algorithm, digestAs(algorithm, data))
        }
    }

    fun toHexString(): String = bytes.toHexString()

    override fun toString(): String {
        return "$algorithm$DELIMITER${toHexString()}"
    }

    /**
     * Returns the first [prefixLen] hexadecimal digits of the [SecureHash] value.
     * @param prefixLen The number of characters in the prefix.
     */
    fun prefixChars(prefixLen: Int = 6) = toHexString().substring(0, prefixLen)

    /**
     * Append a second hash value to this hash value, and then compute the SHA-256 hash of the result.
     * @param other The hash to append to this one.
     */
    fun hashConcat(other: SecureHash) = (this.bytes + other.bytes).sha256()

    /**
     * Append a second hash value to this hash value, and then compute the hash of the result.
     * @param other The hash to append to this one.
     */
    fun concatenate(other: SecureHash): SecureHash {
        require(algorithm == other.algorithm) {
            "Cannot concatenate $algorithm with ${other.algorithm}"
        }
        return generate(this.bytes + other.bytes)
    }

    protected open fun generate(data: ByteArray): SecureHash {
        throw UnsupportedOperationException("Not implemented for $algorithm")
    }

    // Like static methods in Java, except the 'companion' is a singleton that can have state.
    companion object {
        const val SHA2_256 = "SHA-256"
        const val SHA3_256 = "SHA3-256"
        const val DELIMITER = ':'

        private val BANNED: Set<String> = unmodifiableSet(setOf("MD5", "MD2", "SHA-1"))

        @JvmStatic
        fun create(str: String?): SecureHash {
            val txt = str ?: throw IllegalArgumentException("Provided string is null")
            val idx = txt.indexOf(DELIMITER)
            return if (idx == -1) {
                parse(txt)
            } else {
                val algorithm = txt.substring(0, idx).toUpperCase()
                val value = txt.substring(idx + 1)
                if (algorithm == SHA2_256) {
                    parse(value)
                } else {
                    decode(algorithm, value)
                }
            }
        }

        /**
         * @param algorithm [MessageDigest] algorithm name, in uppercase.
         * @param value Hash value as a hexadecimal string.
         */
        private fun decode(algorithm: String, value: String): SecureHash {
            val digestLength = digestFor(algorithm).digestLength
            val data = value.parseAsHex()
            return when (data.size) {
                digestLength -> HASH(algorithm, data)
                else -> throw IllegalArgumentException("Provided string is ${data.size} bytes not $digestLength bytes in hex: $value")
            }
        }

        /**
         * Converts a SHA-256 hash value represented as a hexadecimal [String] into a [SecureHash].
         * @param str A sequence of 64 hexadecimal digits that represents a SHA-256 hash value.
         * @throws IllegalArgumentException The input string does not contain 64 hexadecimal digits, or it contains incorrectly-encoded characters.
         */
        @JvmStatic
        fun parse(str: String?): SHA256 {
            return str?.toUpperCase()?.parseAsHex()?.let {
                when (it.size) {
                    32 -> SHA256(it)
                    else -> throw IllegalArgumentException("Provided string is ${it.size} bytes not 32 bytes in hex: $str")
                }
            } ?: throw IllegalArgumentException("Provided string is null")
        }

        private val messageDigests: ConcurrentMap<String, DigestSupplier> = ConcurrentHashMap()

        private fun digestFor(algorithm: String): DigestSupplier {
            require(algorithm !in BANNED) {
                "$algorithm is forbidden!"
            }
            return messageDigests.computeIfAbsent(algorithm, ::DigestSupplier)
        }

        private fun digestAs(algorithm: String, bytes: ByteArray): ByteArray = digestFor(algorithm).get().digest(bytes)

        /**
         * @param algorithm The [MessageDigest] algorithm to query.
         * @return The length in bytes of this [MessageDigest].
         */
        fun digestLengthFor(algorithm: String): Int {
            return digestFor(algorithm.toUpperCase()).digestLength
        }

        /**
         * Computes the hash value of the [ByteArray].
         * @param algorithm Java provider name of the digest algorithm.
         * @param bytes The [ByteArray] to hash.
         */
        @JvmStatic
        fun hashAs(algorithm: String, bytes: ByteArray): SecureHash {
            val upperAlgorithm = algorithm.toUpperCase()
            val hashBytes = digestAs(upperAlgorithm, bytes)
            return if (upperAlgorithm == SHA2_256) {
                SHA256(hashBytes)
            } else {
                HASH(upperAlgorithm, hashBytes)
            }
        }

        /**
         * Computes the hash of the [ByteArray], and then computes the hash of the hash.
         * @param algorithm The [MessageDigest] algorithm to use.
         * @param bytes The [ByteArray] to hash.
         */
        @JvmStatic
        fun hashTwiceAs(algorithm: String, bytes: ByteArray): SecureHash {
            val upperAlgorithm = algorithm.toUpperCase()
            return if (upperAlgorithm == SHA2_256) {
                sha256Twice(bytes)
            } else {
                val digest = digestFor(upperAlgorithm).get()
                val firstHash = digest.digest(bytes)
                HASH(upperAlgorithm, digest.digest(firstHash))
            }
        }

        /**
         * Computes the SHA-256 hash value of the [ByteArray].
         * @param bytes The [ByteArray] to hash.
         */
        @JvmStatic
        fun sha256(bytes: ByteArray) = SHA256(digestAs(SHA2_256, bytes))

        /**
         * Computes the SHA-256 hash of the [ByteArray], and then computes the SHA-256 hash of the hash.
         * @param bytes The [ByteArray] to hash.
         */
        @JvmStatic
        fun sha256Twice(bytes: ByteArray) = sha256(sha256(bytes).bytes)

        /**
         * Computes the SHA-256 hash of the [String]'s UTF-8 byte contents.
         * @param str [String] whose UTF-8 contents will be hashed.
         */
        @JvmStatic
        fun sha256(str: String) = sha256(str.toByteArray())

        /**
         * Generates a random SHA-256 value.
         */
        @DeleteForDJVM
        @JvmStatic
        fun randomSHA256() = sha256(secureRandomBytes(32))

        /**
         * Generates a random hash value.
         */
        @DeleteForDJVM
        @JvmStatic
        fun random(algorithm: String): SecureHash {
            val upperAlgorithm = algorithm.toUpperCase()
            return if (upperAlgorithm == SHA2_256) {
                randomSHA256()
            } else {
                val digest = digestFor(upperAlgorithm)
                HASH(upperAlgorithm, digest.get().digest(secureRandomBytes(digest.digestLength)))
            }
        }

        /**
         * A SHA-256 hash value consisting of 32 0x00 bytes.
         * This field provides more intuitive access from Java.
         */
        @JvmField
        val zeroHash: SHA256 = SHA256(ByteArray(32) { 0.toByte() })

        /**
         * A SHA-256 hash value consisting of 32 0x00 bytes.
         * This function is provided for API stability.
         */
        @Suppress("Unused")
        fun getZeroHash(): SHA256 = zeroHash

        /**
         * A SHA-256 hash value consisting of 32 0xFF bytes.
         * This field provides more intuitive access from Java.
         */
        @JvmField
        val allOnesHash: SHA256 = SHA256(ByteArray(32) { 255.toByte() })

        /**
         * A SHA-256 hash value consisting of 32 0xFF bytes.
         * This function is provided for API stability.
         */
        @Suppress("Unused")
        fun getAllOnesHash(): SHA256 = allOnesHash

        private val hashConstants: ConcurrentMap<String, HashConstants> = ConcurrentHashMap()
        init {
            hashConstants[SHA2_256] = HashConstants(zeroHash, allOnesHash)
        }

        private fun getConstantsFor(algorithm: String): HashConstants {
            return hashConstants.computeIfAbsent(algorithm.toUpperCase()) { algName ->
                val digestLength = digestFor(algName).digestLength
                HashConstants(
                    zero = HASH(algName, ByteArray(digestLength)),
                    allOnes = HASH(algName, ByteArray(digestLength) { 255.toByte() })
                )
            }
        }

        @JvmStatic
        fun zeroHashFor(algorithm: String): SecureHash {
            return getConstantsFor(algorithm).zero
        }

        @JvmStatic
        fun allOnesHashFor(algorithm: String): SecureHash {
            return getConstantsFor(algorithm).allOnes
        }
    }

    // In future, maybe SHA3, truncated hashes etc.
}

val OpaqueBytes.isZero: Boolean get() {
    for (b in bytes) {
        if (b != 0.toByte()) {
            return false
        }
    }
    return true
}

/**
 * Compute the SHA-256 hash for the contents of the [ByteArray].
 */
fun ByteArray.sha256(): SecureHash.SHA256 = SecureHash.sha256(this)

/**
 * Compute the SHA-256 hash for the contents of the [OpaqueBytes].
 */
fun OpaqueBytes.sha256(): SecureHash.SHA256 = SecureHash.sha256(this.bytes)

/**
 * Compute the [algorithm] hash for the contents of the [ByteArray].
 */
fun ByteArray.hashAs(algorithm: String): SecureHash = SecureHash.hashAs(algorithm, this)

/**
 * Compute the [algorithm] hash for the contents of the [OpaqueBytes].
 */
fun OpaqueBytes.hashAs(algorithm: String): SecureHash = SecureHash.hashAs(algorithm, bytes)

/**
 * Hide the [FastThreadLocal] class behind a [Supplier] interface
 * so that we can remove it for core-deterministic.
 */
private class DigestSupplier(algorithm: String) : Supplier<MessageDigest> {
    private val threadLocalMessageDigest = LocalDigest(algorithm)
    override fun get(): MessageDigest = threadLocalMessageDigest.get()
    val digestLength: Int = get().digestLength
}

// Declaring this as "object : FastThreadLocal<>" would have
// created an extra public class in the API definition.
private class LocalDigest(private val algorithm: String) : FastThreadLocal<MessageDigest>() {
    override fun initialValue(): MessageDigest = try {
        MessageDigest.getInstance(algorithm)
    } catch (_: NoSuchAlgorithmException) {
        throw IllegalArgumentException("Unknown hash algorithm $algorithm")
    }
}

private class HashConstants(val zero: SecureHash, val allOnes: SecureHash)