package net.corda.core.crypto

import java.security.MessageDigest
import java.util.function.Supplier

@Suppress("unused")
private class DigestSupplier(private val algorithm: String) : Supplier<MessageDigest> {
    override fun get(): MessageDigest = MessageDigest.getInstance(algorithm)
    val digestLength: Int = get().digestLength
}
