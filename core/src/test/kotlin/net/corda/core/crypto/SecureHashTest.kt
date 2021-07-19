package net.corda.core.crypto

import net.corda.core.crypto.SecureHash.Companion.SHA2_256
import org.assertj.core.api.Assertions.assertThat
import org.junit.Ignore
import org.junit.Test
import org.junit.jupiter.api.assertThrows
import java.lang.IllegalArgumentException
import kotlin.test.assertEquals

class SecureHashTest {
    @Test(timeout=300_000)
	fun `sha256 does not retain state between same-thread invocations`() {
        assertEquals(SecureHash.sha256("abc"), SecureHash.sha256("abc"))
    }

    @Test(timeout=300_000)
    fun `new sha256 does not retain state between same-thread invocations`() {
        assertEquals(SecureHash.hashAs("sha-256", "abc".toByteArray()), SecureHash.hashAs("sha-256", "abc".toByteArray()))
    }

    @Test(timeout = 300_000)
    fun `test new sha256 secure hash`() {
        val hash = SecureHash.hashAs("sha-256", byteArrayOf(0x64, -0x13, 0x42, 0x3a))
        assertEquals(SecureHash.create("SHA-256:6D1687C143DF792A011A1E80670A4E4E0C25D0D87A39514409B1ABFC2043581F"), hash)
        assertEquals("6D1687C143DF792A011A1E80670A4E4E0C25D0D87A39514409B1ABFC2043581F", hash.toString())
    }

    @Ignore("Requires Java 11+")
    @Test(timeout = 300_000)
    fun `test new sha3-256 secure hash`() {
        val hash = SecureHash.hashAs("sha3-256", byteArrayOf(0x64, -0x13, 0x42, 0x3a))
        assertEquals(SecureHash.create("SHA3-256:A243D53F7273F4C92ED901A14F11B372FDF6FF69583149AFD4AFA24BF17A8880"), hash)
        assertEquals("SHA3-256:A243D53F7273F4C92ED901A14F11B372FDF6FF69583149AFD4AFA24BF17A8880", hash.toString())
    }

    @Test(timeout = 300_000)
    fun `test sha2-256 equivalence`() {
        val data = byteArrayOf(0x64, -0x13, 0x42, 0x3a)
        val oldHash = SecureHash.sha256(data)
        val newHash = SecureHash.hashAs("sha-256", data)
        assertEquals(oldHash.hashCode(), newHash.hashCode())
        assertEquals(oldHash, newHash)
    }

    @Test(timeout = 300_000)
    fun `test unsafe sha-1 secure hash is banned`() {
        val ex = assertThrows<IllegalArgumentException> {
            SecureHash.hashAs("sha-1", byteArrayOf(0x64, -0x13, 0x42, 0x3a))
        }
        assertThat(ex).hasMessage("SHA-1 is forbidden!")
    }

    @Test(timeout = 300_000)
    fun `test double hashing is equivalent`() {
        val data = byteArrayOf(0x64, -0x13, 0x42, 0x3a)
        assertEquals(SecureHash.sha256Twice(data), SecureHash.hashTwiceAs(SHA2_256, data))
    }

    @Test(timeout = 300_000)
    fun `test hash concatenation is equivalent`() {
        val data = byteArrayOf(0x45, 0x33, -0x63, 0x2a, 0x76, -0x64, 0x01, 0x5f)
        val oldHash = data.sha256()
        val newHash = data.hashAs(SHA2_256)
        val expectedHash = oldHash.hashConcat(oldHash)
        assertEquals(expectedHash, newHash.concatenate(newHash))
        assertEquals(expectedHash, newHash.concatenate(oldHash))
        assertEquals(expectedHash, oldHash.concatenate(newHash))
    }
}
