package net.corda.deterministic.verifier

import net.corda.core.contracts.ContractClassName
import net.corda.core.crypto.SecureHash
import net.corda.core.identity.Party
import net.corda.core.internal.AbstractAttachment
import net.corda.core.serialization.CordaSerializable
import java.security.PublicKey

// A valid JAR file with 1 entry.
@Suppress("MagicNumber", "MaxLineLength")
val simpleZip = byteArrayOf(80, 75, 3, 4, 20, 0, 8, 8, 8, 0, -5, 88, 45, 79, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 0, 4, 0, 77,
        69, 84, 65, 45, 73, 78, 70, 47, 77, 65, 78, 73, 70, 69, 83, 84, 46, 77, 70, -2, -54, 0, 0, -29, -27, 2, 0, 80, 75, 7, 8, -84, -123,
        -94, 20, 4, 0, 0, 0, 2, 0, 0, 0, 80, 75, 3, 4, 20, 0, 8, 8, 8, 0, -5, 88, 45, 79, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 70,
        111, 111, 67, 111, 110, 116, 114, 97, 99, 116, -13, 84, -49, 85, 40, -55, 72, 85, 72, 74, 45, 46, 81, 72, -50, -49, 43, 41, 74, 76,
        46, 81, 4, 0, 80, 75, 7, 8, 123, 24, 59, 87, 24, 0, 0, 0, 22, 0, 0, 0, 80, 75, 1, 2, 20, 0, 20, 0, 8, 8, 8, 0, -5, 88, 45, 79, -84, -123,
        -94, 20, 4, 0, 0, 0, 2, 0, 0, 0, 20, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 77, 69, 84, 65, 45, 73, 78, 70, 47, 77, 65, 78,
        73, 70, 69, 83, 84, 46, 77, 70, -2, -54, 0, 0, 80, 75, 1, 2, 20, 0, 20, 0, 8, 8, 8, 0, -5, 88, 45, 79, 123, 24, 59, 87, 24, 0, 0, 0, 22,
        0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 74, 0, 0, 0, 70, 111, 111, 67, 111, 110, 116, 114, 97, 99, 116, 80, 75, 5, 6, 0, 0, 0,
        0, 2, 0, 2, 0, 127, 0, 0, 0, -101, 0, 0, 0, 0, 0)

@CordaSerializable
class MockContractAttachment(
        override val id: SecureHash = SecureHash.zeroHash,
        val contract: ContractClassName,
        override val signerKeys: List<PublicKey> = emptyList(),
        override val signers: List<Party> = emptyList()
) : AbstractAttachment({ simpleZip }, "app")