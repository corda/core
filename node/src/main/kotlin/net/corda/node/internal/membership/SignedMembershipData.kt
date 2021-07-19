package net.corda.node.internal.membership

import net.corda.core.crypto.SignedData
import net.corda.core.node.services.KeyManagementService
import net.corda.core.serialization.serialize
import java.security.PublicKey

inline fun <reified T: Any> T.sign(keyManagementService: KeyManagementService, key: PublicKey): SignedData<T> {
    val serialized = serialize()
    return SignedData(serialized, keyManagementService.sign(serialized.bytes, key))
}