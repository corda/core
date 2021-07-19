package net.corda.core.node

import net.corda.core.crypto.CompositeKey
import net.corda.core.identity.Party
import net.corda.core.serialization.CordaSerializable
import java.security.PublicKey
import javax.security.auth.x500.X500Principal

@CordaSerializable
data class MemberInfo(
        val party: Party,
        val groupId: String,
        val keys: List<PublicKey>,
        val endpoints: List<EndpointInfo>,
        val status: MemberStatus,
        val softwareVersion: String,
        val platformVersion: Int,
        val properties: Map<String, String>
) {
    init {
        require(endpoints.isNotEmpty()) { "Member must have at least one address" }
        require(platformVersion > 0) { "Platform version must be at least 1" }
        require(softwareVersion.isNotEmpty()) { "Software version must not be blank" }
        require(party.owningKey in keys) { "Identity key must be in the key list" }
    }
}

val MemberInfo.distributed: Boolean get() = party.owningKey is CompositeKey

@CordaSerializable
data class EndpointInfo(
        val connectionURL: String,
        val tlsSubjectName: X500Principal,
        val protocolVersion: Int
) {
    init {
        require(protocolVersion > 0) { "Endpoint protocol version must be at least 1" }
    }
}

@CordaSerializable
enum class MemberStatus {
    PENDING,
    ACTIVE,
    SUSPENDED
}
