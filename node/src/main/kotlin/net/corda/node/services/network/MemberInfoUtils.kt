package net.corda.node.services.network

import net.corda.common.logging.CordaVersion
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.node.EndpointInfo
import net.corda.core.node.MemberInfo
import net.corda.core.node.MemberStatus
import net.corda.core.node.NodeInfo
import net.corda.core.node.distributed
import net.corda.core.node.services.MembershipGroupCache
import net.corda.core.node.services.NetworkMapCache
import net.corda.core.node.services.PartyInfo
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.node.services.api.ServiceHubInternal
import net.corda.nodeapi.internal.DEV_INTERMEDIATE_CA
import net.corda.nodeapi.internal.DEV_ROOT_CA
import net.corda.nodeapi.internal.createDevNodeCa
import net.corda.nodeapi.internal.crypto.CertificateAndKeyPair
import net.corda.nodeapi.internal.crypto.CertificateType
import net.corda.nodeapi.internal.crypto.X509Utilities
import java.math.BigInteger
import java.net.URL
import java.security.cert.X509Certificate
import java.util.*

val MGM_NAME = CordaX500Name("MGM", "London", "GB")
@Suppress("MagicNumber")
val MGM_KEY_ENTROPY: BigInteger = BigInteger.valueOf(123456789)
val DEFAULT_MEMBER_GROUP_ID = UUID(1, 1).toString()

fun NodeInfo.toMemberInfo(softwareVersion: String = CordaVersion.releaseVersion): MemberInfo {
    val party = legalIdentities.first()
    return MemberInfo(
            party = party,
            groupId = DEFAULT_MEMBER_GROUP_ID,
            keys = listOf(party.owningKey),
            endpoints = addresses.map {
                EndpointInfo("https://${it.host}:${it.port}", party.name.x500Principal, platformVersion)
            },
            status = MemberStatus.ACTIVE,
            softwareVersion = softwareVersion,
            platformVersion = platformVersion,
            properties = emptyMap()
    )
}

fun memberInfo(party: Party, connectionURL: String? = null): MemberInfo {
    val url = connectionURL ?: "https://test:0"
    return MemberInfo(
            party = party,
            groupId = DEFAULT_MEMBER_GROUP_ID,
            keys = listOf(party.owningKey),
            endpoints = listOf(EndpointInfo(url, party.name.x500Principal, CordaVersion.platformVersion)),
            status = MemberStatus.ACTIVE,
            softwareVersion = CordaVersion.releaseVersion,
            platformVersion = CordaVersion.platformVersion,
            properties = emptyMap()
    )
}

val ServiceHubInternal.mgm: Boolean get() = (myMemberInfo.party.name == MGM_NAME)

val MemberInfo.addresses: List<NetworkHostAndPort> get() = endpoints.map { with(URL(it.connectionURL)) { NetworkHostAndPort(host, port) } }

fun MemberInfo.toNodeInfo(): NodeInfo {
    return NodeInfo(addresses, listOf(getTestPartyAndCertificate(party)), platformVersion, 0)
}

fun getTestPartyAndCertificate(party: Party): PartyAndCertificate {
    val trustRoot: X509Certificate = DEV_ROOT_CA.certificate
    val intermediate: CertificateAndKeyPair = DEV_INTERMEDIATE_CA

    val (nodeCaCert, nodeCaKeyPair) = createDevNodeCa(intermediate, party.name)

    val identityCert = X509Utilities.createCertificate(
            CertificateType.LEGAL_IDENTITY,
            nodeCaCert,
            nodeCaKeyPair,
            party.name.x500Principal,
            party.owningKey)

    val certPath = X509Utilities.buildCertPath(identityCert, nodeCaCert, intermediate.certificate, trustRoot)
    return PartyAndCertificate(certPath)
}

fun MemberInfo.toPartyInfo(): PartyInfo {
    if (distributed) {
        return PartyInfo.DistributedNode(party)
    }
    return PartyInfo.SingleNode(party, addresses)
}

fun MembershipGroupCache.GroupChange.toMapChange(): NetworkMapCache.MapChange {
    return when (this) {
        is MembershipGroupCache.GroupChange.Added -> NetworkMapCache.MapChange.Added(memberInfo.toNodeInfo())
        is MembershipGroupCache.GroupChange.Modified -> NetworkMapCache.MapChange.Modified(memberInfo.toNodeInfo(), previousInfo.toNodeInfo())
        is MembershipGroupCache.GroupChange.Removed -> NetworkMapCache.MapChange.Removed(memberInfo.toNodeInfo())
    }
}