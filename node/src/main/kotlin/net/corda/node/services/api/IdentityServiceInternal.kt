package net.corda.node.services.api

import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.node.services.IdentityService
import java.security.InvalidAlgorithmParameterException
import java.security.cert.CertificateExpiredException
import java.security.cert.CertificateNotYetValidException
import java.security.cert.TrustAnchor

interface IdentityServiceInternal : IdentityService {
    val trustAnchors: Set<TrustAnchor>

    @Throws(CertificateExpiredException::class, CertificateNotYetValidException::class, InvalidAlgorithmParameterException::class)
    fun verifyAndRegisterNewRandomIdentity(identity: PartyAndCertificate)

    fun registerIdentity(identity: PartyAndCertificate)

    fun invalidateCaches(name: CordaX500Name) {}
}