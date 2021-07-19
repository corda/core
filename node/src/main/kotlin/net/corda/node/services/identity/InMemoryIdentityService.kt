package net.corda.node.services.identity

import net.corda.core.crypto.toStringShort
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.identity.x500Matches
import net.corda.core.internal.CertRole
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.trace
import net.corda.node.services.api.IdentityServiceInternal
import net.corda.nodeapi.internal.crypto.X509Utilities
import net.corda.nodeapi.internal.crypto.x509Certificates
import java.security.InvalidAlgorithmParameterException
import java.security.PublicKey
import java.security.cert.*
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import javax.annotation.concurrent.ThreadSafe
import kotlin.collections.ArrayList
import kotlin.collections.LinkedHashSet

/**
 * Simple identity service which caches parties and provides functionality for efficient lookup.
 *
 * @param identities initial set of identities for the service, typically only used for unit tests.
 */
@ThreadSafe
@Suppress("TooManyFunctions")
class InMemoryIdentityService(
        identities: List<PartyAndCertificate> = emptyList(),
        override val trustRoot: X509Certificate
) : SingletonSerializeAsToken(), IdentityServiceInternal {
    companion object {
        private val log = contextLogger()
    }

    /**
     * Certificate store for certificate authority and intermediary certificates.
     */
    override val caCertStore: CertStore = CertStore.getInstance("Collection", CollectionCertStoreParameters(setOf(trustRoot)))
    override val trustAnchor: TrustAnchor = TrustAnchor(trustRoot, null)
    override val trustAnchors = setOf(trustAnchor)
    private val keyToExternalId = ConcurrentHashMap<String, UUID>()
    private val keyToPartyAndCerts = ConcurrentHashMap<PublicKey, PartyAndCertificate>()
    private val nameToKey = ConcurrentHashMap<CordaX500Name, PublicKey>()
    private val keyToName = ConcurrentHashMap<String, CordaX500Name>()
    private val hashToKey = ConcurrentHashMap<String, PublicKey>()

    init {
        keyToPartyAndCerts.putAll(identities.associateBy { it.owningKey })
        nameToKey.putAll(identities.associateBy { it.name }.mapValues { it.value.owningKey })
        keyToName.putAll(identities.associateBy{ it.owningKey.toStringShort() }.mapValues { it.value.party.name })
    }


    @Throws(CertificateExpiredException::class, CertificateNotYetValidException::class, InvalidAlgorithmParameterException::class)
    override fun verifyAndRegisterIdentity(identity: PartyAndCertificate): PartyAndCertificate? {
        return verifyAndRegisterIdentity(trustAnchors, identity)
    }

    @Throws(CertificateExpiredException::class, CertificateNotYetValidException::class, InvalidAlgorithmParameterException::class)
    override fun verifyAndRegisterNewRandomIdentity(identity: PartyAndCertificate) {
        verifyAndRegisterIdentity(trustAnchors, identity)
    }

    @Throws(CertificateExpiredException::class, CertificateNotYetValidException::class, InvalidAlgorithmParameterException::class)
    private fun verifyAndRegisterIdentity(trustAnchors: Set<TrustAnchor>, identity: PartyAndCertificate): PartyAndCertificate? {
        // Validate the chain first, before we do anything clever with it
        val identityCertChain = identity.certPath.x509Certificates
        try {
            identity.verify(trustAnchor)
        } catch (e: CertPathValidatorException) {
            val roots = trustAnchors.map { it.trustedCert.subjectX500Principal }
            log.warn("Certificate validation failed for ${identity.name} against trusted roots $roots.")
            log.warn("Certificate path :")
            identityCertChain.reversed().forEachIndexed { index, certificate ->
                val space = (0 until index).joinToString("") { "   " }
                log.warn("$space${certificate.subjectX500Principal}")
            }
            throw e
        }
        // Ensure we record the first identity of the same name, first
        val wellKnownCert = identityCertChain.single { CertRole.extract(it)?.isWellKnown ?: false }
        if (wellKnownCert != identity.certificate) {
            val idx = identityCertChain.lastIndexOf(wellKnownCert)
            val firstPath = X509Utilities.buildCertPath(identityCertChain.slice(idx until identityCertChain.size))
            verifyAndRegisterIdentity(trustAnchors, PartyAndCertificate(firstPath))
        }
        return registerIdentity(identity, false)
    }

    override fun registerIdentity(identity: PartyAndCertificate) {
        registerIdentity(identity, false)
    }

    private fun registerIdentity(identity: PartyAndCertificate, isNewRandomIdentity: Boolean): PartyAndCertificate? {
        val identityCertChain = identity.certPath.x509Certificates
        log.trace { "Registering identity $identity isNewRandomIdentity=${isNewRandomIdentity}" }
        keyToPartyAndCerts[identity.owningKey] = identity
        // Always keep the first party we registered, as that's the well known identity
        nameToKey.computeIfAbsent(identity.name) {identity.owningKey}
        keyToName.putIfAbsent(identity.owningKey.toStringShort(), identity.name)
        return keyToPartyAndCerts[identityCertChain[1].publicKey]
    }

    override fun partyFromKey(key: PublicKey): Party? {
        return certificateFromKey(key)?.party ?: keyToName[key.toStringShort()]?.let { wellKnownPartyFromX500Name(it) }
    }

    override fun certificateFromKey(owningKey: PublicKey): PartyAndCertificate? = keyToPartyAndCerts[owningKey]

    // We give the caller a copy of the data set to avoid any locking problems
    override fun getAllIdentities(): Iterable<PartyAndCertificate> = ArrayList(keyToPartyAndCerts.values)

    override fun wellKnownPartyFromX500Name(name: CordaX500Name): Party? {
        val key = nameToKey[name]
        return if (key != null) {
            keyToPartyAndCerts[key]?.party
        } else
            null
    }

    override fun partiesFromName(query: String, exactMatch: Boolean): Set<Party> {
        val results = LinkedHashSet<Party>()
        nameToKey.forEach { (x500name, key) ->
            if (x500Matches(query, exactMatch, x500name)) {
                results += keyToPartyAndCerts[key]?.party ?: throw IllegalArgumentException("Could not find an entry in the database for the public key $key.")
            }
        }
        return results
    }

    override fun registerKey(publicKey: PublicKey, party: Party, externalId: UUID?) {
        val publicKeyHash = publicKey.toStringShort()
        val existingEntry = keyToName[publicKeyHash]
        if (existingEntry == null) {
            registerKeyToParty(publicKey, party)
            hashToKey[publicKeyHash] = publicKey
            if (externalId != null) {
                registerKeyToExternalId(publicKey, externalId)
            }
        }
    }

    override fun externalIdForPublicKey(publicKey: PublicKey): UUID? {
        return keyToExternalId[publicKey.toStringShort()]
    }

    fun registerKeyToExternalId(key: PublicKey, externalId: UUID) {
        keyToExternalId[key.toStringShort()] = externalId
    }

    fun registerKeyToParty(publicKey: PublicKey, party: Party) {
        keyToName[publicKey.toStringShort()] = party.name
    }

    override fun publicKeysForExternalId(externalId: UUID): Iterable<PublicKey> {
        throw NotImplementedError("This method is not implemented in the InMemoryIdentityService at it requires access to CordaPersistence.")
    }
}
