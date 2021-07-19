package net.corda.node.services.identity

import net.corda.core.crypto.Crypto
import net.corda.core.crypto.generateKeyPair
import net.corda.core.identity.AnonymousParty
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.node.services.UnknownAnonymousPartyException
import net.corda.nodeapi.internal.crypto.CertificateType
import net.corda.nodeapi.internal.crypto.X509Utilities
import net.corda.nodeapi.internal.crypto.x509Certificates
import net.corda.testing.core.*
import net.corda.coretesting.internal.DEV_INTERMEDIATE_CA
import net.corda.coretesting.internal.DEV_ROOT_CA
import org.junit.Rule
import org.junit.Test
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

/**
 * Tests for the in memory identity service.
 */
class InMemoryIdentityServiceTests {
    private companion object {
        val alice = TestIdentity(ALICE_NAME, 70)
        val bob = TestIdentity(BOB_NAME, 80)
        val ALICE get() = alice.party
        val ALICE_IDENTITY get() = alice.identity
        val ALICE_PUBKEY get() = alice.publicKey
        val BOB get() = bob.party
        val BOB_IDENTITY get() = bob.identity
        val BOB_PUBKEY get() = bob.publicKey
        fun createService(vararg identities: PartyAndCertificate) = InMemoryIdentityService(identities.toList(), DEV_ROOT_CA.certificate)
    }

    @Rule
    @JvmField
    val testSerialization = SerializationEnvironmentRule()

    @Test(timeout=300_000)
	fun `get all identities`() {
        val service = createService()
        // Nothing registered, so empty set
        assertNull(service.getAllIdentities().firstOrNull())

        service.verifyAndRegisterIdentity(ALICE_IDENTITY)
        var expected = setOf(ALICE)
        var actual = service.getAllIdentities().map { it.party }.toHashSet()
        assertEquals(expected, actual)

        // Add a second party and check we get both back
        service.verifyAndRegisterIdentity(BOB_IDENTITY)
        expected = setOf(ALICE, BOB)
        actual = service.getAllIdentities().map { it.party }.toHashSet()
        assertEquals(expected, actual)
    }

    @Test(timeout=300_000)
	fun `get identity by key`() {
        val service = createService()
        assertNull(service.partyFromKey(ALICE_PUBKEY))
        service.verifyAndRegisterIdentity(ALICE_IDENTITY)
        assertEquals(ALICE, service.partyFromKey(ALICE_PUBKEY))
        assertNull(service.partyFromKey(BOB_PUBKEY))
    }

    @Test(timeout=300_000)
	fun `get identity by name with no registered identities`() {
        val service = createService()
        assertNull(service.wellKnownPartyFromX500Name(ALICE.name))
    }

    @Test(timeout=300_000)
	fun `get identity by substring match`() {
        val service = createService()
        service.verifyAndRegisterIdentity(ALICE_IDENTITY)
        service.verifyAndRegisterIdentity(BOB_IDENTITY)
        val alicente = getTestPartyAndCertificate(CordaX500Name(organisation = "Alicente Worldwide", locality = "London", country = "GB"), generateKeyPair().public)
        service.verifyAndRegisterIdentity(alicente)
        assertEquals(setOf(ALICE, alicente.party), service.partiesFromName("Alice", false))
        assertEquals(setOf(ALICE), service.partiesFromName("Alice Corp", true))
        assertEquals(setOf(BOB), service.partiesFromName("Bob Plc", true))
    }

    @Test(timeout=300_000)
	fun `get identity by name`() {
        val service = createService()
        val identities = listOf("Org A", "Org B", "Org C")
                .map { getTestPartyAndCertificate(CordaX500Name(organisation = it, locality = "London", country = "GB"), generateKeyPair().public) }
        assertNull(service.wellKnownPartyFromX500Name(identities.first().name))
        identities.forEach { service.verifyAndRegisterIdentity(it) }
        identities.forEach { assertEquals(it.party, service.wellKnownPartyFromX500Name(it.name)) }
    }

    @Test(timeout = 300_000)
    fun `get identity by external id with registration straight away`() {
        val service = createService()
        val newPair = Crypto.generateKeyPair()
        val extId = UUID.randomUUID()
        assertNull(service.externalIdForPublicKey(newPair.public))
        service.verifyAndRegisterIdentity(ALICE_IDENTITY)

        service.registerKey(newPair.public, ALICE, extId)
        assertEquals(extId, service.externalIdForPublicKey(newPair.public))

        service.registerKey(newPair.public, ALICE, null)
        assertEquals(extId, service.externalIdForPublicKey(newPair.public))
    }

    @Test(timeout = 300_000)
    fun `get identity by external id with registration on second call`() {
        val service = createService()
        val newPair = Crypto.generateKeyPair()
        val extId = UUID.randomUUID()
        assertNull(service.externalIdForPublicKey(newPair.public))
        service.verifyAndRegisterIdentity(ALICE_IDENTITY)
        assertNull(service.externalIdForPublicKey(newPair.public))

        service.registerKey(newPair.public, ALICE, null)
        assertNull(service.externalIdForPublicKey(newPair.public))

        service.registerKey(newPair.public, ALICE, extId)
        assertEquals(extId, service.externalIdForPublicKey(newPair.public))

        service.registerKey(newPair.public, ALICE, null)
        assertEquals(extId, service.externalIdForPublicKey(newPair.public))
    }

    /**
     * Generate a certificate path from a root CA, down to a transaction key, store and verify the association.
     */
    @Test(timeout=300_000)
	fun `assert unknown anonymous key is unrecognised`() {
        val rootKey = Crypto.generateKeyPair(X509Utilities.DEFAULT_TLS_SIGNATURE_SCHEME)
        val rootCert = X509Utilities.createSelfSignedCACertificate(ALICE.name.x500Principal, rootKey)
        val txKey = Crypto.generateKeyPair(X509Utilities.DEFAULT_TLS_SIGNATURE_SCHEME)
        val service = createService()
        // TODO: Generate certificate with an EdDSA key rather than ECDSA
        val identity = Party(rootCert)
        val txIdentity = AnonymousParty(txKey.public)

        assertFailsWith<UnknownAnonymousPartyException> {
            service.assertOwnership(identity, txIdentity)
        }
    }

    /**
     * Generate a pair of certificate paths from a root CA, down to a transaction key, store and verify the associations.
     * Also checks that incorrect associations are rejected.
     */
    @Test(timeout=300_000)
	fun `get anonymous identity by key`() {
        val (alice, aliceTxIdentity) = createConfidentialIdentity(ALICE.name)
        val (_, bobTxIdentity) = createConfidentialIdentity(ALICE.name)

        // Now we have identities, construct the service and let it know about both
        val service = createService(alice)
        service.verifyAndRegisterIdentity(aliceTxIdentity)

        var actual = @Suppress("DEPRECATION") service.certificateFromKey(aliceTxIdentity.party.owningKey)
        assertEquals(aliceTxIdentity, actual!!)

        assertNull(@Suppress("DEPRECATION") service.certificateFromKey(bobTxIdentity.party.owningKey))
        service.verifyAndRegisterIdentity(bobTxIdentity)
        actual = @Suppress("DEPRECATION") service.certificateFromKey(bobTxIdentity.party.owningKey)
        assertEquals(bobTxIdentity, actual!!)
    }

    /**
     * Generate a pair of certificate paths from a root CA, down to a transaction key, store and verify the associations.
     * Also checks that incorrect associations are rejected.
     */
    @Test(timeout=300_000)
	fun `assert ownership`() {
        val (alice, anonymousAlice) = createConfidentialIdentity(ALICE.name)
        val (bob, anonymousBob) = createConfidentialIdentity(BOB.name)

        // Now we have identities, construct the service and let it know about both
        val service = createService(alice, bob)
        service.verifyAndRegisterIdentity(anonymousAlice)
        service.verifyAndRegisterIdentity(anonymousBob)

        // Verify that paths are verified
        service.assertOwnership(alice.party, anonymousAlice.party.anonymise())
        service.assertOwnership(bob.party, anonymousBob.party.anonymise())
        assertFailsWith<IllegalArgumentException> {
            service.assertOwnership(alice.party, anonymousBob.party.anonymise())
        }
        assertFailsWith<IllegalArgumentException> {
            service.assertOwnership(bob.party, anonymousAlice.party.anonymise())
        }

        assertFailsWith<IllegalArgumentException> {
            val owningKey = DEV_INTERMEDIATE_CA.certificate.publicKey
            val subject = CordaX500Name.build(DEV_INTERMEDIATE_CA.certificate.subjectX500Principal)
            service.assertOwnership(Party(subject, owningKey), anonymousAlice.party.anonymise())
        }
    }

    private fun createConfidentialIdentity(x500Name: CordaX500Name): Pair<PartyAndCertificate, PartyAndCertificate> {
        val issuerKeyPair = generateKeyPair()
        val issuer = getTestPartyAndCertificate(x500Name, issuerKeyPair.public)
        val txKeyPair = Crypto.generateKeyPair()
        val txCert = X509Utilities.createCertificate(
                CertificateType.CONFIDENTIAL_LEGAL_IDENTITY,
                issuer.certificate,
                issuerKeyPair,
                x500Name.x500Principal,
                txKeyPair.public)
        val txCertPath = X509Utilities.buildCertPath(txCert, issuer.certPath.x509Certificates)
        return Pair(issuer, PartyAndCertificate(txCertPath))
    }

    /**
     * Ensure if we feed in a full identity, we get the same identity back.
     */
    @Test(timeout=300_000)
	fun `deanonymising a well known identity should return the identity`() {
        val service = createService()
        val expected = ALICE
        service.verifyAndRegisterIdentity(ALICE_IDENTITY)
        val actual = service.wellKnownPartyFromAnonymous(expected)
        assertEquals(expected, actual)
    }

    /**
     * Ensure we don't blindly trust what an anonymous identity claims to be.
     */
    @Test(timeout=300_000)
	fun `deanonymising a false well known identity should return null`() {
        val service = createService()
        val notAlice = Party(ALICE.name, generateKeyPair().public)
        service.verifyAndRegisterIdentity(ALICE_IDENTITY)
        val actual = service.wellKnownPartyFromAnonymous(notAlice)
        assertNull(actual)
    }
}
