package net.corda.node.services.identity

import net.corda.core.internal.PLATFORM_VERSION
import net.corda.core.internal.div
import net.corda.core.utilities.OpaqueBytes
import net.corda.coretesting.internal.stubs.CertificateStoreStubs
import net.corda.finance.DOLLARS
import net.corda.finance.GBP
import net.corda.finance.POUNDS
import net.corda.finance.USD
import net.corda.finance.flows.CashIssueAndPaymentFlow
import net.corda.finance.flows.CashPaymentFlow
import net.corda.finance.workflows.getCashBalance
import net.corda.node.services.keys.KeyManagementServiceInternal
import net.corda.nodeapi.internal.DEV_CA_KEY_STORE_PASS
import net.corda.nodeapi.internal.crypto.X509Utilities.NODE_IDENTITY_KEY_ALIAS
import net.corda.nodeapi.internal.storeLegalIdentity
import net.corda.testing.common.internal.testNetworkParameters
import net.corda.testing.core.ALICE_NAME
import net.corda.testing.core.BOB_NAME
import net.corda.testing.core.CHARLIE_NAME
import net.corda.testing.node.internal.FINANCE_CORDAPPS
import net.corda.testing.node.internal.InternalMockNetwork
import net.corda.testing.node.internal.TestStartedNode
import net.corda.testing.node.internal.startFlow
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.After
import org.junit.Ignore
import org.junit.Test
import java.nio.file.Path
import java.security.PublicKey
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNull

class CertificateRotationTest {
    private val ref = OpaqueBytes.of(0x01)

    private val TestStartedNode.party get() = info.legalIdentities.first()

    private lateinit var mockNet: InternalMockNetwork

    @After
    fun tearDown() {
        mockNet.stopNodes()
    }

    @Test(timeout = 300_000)
    fun `restart with the same identities`() {
        mockNet = InternalMockNetwork(cordappsForAllNodes = FINANCE_CORDAPPS)
        val alice = mockNet.createPartyNode(ALICE_NAME)
        val bob = mockNet.createPartyNode(BOB_NAME)

        alice.services.startFlow(CashIssueAndPaymentFlow(300.DOLLARS, ref, alice.party, false, mockNet.defaultNotaryIdentity))
        alice.services.startFlow(CashIssueAndPaymentFlow(1000.DOLLARS, ref, bob.party, false, mockNet.defaultNotaryIdentity))
        bob.services.startFlow(CashIssueAndPaymentFlow(300.POUNDS, ref, bob.party, false, mockNet.defaultNotaryIdentity))
        bob.services.startFlow(CashIssueAndPaymentFlow(1000.POUNDS, ref, alice.party, false, mockNet.defaultNotaryIdentity))
        mockNet.runNetwork()

        val alice2 = mockNet.restartNode(alice)
        val bob2 = mockNet.restartNode(bob)

        assertEquals(alice.party, alice2.party)
        assertEquals(bob.party, bob2.party)
        assertEquals(alice2.party, alice2.services.identityService.wellKnownPartyFromX500Name(ALICE_NAME))
        assertEquals(bob2.party, alice2.services.identityService.wellKnownPartyFromX500Name(BOB_NAME))
        assertEquals(alice2.party, bob2.services.identityService.wellKnownPartyFromX500Name(ALICE_NAME))
        assertEquals(bob2.party, bob2.services.identityService.wellKnownPartyFromX500Name(BOB_NAME))

        alice2.services.startFlow(CashPaymentFlow(300.DOLLARS, bob2.party, false))
        bob2.services.startFlow(CashPaymentFlow(300.POUNDS, alice2.party, false))
        mockNet.runNetwork()
        bob2.services.startFlow(CashPaymentFlow(1300.DOLLARS, alice2.party, false))
        alice2.services.startFlow(CashPaymentFlow(1300.POUNDS, bob2.party, false))
        mockNet.runNetwork()

        assertEquals(1300.DOLLARS, alice2.services.getCashBalance(USD))
        assertEquals(0.POUNDS, alice2.services.getCashBalance(GBP))
        assertEquals(0.DOLLARS, bob2.services.getCashBalance(USD))
        assertEquals(1300.POUNDS, bob2.services.getCashBalance(GBP))
    }

    @Test(timeout = 300_000)
    fun `restart with rotated key for one node`() {
        mockNet = InternalMockNetwork(
                cordappsForAllNodes = FINANCE_CORDAPPS,
                initialNetworkParameters = testNetworkParameters(minimumPlatformVersion = PLATFORM_VERSION)
        )
        val alice = mockNet.createPartyNode(ALICE_NAME)
        val bob = mockNet.createPartyNode(BOB_NAME)

        alice.services.startFlow(CashIssueAndPaymentFlow(300.DOLLARS, ref, alice.party, false, mockNet.defaultNotaryIdentity))
        alice.services.startFlow(CashIssueAndPaymentFlow(1000.DOLLARS, ref, bob.party, false, mockNet.defaultNotaryIdentity))
        bob.services.startFlow(CashIssueAndPaymentFlow(300.POUNDS, ref, bob.party, false, mockNet.defaultNotaryIdentity))
        bob.services.startFlow(CashIssueAndPaymentFlow(1000.POUNDS, ref, alice.party, false, mockNet.defaultNotaryIdentity))
        mockNet.runNetwork()

        val alice2 = mockNet.restartNodeWithRotateIdentityKey(alice)
        val bob2 = mockNet.restartNode(bob)

        assertNotEquals(alice.party, alice2.party)
        assertEquals(bob.party, bob2.party)
        assertEquals(alice2.party, alice2.services.identityService.wellKnownPartyFromX500Name(ALICE_NAME))
        assertEquals(bob2.party, alice2.services.identityService.wellKnownPartyFromX500Name(BOB_NAME))
        assertEquals(alice2.party, bob2.services.identityService.wellKnownPartyFromX500Name(ALICE_NAME))
        assertEquals(bob2.party, bob2.services.identityService.wellKnownPartyFromX500Name(BOB_NAME))

        alice2.services.startFlow(CashPaymentFlow(300.DOLLARS, bob2.party, false))
        bob2.services.startFlow(CashPaymentFlow(300.POUNDS, alice2.party, false))
        mockNet.runNetwork()
        bob2.services.startFlow(CashPaymentFlow(1300.DOLLARS, alice2.party, false))
        alice2.services.startFlow(CashPaymentFlow(1300.POUNDS, bob2.party, false))
        mockNet.runNetwork()

        assertEquals(1300.DOLLARS, alice2.services.getCashBalance(USD))
        assertEquals(0.POUNDS, alice2.services.getCashBalance(GBP))
        assertEquals(0.DOLLARS, bob2.services.getCashBalance(USD))
        assertEquals(1300.POUNDS, bob2.services.getCashBalance(GBP))
    }

    @Ignore("MPV check for key rotation has been removed with membership groups")
    @Test(timeout = 300_000)
    fun `fail to restart with rotated key and wrong minimum platform version`() {
        mockNet = InternalMockNetwork(
                cordappsForAllNodes = FINANCE_CORDAPPS,
                initialNetworkParameters = testNetworkParameters(minimumPlatformVersion = 8)
        )
        val alice = mockNet.createPartyNode(ALICE_NAME)
        assertThatThrownBy {
            mockNet.restartNodeWithRotateIdentityKey(alice)
        }.hasMessageContaining("Failed to change node legal identity key")
    }

    @Test(timeout = 300_000)
    fun `backchain resolution with rotated issuer key`() {
        mockNet = InternalMockNetwork(
                cordappsForAllNodes = FINANCE_CORDAPPS,
                initialNetworkParameters = testNetworkParameters(minimumPlatformVersion = PLATFORM_VERSION)
        )
        val alice = mockNet.createPartyNode(ALICE_NAME)
        val bob = mockNet.createPartyNode(BOB_NAME)

        alice.services.startFlow(CashIssueAndPaymentFlow(1000.DOLLARS, ref, alice.party, false, mockNet.defaultNotaryIdentity))
        mockNet.runNetwork()
        alice.services.startFlow(CashPaymentFlow(1000.DOLLARS, bob.party, false))
        mockNet.runNetwork()

        val alice2 = mockNet.restartNodeWithRotateIdentityKey(alice)
        val bob2 = mockNet.restartNode(bob)
        val charlie = mockNet.createPartyNode(CHARLIE_NAME)

        assertNotEquals(alice.party, alice2.party)
        assertEquals(alice2.party, charlie.services.identityService.wellKnownPartyFromX500Name(ALICE_NAME))
        assertEquals(bob2.party, charlie.services.identityService.wellKnownPartyFromX500Name(BOB_NAME))
        assertEquals(charlie.party, charlie.services.identityService.wellKnownPartyFromX500Name(CHARLIE_NAME))

        bob2.services.startFlow(CashPaymentFlow(1000.DOLLARS, charlie.party, false))
        mockNet.runNetwork()

        assertEquals(0.DOLLARS, alice2.services.getCashBalance(USD))
        assertEquals(0.DOLLARS, bob2.services.getCashBalance(USD))
        assertEquals(1000.DOLLARS, charlie.services.getCashBalance(USD))
    }

    @Test(timeout = 300_000)
    fun `backchain resolution with issuer removed from network map`() {
        mockNet = InternalMockNetwork(cordappsForAllNodes = FINANCE_CORDAPPS, autoVisibleNodes = false)
        val alice = mockNet.createPartyNode(ALICE_NAME)
        val bob = mockNet.createPartyNode(BOB_NAME)

        advertiseNodesToNetwork(mockNet.defaultNotaryNode, alice, bob)

        alice.services.startFlow(CashIssueAndPaymentFlow(1000.DOLLARS, ref, alice.party, false, mockNet.defaultNotaryIdentity))
        mockNet.runNetwork()
        alice.services.startFlow(CashPaymentFlow(1000.DOLLARS, bob.party, false))
        mockNet.runNetwork()

        bob.services.networkMapCache.clearCache()

        val bob2 = mockNet.restartNode(bob)
        val charlie = mockNet.createPartyNode(CHARLIE_NAME)

        advertiseNodesToNetwork(mockNet.defaultNotaryNode, bob2, charlie)

        assertNull(bob2.services.identityService.wellKnownPartyFromX500Name(ALICE_NAME))
        assertNull(charlie.services.identityService.wellKnownPartyFromX500Name(ALICE_NAME))

        bob2.services.startFlow(CashPaymentFlow(1000.DOLLARS, charlie.party, false))
        mockNet.runNetwork()
        charlie.services.startFlow(CashPaymentFlow(300.DOLLARS, bob2.party, false))
        mockNet.runNetwork()

        assertEquals(300.DOLLARS, bob2.services.getCashBalance(USD))
        assertEquals(700.DOLLARS, charlie.services.getCashBalance(USD))
    }

    private fun InternalMockNetwork.restartNodeWithRotateIdentityKey(node: TestStartedNode): TestStartedNode {
        val oldIdentity = rotateIdentityKey(baseDirectory(node) / "certificates")
        val restartedNode = restartNode(node)
        (restartedNode.services.keyManagementService as KeyManagementServiceInternal).start(listOf(oldIdentity))
        return restartedNode
    }

    private fun rotateIdentityKey(certificatesDirectory: Path): Pair<PublicKey, String> {
        val oldIdentityAlias = "old-identity"
        val certStore = CertificateStoreStubs.Signing.withCertificatesDirectory(certificatesDirectory).get()
        certStore.update {
            val oldKey = getPrivateKey(NODE_IDENTITY_KEY_ALIAS, DEV_CA_KEY_STORE_PASS)
            setPrivateKey(oldIdentityAlias, oldKey, getCertificateChain(NODE_IDENTITY_KEY_ALIAS), DEV_CA_KEY_STORE_PASS)
        }
        certStore.storeLegalIdentity(NODE_IDENTITY_KEY_ALIAS)
        return certStore[oldIdentityAlias].publicKey to oldIdentityAlias
    }

    private fun advertiseNodesToNetwork(vararg nodes: TestStartedNode) {
        nodes.forEach { node ->
            nodes.forEach { node.services.networkMapCache.addOrUpdateNode(it.info) }
        }
    }
}