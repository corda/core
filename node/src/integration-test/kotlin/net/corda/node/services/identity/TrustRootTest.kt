package net.corda.node.services.identity

import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.whenever
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.div
import net.corda.core.utilities.OpaqueBytes
import net.corda.coretesting.internal.stubs.CertificateStoreStubs
import net.corda.finance.DOLLARS
import net.corda.finance.GBP
import net.corda.finance.POUNDS
import net.corda.finance.USD
import net.corda.finance.flows.CashIssueAndPaymentFlow
import net.corda.finance.workflows.getCashBalance
import net.corda.node.services.config.NotaryConfig
import net.corda.nodeapi.internal.createDevNetworkMapCa
import net.corda.nodeapi.internal.createDevNodeCa
import net.corda.nodeapi.internal.crypto.CertificateAndKeyPair
import net.corda.nodeapi.internal.crypto.X509Utilities
import net.corda.nodeapi.internal.installDevNodeCaCertPath
import net.corda.nodeapi.internal.network.NetworkParametersCopier
import net.corda.nodeapi.internal.registerDevP2pCertificates
import net.corda.nodeapi.internal.storeLegalIdentity
import net.corda.testing.common.internal.addNotary
import net.corda.testing.common.internal.testNetworkParameters
import net.corda.testing.core.ALICE_NAME
import net.corda.testing.core.BOB_NAME
import net.corda.testing.core.DUMMY_NOTARY_NAME
import net.corda.testing.internal.createDevIntermediateCaCertPath
import net.corda.testing.node.internal.FINANCE_CORDAPPS
import net.corda.testing.node.internal.InternalMockNetwork
import net.corda.testing.node.internal.InternalMockNodeParameters
import net.corda.testing.node.internal.TestStartedNode
import net.corda.testing.node.internal.startFlow
import org.junit.After
import org.junit.Ignore
import org.junit.Test
import javax.security.auth.x500.X500Principal
import kotlin.test.assertEquals

class TrustRootTest {
    private val ref = OpaqueBytes.of(0x01)

    private val TestStartedNode.party get() = info.legalIdentities.first()

    private lateinit var mockNet: InternalMockNetwork

    @After
    fun tearDown() {
        mockNet.stopNodes()
    }

    @Test(timeout = 300_000)
    fun `can start flow between nodes with different roots`() {
        mockNet = InternalMockNetwork(cordappsForAllNodes = FINANCE_CORDAPPS, notarySpecs = listOf())
        val (rootCa1, intermediateCa1) = createDevIntermediateCaCertPath(X500Principal("CN=Root1"))
        val (rootCa2, intermediateCa2) = createDevIntermediateCaCertPath(X500Principal("CN=Root2"))

        // Initiator and acceptor have different roots, both are present on all truststores.
        createKeyStores(0, DUMMY_NOTARY_NAME, intermediateCa1, rootCa1, rootCa2)
        createKeyStores(1, ALICE_NAME, intermediateCa1, rootCa1, rootCa2)
        createKeyStores(2, BOB_NAME, intermediateCa2, rootCa2, rootCa1)
        createNetworkParameters(rootCa1, 0, 1, 2)

        val notary = mockNet.createNode(InternalMockNodeParameters(
                forcedID = 0,
                legalName = DUMMY_NOTARY_NAME,
                configOverrides = { doReturn(NotaryConfig(false)).whenever(it).notary }
        ))
        val alice = mockNet.createNode(InternalMockNodeParameters(forcedID = 1, legalName = ALICE_NAME))
        val bob = mockNet.createNode(InternalMockNodeParameters(forcedID = 2, legalName = BOB_NAME))

        assertEquals(rootCa1.certificate, notary.services.identityService.trustRoot)
        assertEquals(rootCa1.certificate, alice.services.identityService.trustRoot)
        assertEquals(rootCa2.certificate, bob.services.identityService.trustRoot)

        alice.services.startFlow(CashIssueAndPaymentFlow(1000.DOLLARS, ref, bob.party, false, notary.party))
        mockNet.runNetwork()
        bob.services.startFlow(CashIssueAndPaymentFlow(1000.POUNDS, ref, alice.party, false, notary.party))
        mockNet.runNetwork()

        // Ledger state was changed
        assertEquals(1000.POUNDS, alice.services.getCashBalance(GBP))
        assertEquals(1000.DOLLARS, bob.services.getCashBalance(USD))
    }

    @Ignore("Trust root is not relevant to holding identities")
    @Test(timeout = 300_000)
    fun `fail to start flow when missing acceptor's root on the initiator side`() {
        mockNet = InternalMockNetwork(cordappsForAllNodes = FINANCE_CORDAPPS, notarySpecs = listOf())
        val (rootCa1, intermediateCa1) = createDevIntermediateCaCertPath(X500Principal("CN=Root1"))
        val (rootCa2, intermediateCa2) = createDevIntermediateCaCertPath(X500Principal("CN=Root2"))

        // Acceptor's root is missing on the initiator side.
        createKeyStores(0, DUMMY_NOTARY_NAME, intermediateCa1, rootCa1, rootCa2)
        createKeyStores(1, ALICE_NAME, intermediateCa1, rootCa1)
        createKeyStores(2, BOB_NAME, intermediateCa2, rootCa2, rootCa1)
        createNetworkParameters(rootCa1, 0, 1, 2)

        val notary = mockNet.createNode(InternalMockNodeParameters(
                forcedID = 0,
                legalName = DUMMY_NOTARY_NAME,
                configOverrides = { doReturn(NotaryConfig(false)).whenever(it).notary }
        ))
        val alice = mockNet.createNode(InternalMockNodeParameters(forcedID = 1, legalName = ALICE_NAME))
        val bob = mockNet.createNode(InternalMockNodeParameters(forcedID = 2, legalName = BOB_NAME))

        assertEquals(rootCa1.certificate, notary.services.identityService.trustRoot)
        assertEquals(rootCa1.certificate, alice.services.identityService.trustRoot)
        assertEquals(rootCa2.certificate, bob.services.identityService.trustRoot)

        alice.services.startFlow(CashIssueAndPaymentFlow(1000.DOLLARS, ref, bob.party, false, notary.party))
        mockNet.runNetwork()

        // Ledger state remains unchanged.
        assertEquals(1000.DOLLARS, alice.services.getCashBalance(USD))
        assertEquals(0.DOLLARS, bob.services.getCashBalance(USD))
    }

    @Ignore("Trust root is not relevant to holding identities")
    @Test(timeout = 300_000)
    fun `fail to notarise when missing initiator's root on the notary side`() {
        mockNet = InternalMockNetwork(cordappsForAllNodes = FINANCE_CORDAPPS, notarySpecs = listOf())
        val (rootCa1, intermediateCa1) = createDevIntermediateCaCertPath(X500Principal("CN=Root1"))
        val (rootCa2, intermediateCa2) = createDevIntermediateCaCertPath(X500Principal("CN=Root2"))

        // Initiator's root is missing on the notary side.
        createKeyStores(0, DUMMY_NOTARY_NAME, intermediateCa1, rootCa1)
        createKeyStores(1, ALICE_NAME, intermediateCa2, rootCa2, rootCa1)
        createKeyStores(2, BOB_NAME, intermediateCa1, rootCa1, rootCa2)
        createNetworkParameters(rootCa1, 0, 1, 2)

        val notary = mockNet.createNode(InternalMockNodeParameters(
                forcedID = 0,
                legalName = DUMMY_NOTARY_NAME,
                configOverrides = { doReturn(NotaryConfig(false)).whenever(it).notary }
        ))
        val alice = mockNet.createNode(InternalMockNodeParameters(forcedID = 1, legalName = ALICE_NAME))
        val bob = mockNet.createNode(InternalMockNodeParameters(forcedID = 2, legalName = BOB_NAME))

        assertEquals(rootCa1.certificate, notary.services.identityService.trustRoot)
        assertEquals(rootCa2.certificate, alice.services.identityService.trustRoot)
        assertEquals(rootCa1.certificate, bob.services.identityService.trustRoot)

        alice.services.startFlow(CashIssueAndPaymentFlow(1000.DOLLARS, ref, bob.party, false, notary.party))
        mockNet.runNetwork()

        // Ledger state remains unchanged
        assertEquals(1000.DOLLARS, alice.services.getCashBalance(USD))
        assertEquals(0.DOLLARS, bob.services.getCashBalance(USD))
    }

    @Test(timeout = 300_000)
    fun `can notarise when missing acceptor's root on the notary side`() {
        mockNet = InternalMockNetwork(cordappsForAllNodes = FINANCE_CORDAPPS, notarySpecs = listOf())
        val (rootCa1, intermediateCa1) = createDevIntermediateCaCertPath(X500Principal("CN=Root1"))
        val (rootCa2, intermediateCa2) = createDevIntermediateCaCertPath(X500Principal("CN=Root2"))

        // Acceptor's root is missing on the notary side.
        createKeyStores(0, DUMMY_NOTARY_NAME, intermediateCa1, rootCa1)
        createKeyStores(1, ALICE_NAME, intermediateCa1, rootCa1, rootCa2)
        createKeyStores(2, BOB_NAME, intermediateCa2, rootCa2, rootCa1)
        createNetworkParameters(rootCa1, 0, 1, 2)

        val notary = mockNet.createNode(InternalMockNodeParameters(
                forcedID = 0,
                legalName = DUMMY_NOTARY_NAME,
                configOverrides = { doReturn(NotaryConfig(false)).whenever(it).notary }
        ))
        val alice = mockNet.createNode(InternalMockNodeParameters(forcedID = 1, legalName = ALICE_NAME))
        val bob = mockNet.createNode(InternalMockNodeParameters(forcedID = 2, legalName = BOB_NAME))

        assertEquals(rootCa1.certificate, notary.services.identityService.trustRoot)
        assertEquals(rootCa1.certificate, alice.services.identityService.trustRoot)
        assertEquals(rootCa2.certificate, bob.services.identityService.trustRoot)

        alice.services.startFlow(CashIssueAndPaymentFlow(1000.DOLLARS, ref, bob.party, false, notary.party))
        mockNet.runNetwork()

        // Ledger state was changed
        assertEquals(0.DOLLARS, alice.services.getCashBalance(USD))
        assertEquals(1000.DOLLARS, bob.services.getCashBalance(USD))
    }

    private fun createKeyStores(nodeId: Int,
                                legalName: CordaX500Name,
                                intermediateCa: CertificateAndKeyPair,
                                vararg rootCa: CertificateAndKeyPair) {
        val certDir = mockNet.baseDirectory(nodeId) / "certificates"
        val keyStore = CertificateStoreStubs.Signing.withCertificatesDirectory(certDir)
        val sslConfig = CertificateStoreStubs.P2P.withCertificatesDirectory(certDir)

        val nodeCa = createDevNodeCa(intermediateCa, legalName)
        keyStore.get(true).apply {
            installDevNodeCaCertPath(legalName, rootCa.first().certificate, intermediateCa, nodeCa)
            storeLegalIdentity(X509Utilities.NODE_IDENTITY_KEY_ALIAS)
        }
        sslConfig.keyStore.get(true).apply {
            registerDevP2pCertificates(legalName, rootCa.first().certificate, intermediateCa, nodeCa)
        }
        sslConfig.trustStore.get(true).apply {
            rootCa.forEachIndexed { i, root ->
                this["${X509Utilities.CORDA_ROOT_CA}-$i"] = root.certificate
            }
        }
    }

    private fun createNetworkParameters(rootCa: CertificateAndKeyPair, vararg nodeIds: Int) {
        val notaryCertDir = mockNet.baseDirectory(nodeIds.first()) / "certificates"
        val notaryKeyStore = CertificateStoreStubs.Signing.withCertificatesDirectory(notaryCertDir)
        val notary = Party(notaryKeyStore.get()[X509Utilities.NODE_IDENTITY_KEY_ALIAS])
        val networkParameters = testNetworkParameters(epoch = 2).addNotary(notary, false)
        NetworkParametersCopier(networkParameters, createDevNetworkMapCa(rootCa), overwriteFile = true).apply {
            nodeIds.forEach { install(mockNet.baseDirectory(it)) }
        }
    }
}