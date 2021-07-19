package net.corda.node.services.identity

import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.whenever
import net.corda.core.internal.createDirectories
import net.corda.core.utilities.OpaqueBytes
import net.corda.finance.DOLLARS
import net.corda.finance.USD
import net.corda.finance.flows.CashIssueAndPaymentFlow
import net.corda.finance.flows.CashIssueFlow
import net.corda.finance.flows.CashPaymentFlow
import net.corda.finance.workflows.getCashBalance
import net.corda.node.services.config.NotaryConfig
import net.corda.nodeapi.internal.DevIdentityGenerator
import net.corda.nodeapi.internal.createDevNetworkMapCa
import net.corda.nodeapi.internal.network.NetworkParametersCopier
import net.corda.testing.common.internal.addNotary
import net.corda.testing.common.internal.testNetworkParameters
import net.corda.testing.core.ALICE_NAME
import net.corda.testing.core.BOB_NAME
import net.corda.testing.core.CHARLIE_NAME
import net.corda.testing.core.DUMMY_NOTARY_NAME
import net.corda.testing.node.MockNetworkNotarySpec
import net.corda.testing.node.internal.FINANCE_CORDAPPS
import net.corda.testing.node.internal.InternalMockNetwork
import net.corda.testing.node.internal.InternalMockNodeParameters
import net.corda.testing.node.internal.TestStartedNode
import net.corda.testing.node.internal.startFlow
import org.junit.After
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import kotlin.test.assertEquals

@RunWith(Parameterized::class)
class NotaryCertificateRotationTest(private val validating: Boolean) {
    private val ref = OpaqueBytes.of(0x01)

    private val TestStartedNode.party get() = info.legalIdentities.first()

    private lateinit var mockNet: InternalMockNetwork

    companion object {
        @JvmStatic
        @Parameterized.Parameters(name = "validating = {0}")
        fun data() = listOf(false, true)
    }

    @After
    fun tearDown() {
        mockNet.stopNodes()
    }

    @Ignore("More work is needed to support notary key rotation with membership groups")
    @Test(timeout = 300_000)
    fun `rotate notary identity`() {
        mockNet = InternalMockNetwork(
                cordappsForAllNodes = FINANCE_CORDAPPS,
                notarySpecs = listOf(MockNetworkNotarySpec(DUMMY_NOTARY_NAME, validating))
        )
        val alice = mockNet.createPartyNode(ALICE_NAME)
        val bob = mockNet.createPartyNode(BOB_NAME)

        // Issue states and notarize them with initial notary identity.
        alice.services.startFlow(CashIssueFlow(1000.DOLLARS, ref, mockNet.defaultNotaryIdentity))
        alice.services.startFlow(CashIssueAndPaymentFlow(2000.DOLLARS, ref, alice.party, false, mockNet.defaultNotaryIdentity))
        alice.services.startFlow(CashIssueAndPaymentFlow(4000.DOLLARS, ref, bob.party, false, mockNet.defaultNotaryIdentity))
        mockNet.runNetwork()

        // Rotate notary identity and update network parameters.
        val newNotaryIdentity = DevIdentityGenerator.installKeyStoreWithNodeIdentity(
                mockNet.baseDirectory(mockNet.nextNodeId),
                DUMMY_NOTARY_NAME
        )
        val newNetworkParameters = testNetworkParameters(epoch = 2)
                .addNotary(mockNet.defaultNotaryIdentity, validating)
                .addNotary(newNotaryIdentity, validating)
        val ca = createDevNetworkMapCa()
        NetworkParametersCopier(newNetworkParameters, ca, overwriteFile = true).apply {
            install(mockNet.baseDirectory(alice))
            install(mockNet.baseDirectory(bob))
            install(mockNet.baseDirectory(mockNet.nextNodeId))
            install(mockNet.baseDirectory(mockNet.nextNodeId + 1).apply { createDirectories() })
        }

        // Start notary with new identity and restart nodes.
        val notary2 = mockNet.createNode(InternalMockNodeParameters(
                legalName = DUMMY_NOTARY_NAME,
                configOverrides = { doReturn(NotaryConfig(validating)).whenever(it).notary }
        ))
        val alice2 = mockNet.restartNode(alice)
        val bob2 = mockNet.restartNode(bob)
        val charlie = mockNet.createPartyNode(CHARLIE_NAME)

        // Save previous network parameters for subsequent backchain verification.
        mockNet.nodes.forEach { it.services.networkParametersService.saveParameters(ca.sign(mockNet.networkParameters)) }

        // Verify that notary identity has been changed.
        assertEquals(listOf(newNotaryIdentity), alice2.services.networkMapCache.notaryIdentities)
        assertEquals(listOf(newNotaryIdentity), bob2.services.networkMapCache.notaryIdentities)
        assertEquals(listOf(newNotaryIdentity), charlie.services.networkMapCache.notaryIdentities)

        assertEquals(newNotaryIdentity, alice2.services.identityService.wellKnownPartyFromX500Name(DUMMY_NOTARY_NAME))
        assertEquals(newNotaryIdentity, bob2.services.identityService.wellKnownPartyFromX500Name(DUMMY_NOTARY_NAME))
        assertEquals(newNotaryIdentity, charlie.services.identityService.wellKnownPartyFromX500Name(DUMMY_NOTARY_NAME))

        assertEquals(newNotaryIdentity, alice2.services.identityService.wellKnownPartyFromAnonymous(mockNet.defaultNotaryIdentity))
        assertEquals(newNotaryIdentity, bob2.services.identityService.wellKnownPartyFromAnonymous(mockNet.defaultNotaryIdentity))
        assertEquals(newNotaryIdentity, charlie.services.identityService.wellKnownPartyFromAnonymous(mockNet.defaultNotaryIdentity))

        // Move states notarized with previous notary identity.
        alice2.services.startFlow(CashPaymentFlow(3000.DOLLARS, bob2.party, false))
        mockNet.runNetwork()
        bob2.services.startFlow(CashPaymentFlow(7000.DOLLARS, charlie.party, false))
        mockNet.runNetwork()
        charlie.services.startFlow(CashPaymentFlow(7000.DOLLARS, alice2.party, false))
        mockNet.runNetwork()

        // Combine states notarized with previous and present notary identities.
        bob2.services.startFlow(CashIssueAndPaymentFlow(300.DOLLARS, ref, alice2.party, false, notary2.party))
        mockNet.runNetwork()
        alice2.services.startFlow(CashPaymentFlow(7300.DOLLARS, charlie.party, false))
        mockNet.runNetwork()

        // Verify that the ledger contains expected states.
        assertEquals(0.DOLLARS, alice2.services.getCashBalance(USD))
        assertEquals(0.DOLLARS, bob2.services.getCashBalance(USD))
        assertEquals(7300.DOLLARS, charlie.services.getCashBalance(USD))
    }
}