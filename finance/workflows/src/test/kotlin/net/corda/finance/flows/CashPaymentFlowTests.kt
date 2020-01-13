package net.corda.finance.flows

import net.corda.core.contracts.InsufficientBalanceException
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.node.services.Vault
import net.corda.core.node.services.trackBy
import net.corda.core.node.services.vault.QueryCriteria
import net.corda.core.transactions.WireTransaction
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.getOrThrow
import net.corda.finance.DOLLARS
import net.corda.finance.`issued by`
import net.corda.finance.contracts.asset.Cash
import net.corda.testing.core.*
import net.corda.testing.node.InMemoryMessagingNetwork.ServicePeerAllocationStrategy.RoundRobin
import net.corda.testing.node.MockNetwork
import net.corda.testing.node.MockNetworkParameters
import net.corda.testing.node.StartedMockNode
import net.corda.testing.node.internal.FINANCE_CORDAPPS
import org.assertj.core.api.Assertions.assertThat
import org.hamcrest.CoreMatchers.containsString
import org.hamcrest.core.Is.`is`
import org.hamcrest.core.StringContains
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class CashPaymentFlowTests {
    private lateinit var mockNet: MockNetwork
    private val initialBalance = 2000.DOLLARS
    private val ref = OpaqueBytes.of(0x01)
    private lateinit var bankOfCordaNode: StartedMockNode
    private lateinit var bankOfCorda: Party
    private lateinit var aliceNode: StartedMockNode
    private lateinit var bobV5Node: StartedMockNode

    @Before
    fun start() {
        mockNet = MockNetwork(MockNetworkParameters(servicePeerAllocationStrategy = RoundRobin(), cordappsForAllNodes = FINANCE_CORDAPPS))
        bankOfCordaNode = mockNet.createPartyNode(BOC_NAME)
        bankOfCorda = bankOfCordaNode.info.identityFromX500Name(BOC_NAME)
        aliceNode = mockNet.createPartyNode(ALICE_NAME)
        bobV5Node = mockNet.createPartyNode(CordaX500Name(commonName = "Bob", organisationUnit = "V5Node", organisation = "Banking", locality = "London", state = "London", country = "GB"))
    }

    @After
    fun cleanUp() {
        mockNet.stopNodes()
    }


    @Test
    fun `issue some V5 cash`(){
        val issueRef = bankOfCorda.ref(123).reference
        //issue $50V4
        val issueHandleV4 = bankOfCordaNode.startFlow(CashIssueFlow(50.DOLLARS, issueRef))
        mockNet.runNetwork()
        val v4IssueResult = issueHandleV4.getOrThrow()
        Assert.assertThat((v4IssueResult.stx.coreTransaction as WireTransaction).txVersion, `is`(4))

        //issue $50V5
        val issueHandleV5 = bankOfCordaNode.startFlow(CashIssueFlowV5(50.DOLLARS, issueRef))
        mockNet.runNetwork()
        val v5IssueResult = issueHandleV5.getOrThrow()
        Assert.assertThat((v5IssueResult.stx.coreTransaction as WireTransaction).txVersion, `is`(5))

        //OK so we now have $50V4 and $50V5
        //we are trying to move 2x$26 (26 to V4 and 26 to V5) this should throw an invalid balance exception

        val paymentFlowHandle = bankOfCordaNode.startFlow(VersionAwareCashPaymentFlow(26.DOLLARS, listOf(aliceNode.info.legalIdentities.first(), bobV5Node.info.legalIdentities.first())))
        mockNet.runNetwork()
        try{
            val paymentResult = paymentFlowHandle.getOrThrow()
            throw IllegalStateException("Should not be possible to get here!")
        }catch (isbe: InsufficientBalanceException){
            Assert.assertThat(isbe.message!!, containsString("missing 2.00 USD"))
        }

        //now lets try and move 52 just to the V5 node, this should succeed as V4 AND V5 states are eligible for selection
        val paymentFlowHandleV5Only = bankOfCordaNode.startFlow(VersionAwareCashPaymentFlow(52.DOLLARS, listOf(bobV5Node.info.legalIdentities.first())))
        mockNet.runNetwork()
        val v5PaymentResult = paymentFlowHandleV5Only.getOrThrow()
        //TX version should be 5
        Assert.assertThat((v5PaymentResult.stx.coreTransaction as WireTransaction).txVersion, `is`(5))

        //OK so now we have consumed the V4 state into a V5 transaction there should be zero v4 states available for selection
        val paymentFlowHandleV4Only = bankOfCordaNode.startFlow(VersionAwareCashPaymentFlow(2.DOLLARS, listOf(aliceNode.info.legalIdentities.first())))
        mockNet.runNetwork()
        try{
            val v4PaymentResult = paymentFlowHandleV4Only.getOrThrow()
            throw IllegalStateException("Should not be possible to get here!")
        }catch (isbe: InsufficientBalanceException){
            Assert.assertThat(isbe.message!!, containsString("missing 2.00 USD"))
        }
    }

    @Test
    fun `pay some cash`() {
        val issueFuture = bankOfCordaNode.startFlow(CashIssueFlow(initialBalance, ref, mockNet.defaultNotaryIdentity))
        issueFuture.getOrThrow()
        val payTo = aliceNode.info.singleIdentity()
        val expectedPayment = 500.DOLLARS
        val expectedChange = 1500.DOLLARS

        // Register for vault updates
        val criteria = QueryCriteria.VaultQueryCriteria(status = Vault.StateStatus.ALL)
        val (_, vaultUpdatesBoc) = bankOfCordaNode.services.vaultService.trackBy<Cash.State>(criteria)
        val (_, vaultUpdatesBankClient) = aliceNode.services.vaultService.trackBy<Cash.State>(criteria)

        val future = bankOfCordaNode.startFlow(CashPaymentFlow(expectedPayment, payTo))
        mockNet.runNetwork()
        future.getOrThrow()

        // Check Bank of Corda vault updates - we take in some issued cash and split it into $500 to the notary
        // and $1,500 back to us, so we expect to consume one state, produce one state for our own vault
        vaultUpdatesBoc.expectEvents {
            expect { (consumed, produced) ->
                assertThat(consumed).hasSize(1)
                assertThat(produced).hasSize(1)
                val changeState = produced.single().state.data
                assertEquals(expectedChange.`issued by`(bankOfCorda.ref(ref)), changeState.amount)
            }
        }

        // Check notary node vault updates
        vaultUpdatesBankClient.expectEvents {
            expect { (consumed, produced) ->
                assertThat(consumed).isEmpty()
                assertThat(produced).hasSize(1)
                val paymentState = produced.single().state.data
                assertEquals(expectedPayment.`issued by`(bankOfCorda.ref(ref)), paymentState.amount)
            }
        }
    }

    @Test
    fun `pay more than we have`() {
        val issueFuture = bankOfCordaNode.startFlow(CashIssueFlow(initialBalance, ref, mockNet.defaultNotaryIdentity))
        issueFuture.getOrThrow()
        val payTo = aliceNode.info.singleIdentity()
        val expected = 4000.DOLLARS
        val future = bankOfCordaNode.startFlow(CashPaymentFlow(expected,
                payTo))
        mockNet.runNetwork()
        assertFailsWith<CashException> {
            future.getOrThrow()
        }
    }

    @Test
    fun `pay zero cash`() {
        val issueFuture = bankOfCordaNode.startFlow(CashIssueFlow(initialBalance, ref, mockNet.defaultNotaryIdentity))
        issueFuture.getOrThrow()
        val payTo = aliceNode.info.singleIdentity()
        val expected = 0.DOLLARS
        val future = bankOfCordaNode.startFlow(CashPaymentFlow(expected,
                payTo))
        mockNet.runNetwork()
        assertFailsWith<IllegalArgumentException> {
            future.getOrThrow()
        }
    }
}
