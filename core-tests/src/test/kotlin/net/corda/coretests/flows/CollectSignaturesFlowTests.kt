package net.corda.coretests.flows

import co.paralleluniverse.fibers.Suspendable
import com.natpryce.hamkrest.assertion.assertThat
import net.corda.core.contracts.Command
import net.corda.core.contracts.StateAndContract
import net.corda.core.contracts.requireThat
import net.corda.core.flows.CollectSignaturesFlow
import net.corda.core.flows.Destination
import net.corda.core.flows.FinalityFlow
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.FlowSession
import net.corda.core.flows.InitiatedBy
import net.corda.core.flows.InitiatingFlow
import net.corda.core.flows.ReceiveFinalityFlow
import net.corda.core.flows.SignTransactionFlow
import net.corda.core.identity.AnonymousParty
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.identity.excludeHostNode
import net.corda.core.identity.groupAbstractPartyByWellKnownParty
import net.corda.core.node.services.IdentityService
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.getOrThrow
import net.corda.testing.contracts.DummyContract
import net.corda.testing.core.ALICE_NAME
import net.corda.testing.core.BOB_NAME
import net.corda.testing.core.CHARLIE_NAME
import net.corda.testing.core.TestIdentity
import net.corda.testing.core.singleIdentity
import net.corda.testing.internal.matchers.flow.willReturn
import net.corda.testing.internal.matchers.flow.willThrow
import net.corda.testing.internal.rigorousMock
import net.corda.testing.node.MockServices
import net.corda.testing.node.internal.DUMMY_CONTRACTS_CORDAPP
import net.corda.testing.node.internal.InternalMockNetwork
import net.corda.testing.node.internal.TestStartedNode
import net.corda.testing.node.internal.enclosedCordapp
import org.hamcrest.CoreMatchers.`is`
import org.junit.AfterClass
import org.junit.Assert
import org.junit.Test
import java.security.PublicKey

class CollectSignaturesFlowTests : WithContracts {
    companion object {
        private val miniCorp = TestIdentity(CordaX500Name("MiniCorp", "London", "GB"))
        private val miniCorpServices = MockServices(listOf("net.corda.testing.contracts"), miniCorp, rigorousMock<IdentityService>())
        private val classMockNet = InternalMockNetwork(cordappsForAllNodes = listOf(DUMMY_CONTRACTS_CORDAPP, enclosedCordapp()))

        private const val MAGIC_NUMBER = 1337

        @JvmStatic
        @AfterClass
        fun tearDown() = classMockNet.stopNodes()
    }

    override val mockNet = classMockNet

    private val aliceNode = makeNode(ALICE_NAME)
    private val bobNode = makeNode(BOB_NAME)
    private val charlieNode = makeNode(CHARLIE_NAME)

    private val alice = aliceNode.info.singleIdentity()
    private val bob = bobNode.info.singleIdentity()
    private val charlie = charlieNode.info.singleIdentity()

    @Test
    fun `successfully collects three signatures`() {
        val bConfidentialIdentity = bobNode.createConfidentialIdentity(bob)
        aliceNode.verifyAndRegister(bConfidentialIdentity)

        assertThat(
                aliceNode.startTestFlow(alice, bConfidentialIdentity.party, charlie),
                willReturn(requiredSignatures(3))
        )
    }

    @Test
    fun `successfully collects signatures when sessions are initiated with AnonymousParty`() {
        val aConfidentialIdentity1 = aliceNode.createConfidentialIdentity(alice)
        val bConfidentialIdentity1 = bobNode.createConfidentialIdentity(bob)
        val bConfidentialIdentity2 = bobNode.createConfidentialIdentity(bob)
        val cConfidentialIdentity1 = charlieNode.createConfidentialIdentity(charlie)

        bobNode.registerInitiatedFlow(AnonymousSessionTestFlowResponder::class.java)
        charlieNode.registerInitiatedFlow(AnonymousSessionTestFlowResponder::class.java)

        val owners = listOf(aConfidentialIdentity1, bConfidentialIdentity1, bConfidentialIdentity2, cConfidentialIdentity1)

        val future = aliceNode.startFlow(AnonymousSessionTestFlow(owners)).resultFuture
        mockNet.runNetwork()
        val stx = future.get()
        val missingSigners = stx.getMissingSigners()
        Assert.assertThat(missingSigners, `is`(emptySet()))
    }

    @Test
    fun `successfully collects signatures when sessions are initiated with both AnonymousParty and WellKnownParty`() {
        val aConfidentialIdentity1 = aliceNode.createConfidentialIdentity(alice)
        val bConfidentialIdentity1 = bobNode.createConfidentialIdentity(bob)
        val bConfidentialIdentity2 = bobNode.createConfidentialIdentity(bob)
        val cConfidentialIdentity1 = charlieNode.createConfidentialIdentity(charlie)
        val cConfidentialIdentity2 = charlieNode.createConfidentialIdentity(charlie)

        bobNode.registerInitiatedFlow(MixAndMatchAnonymousSessionTestFlowResponder::class.java)
        charlieNode.registerInitiatedFlow(MixAndMatchAnonymousSessionTestFlowResponder::class.java)

        val owners = listOf(
                aConfidentialIdentity1,
                bConfidentialIdentity1,
                bConfidentialIdentity2,
                cConfidentialIdentity1,
                cConfidentialIdentity2
        )

        val keysToLookup = listOf(bConfidentialIdentity1.owningKey, bConfidentialIdentity2.owningKey, cConfidentialIdentity1.owningKey)
        val keysToKeepAnonymous = listOf(cConfidentialIdentity2.owningKey)

        val future = aliceNode.startFlow(MixAndMatchAnonymousSessionTestFlow(owners, keysToLookup.toSet(), keysToKeepAnonymous.toSet()))
                .resultFuture
        mockNet.runNetwork()
        val stx = future.get()
        val missingSigners = stx.getMissingSigners()
        Assert.assertThat(missingSigners, `is`(emptySet()))
    }

    @Test(expected = IllegalArgumentException::class)
    fun `throws exception when extra sessions are initiated`() {
        bobNode.registerInitiatedFlow(ExtraSessionsFlowResponder::class.java)
        charlieNode.registerInitiatedFlow(ExtraSessionsFlowResponder::class.java)
        val future = aliceNode.startFlow(ExtraSessionsFlow(
                listOf(
                        bobNode.info.singleIdentity(),
                        charlieNode.info.singleIdentity()
                ),
                listOf(bobNode.info.singleIdentity(), alice)))
                .resultFuture
        mockNet.runNetwork()
        future.getOrThrow()
    }

    @Test
    fun `it is possible to collect from multiple well known sessions`() {
        bobNode.registerInitiatedFlow(ExtraSessionsFlowResponder::class.java)
        charlieNode.registerInitiatedFlow(ExtraSessionsFlowResponder::class.java)
        val future = aliceNode.startFlow(ExtraSessionsFlow(listOf(
                bobNode.info.singleIdentity(),
                bobNode.info.singleIdentity(),
                bobNode.info.singleIdentity(),
                bobNode.info.singleIdentity()),
                listOf(bobNode.info.singleIdentity(), alice))).resultFuture
        mockNet.runNetwork()
        val signedTx = future.getOrThrow()
        Assert.assertThat(signedTx.getMissingSigners(), `is`(emptySet()))
    }

    @Test
    fun `no need to collect any signatures`() {
        val ptx = aliceNode.signDummyContract(alice.ref(1))

        assertThat(
                aliceNode.collectSignatures(ptx),
                willReturn(requiredSignatures(1))
        )
    }

    @Test
    fun `fails for missing notary session for non-notarized transaction with notary as signer`() {
        val notaryNode = mockNet.defaultNotaryNode
        val notary = notaryNode.info.singleIdentity()

        val ptx = aliceNode.signDummyContract(alice.ref(1), 0, notary.ref(1))
        val needsNotarisation = ptx.tx.inputs.isNotEmpty() || ptx.tx.references.isNotEmpty() || ptx.tx.timeWindow != null
        Assert.assertFalse(needsNotarisation)
        Assert.assertEquals(ptx.tx.requiredSigningKeys.size, 2)

        // non-notarized transaction are special because notary cannot sign in finality step
        assertThat(
                aliceNode.collectSignatures(ptx), willThrow(errorMessage(
                "O=Notary Service, L=Zurich, C=CH is a required signer, but no session has been passed in for them")))
    }


    @Test
    fun `fails when not signed by initiator`() {
        val ptx = miniCorpServices.signDummyContract(alice.ref(1))

        assertThat(
                aliceNode.collectSignatures(ptx),
                willThrow(errorMessage("The Initiator of CollectSignaturesFlow must have signed the transaction.")))
    }



    @Test
    fun `passes with multiple initial signatures`() {
        val signedByA = aliceNode.signDummyContract(
                alice.ref(1),
                MAGIC_NUMBER,
                bob.ref(2),
                bob.ref(3))
        val signedByBoth = bobNode.addSignatureTo(signedByA)

        assertThat(
                aliceNode.collectSignatures(signedByBoth),
                willReturn(requiredSignatures(2))
        )
    }

    //region Operators
    private fun TestStartedNode.startTestFlow(vararg party: Party) =
            startFlowAndRunNetwork(
                    TestFlow.Initiator(DummyContract.MultiOwnerState(
                            MAGIC_NUMBER,
                            listOf(*party)),
                            mockNet.defaultNotaryIdentity))

    //region Test Flow
    // With this flow, the initiator starts the "CollectTransactionFlow". It is then the responders responsibility to
    // override "checkTransaction" and add whatever logic their require to verify the SignedTransaction they are
    // receiving off the wire.
    object TestFlow {
        @InitiatingFlow
        class Initiator(private val state: DummyContract.MultiOwnerState, private val notary: Party) : FlowLogic<SignedTransaction>() {
            @Suspendable
            override fun call(): SignedTransaction {
                val myInputKeys = state.participants.map { it.owningKey }
                val command = Command(DummyContract.Commands.Create(), myInputKeys)
                val builder = TransactionBuilder(notary).withItems(StateAndContract(state, DummyContract.PROGRAM_ID), command)
                val ptx = serviceHub.signInitialTransaction(builder)
                val sessions = excludeHostNode(serviceHub, groupAbstractPartyByWellKnownParty(serviceHub, state.owners)).map { initiateFlow(it.key) }
                val stx = subFlow(CollectSignaturesFlow(ptx, sessions, myInputKeys))
                return subFlow(FinalityFlow(stx, sessions))
            }
        }

        @InitiatedBy(TestFlow.Initiator::class)
        class Responder(private val otherSideSession: FlowSession) : FlowLogic<Unit>() {
            @Suspendable
            override fun call() {
                val signFlow = object : SignTransactionFlow(otherSideSession) {
                    @Suspendable
                    override fun checkTransaction(stx: SignedTransaction) = requireThat {
                        val tx = stx.tx
                        val ltx = tx.toLedgerTransaction(serviceHub)
                        "There should only be one output state" using (tx.outputs.size == 1)
                        "There should only be one output state" using (tx.inputs.isEmpty())
                        val magicNumberState = ltx.outputsOfType<DummyContract.MultiOwnerState>().single()
                        "Must be $MAGIC_NUMBER or greater" using (magicNumberState.magicNumber >= MAGIC_NUMBER)
                    }
                }

                val stxId = subFlow(signFlow).id
                subFlow(ReceiveFinalityFlow(otherSideSession, expectedTxId = stxId))
            }
        }
    }
    //region
}

@InitiatingFlow
class AnonymousSessionTestFlow(private val cis: List<PartyAndCertificate>) : FlowLogic<SignedTransaction>() {
    @Suspendable
    override fun call(): SignedTransaction {

        for (ci in cis) {
            if (ci.name != ourIdentity.name) {
                serviceHub.identityService.verifyAndRegisterIdentity(ci)
            }
        }
        val state = DummyContract.MultiOwnerState(owners = cis.map { AnonymousParty(it.owningKey) })
        val create = net.corda.testing.contracts.DummyContract.Commands.Create()
        val txBuilder = TransactionBuilder(notary = serviceHub.networkMapCache.notaryIdentities.first())
                .addOutputState(state)
                .addCommand(create, cis.map { it.owningKey })

        val ourKey = cis.single { it.name == ourIdentity.name }.owningKey
        val signedByUsTx = serviceHub.signInitialTransaction(txBuilder, ourKey)
        val sessionsToCollectFrom = cis.filter { it.name != ourIdentity.name }.map { initiateFlow(AnonymousParty(it.owningKey)) }
        return subFlow(CollectSignaturesFlow(signedByUsTx, sessionsToCollectFrom, myOptionalKeys = listOf(ourKey)))
    }
}

@InitiatedBy(AnonymousSessionTestFlow::class)
class AnonymousSessionTestFlowResponder(private val otherSideSession: FlowSession) : FlowLogic<Unit>() {
    @Suspendable
    override fun call() {
        val signFlow = object : SignTransactionFlow(otherSideSession) {
            @Suspendable
            override fun checkTransaction(stx: SignedTransaction) = requireThat {
            }
        }
        subFlow(signFlow)
    }
}

@InitiatingFlow
class MixAndMatchAnonymousSessionTestFlow(private val cis: List<PartyAndCertificate>,
                                          private val keysToLookUp: Set<PublicKey>,
                                          private val keysToKeepAnonymous: Set<PublicKey>) : FlowLogic<SignedTransaction>() {
    @Suspendable
    override fun call(): SignedTransaction {

        for (ci in cis) {
            if (ci.name != ourIdentity.name) {
                serviceHub.identityService.verifyAndRegisterIdentity(ci)
            }
        }
        val state = DummyContract.MultiOwnerState(owners = cis.map { AnonymousParty(it.owningKey) })
        val create = net.corda.testing.contracts.DummyContract.Commands.Create()
        val txBuilder = TransactionBuilder(notary = serviceHub.networkMapCache.notaryIdentities.first())
                .addOutputState(state)
                .addCommand(create, cis.map { it.owningKey })

        val ourKey = cis.single { it.name == ourIdentity.name }.owningKey
        val signedByUsTx = serviceHub.signInitialTransaction(txBuilder, ourKey)

        val resolvedParties = keysToLookUp.map { serviceHub.identityService.wellKnownPartyFromAnonymous(AnonymousParty(it))!! }.toSet()
        val anonymousParties = keysToKeepAnonymous.map { AnonymousParty(it) }
        val sessionsToCollectFrom = (resolvedParties + anonymousParties).map { initiateFlow(it as Destination) }
        return subFlow(CollectSignaturesFlow(signedByUsTx, sessionsToCollectFrom, myOptionalKeys = listOf(ourKey)))
    }
}

@InitiatedBy(MixAndMatchAnonymousSessionTestFlow::class)
class MixAndMatchAnonymousSessionTestFlowResponder(private val otherSideSession: FlowSession) : FlowLogic<Unit>() {
    @Suspendable
    override fun call() {
        val signFlow = object : SignTransactionFlow(otherSideSession) {
            @Suspendable
            override fun checkTransaction(stx: SignedTransaction) = requireThat {
            }
        }
        subFlow(signFlow)
    }
}

@InitiatingFlow
class ExtraSessionsFlow(private val openFor: List<Party>, private val involve: List<Party>) : FlowLogic<SignedTransaction>() {
    @Suspendable
    override fun call(): SignedTransaction {

        val sessions = openFor.map { initiateFlow(it) }
        val state = DummyContract.MultiOwnerState(owners = involve.map { AnonymousParty(it.owningKey) })
        val create = net.corda.testing.contracts.DummyContract.Commands.Create()
        val txBuilder = TransactionBuilder(notary = serviceHub.networkMapCache.notaryIdentities.first())
                .addOutputState(state)
                .addCommand(create, involve.map { it.owningKey })

        val signedByUsTx = serviceHub.signInitialTransaction(txBuilder)
        return subFlow(CollectSignaturesFlow(signedByUsTx, sessions))
    }
}

@InitiatedBy(ExtraSessionsFlow::class)
class ExtraSessionsFlowResponder(private val otherSideSession: FlowSession) : FlowLogic<Unit>() {
    @Suspendable
    override fun call() {
        val signFlow = object : SignTransactionFlow(otherSideSession) {
            @Suspendable
            override fun checkTransaction(stx: SignedTransaction) = requireThat {
            }
        }
        subFlow(signFlow)
    }
}
