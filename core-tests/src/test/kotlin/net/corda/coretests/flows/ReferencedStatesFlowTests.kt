package net.corda.coretests.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.contracts.*
import net.corda.core.flows.*
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.Party
import net.corda.core.node.StatesToRecord
import net.corda.core.node.services.Vault
import net.corda.core.node.services.queryBy
import net.corda.core.node.services.vault.QueryCriteria
import net.corda.core.transactions.LedgerTransaction
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.getOrThrow
import net.corda.node.VersionInfo
import net.corda.testing.common.internal.testNetworkParameters
import net.corda.testing.node.internal.*
import org.junit.After
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals

class ReferencedStatesFlowTests {

    var mockNet: InternalMockNetwork = InternalMockNetwork(
            cordappsForAllNodes = listOf(DUMMY_CONTRACTS_CORDAPP, enclosedCordapp()),
            threadPerNode = true,
            initialNetworkParameters = testNetworkParameters(minimumPlatformVersion = 4)
    )
    lateinit var nodes: List<TestStartedNode>

    @Before
    fun setup() {
        nodes = (0..2).map {
            mockNet.createNode(
                    parameters = InternalMockNodeParameters(version = VersionInfo(4, "Blah", "Blah", "Blah"))
            )
        }
    }

    @After
    fun stop() {
        mockNet.stopNodes()
    }

    @Test(timeout=300_000)
	fun `with referenced states flow blocks until the reference state update is received`() {
        // 1. Create reference state.
        val newRefTx = nodes[0].services.startFlow(CreateRefState()).resultFuture.getOrThrow()
        val newRefState = newRefTx.tx.outRefsOfType<RefState.State>().single()

        // 2. Share it with others.
        nodes[0].services.startFlow(Initiator(newRefState)).resultFuture.getOrThrow()

        // 3. Update the reference state but don't share the update.
        val updatedRefTx = nodes[0].services.startFlow(UpdateRefState(newRefState)).resultFuture.getOrThrow()
        val updatedRefState = updatedRefTx.tx.outRefsOfType<RefState.State>().single()

        // 4. Try to use the old reference state. This will throw a NotaryException.
        val nodeOneIdentity = nodes[1].info.legalIdentities.first()
        val useRefTx = nodes[1].services.startFlow(WithReferencedStatesFlow { UseRefState(nodeOneIdentity, newRefState.state.data.linearId) })
                .resultFuture

        // 5. Share the update reference state.
        nodes[0].services.startFlow(Initiator(updatedRefState)).resultFuture.getOrThrow()

        // 6. Check that we have a valid signed transaction with the updated reference state.
        val result = useRefTx.getOrThrow()
        assertEquals(updatedRefState.ref, result.tx.references.single())
    }

    @Test(timeout=300_000)
	fun `check ref state is persisted when used in tx with relevant states`() {
        // 1. Create a state to be used as a reference state. Don't share it.
        val newRefTx = nodes[0].services.startFlow(CreateRefState()).resultFuture.getOrThrow()
        val newRefState = newRefTx.tx.outRefsOfType<RefState.State>().single()
        // 2. Use the "newRefState" a transaction involving another party (nodes[1]) which creates a new state. They should store the new state and the reference state.
        val newTx = nodes[0].services.startFlow(UseRefState(nodes[1].info.legalIdentities.first(), newRefState.state.data.linearId))
                .resultFuture.getOrThrow()
        // Wait until node 1 stores the new tx.
        nodes[1].services.validatedTransactions.trackTransaction(newTx.id).getOrThrow()
        // Check that nodes[1] has finished recording the transaction (and updating the vault.. hopefully!).
        // nodes[1] should have two states. The newly created output of type "Regular.State" and the reference state created by nodes[0].
        assertEquals(2, nodes[1].services.vaultService.queryBy<LinearState>().states.size)
        // Now let's find the specific reference state on nodes[1].
        val refStateLinearId = newRefState.state.data.linearId
        val query = QueryCriteria.LinearStateQueryCriteria(linearId = listOf(refStateLinearId))
        val theReferencedState = nodes[1].services.vaultService.queryBy<RefState.State>(query)
        // There should be one result - the reference state.
        assertEquals(newRefState, theReferencedState.states.single())
        println(theReferencedState.statesMetadata.single())
        // nodes[0] should also have the same state.
        val nodeZeroQuery = QueryCriteria.LinearStateQueryCriteria(linearId = listOf(refStateLinearId))
        val theReferencedStateOnNodeZero = nodes[0].services.vaultService.queryBy<RefState.State>(nodeZeroQuery)
        assertEquals(newRefState, theReferencedStateOnNodeZero.states.single())
        // nodes[0] sends the tx that created the reference state to nodes[1].
        nodes[0].services.startFlow(Initiator(newRefState)).resultFuture.getOrThrow()
        // Query again.
        val theReferencedStateAgain = nodes[1].services.vaultService.queryBy<RefState.State>(query)
        // There should be one result - the reference state.
        assertEquals(newRefState, theReferencedStateAgain.states.single())
    }

    @Test(timeout=300_000)
	fun `check schema mappings are updated for reference states`() {
        // 1. Create a state to be used as a reference state. Don't share it.
        val newRefTx = nodes[0].services.startFlow(CreateRefState()).resultFuture.getOrThrow()
        val newRefState = newRefTx.tx.outRefsOfType<RefState.State>().single()
        // 2. Use the "newRefState" a transaction involving another party (nodes[1]) which creates a new state. They should store the new state and the reference state.
        val newTx = nodes[0].services.startFlow(UseRefState(nodes[1].info.legalIdentities.first(), newRefState.state.data.linearId))
                .resultFuture.getOrThrow()
        // Wait until node 1 stores the new tx.
        nodes[1].services.validatedTransactions.trackTransaction(newTx.id).getOrThrow()
        // Check that nodes[1] has finished recording the transaction (and updating the vault.. hopefully!).
        val allRefStates = nodes[1].services.vaultService.queryBy<LinearState>()
        // nodes[1] should have two states. The newly created output and the reference state created by nodes[0].
        assertEquals(2, allRefStates.states.size)
    }

    @Test(timeout=300_000)
	fun `check old ref state is consumed when update used in tx with relevant states`() {
        // 1. Create a state to be used as a reference state. Don't share it.
        val newRefTx = nodes[0].services.startFlow(CreateRefState()).resultFuture.getOrThrow()
        val newRefState = newRefTx.tx.outRefsOfType<RefState.State>().single()

        // 2. Use the "newRefState" in a transaction involving another party (nodes[1]) which creates a new state. They should store the new state and the reference state.
        val newTx = nodes[0].services.startFlow(UseRefState(nodes[1].info.legalIdentities.first(), newRefState.state.data.linearId))
                .resultFuture.getOrThrow()
        // Wait until node 1 stores the new tx.
        nodes[1].services.validatedTransactions.trackTransaction(newTx.id).getOrThrow()
        // Check that nodes[1] has finished recording the transaction (and updating the vault.. hopefully!).
        // nodes[1] should have two states. The newly created output of type "Regular.State" and the reference state created by nodes[0].
        assertEquals(2, nodes[1].services.vaultService.queryBy<LinearState>().states.size)
        // Now let's find the specific reference state on nodes[1].
        val refStateLinearId = newRefState.state.data.linearId
        val query = QueryCriteria.LinearStateQueryCriteria(linearId = listOf(refStateLinearId))
        val theReferencedState = nodes[1].services.vaultService.queryBy<RefState.State>(query)
        // There should be one result - the reference state.
        assertEquals(newRefState, theReferencedState.states.single())
        // The reference state should not be consumed.
        assertEquals(Vault.StateStatus.UNCONSUMED, theReferencedState.statesMetadata.single().status)
        // nodes[0] should also have the same state.
        val nodeZeroQuery = QueryCriteria.LinearStateQueryCriteria(linearId = listOf(refStateLinearId))
        val theReferencedStateOnNodeZero = nodes[0].services.vaultService.queryBy<RefState.State>(nodeZeroQuery)
        assertEquals(newRefState, theReferencedStateOnNodeZero.states.single())
        assertEquals(Vault.StateStatus.UNCONSUMED, theReferencedStateOnNodeZero.statesMetadata.single().status)

        // 3. Update the reference state but don't share the update.
        nodes[0].services.startFlow(UpdateRefState(newRefState)).resultFuture.getOrThrow()

        // 4. Use the evolved state as a reference state.
        val updatedTx = nodes[0].services.startFlow(UseRefState(nodes[1].info.legalIdentities.first(), newRefState.state.data.linearId))
                .resultFuture.getOrThrow()
        // Wait until node 1 stores the new tx.
        nodes[1].services.validatedTransactions.trackTransaction(updatedTx.id).getOrThrow()
        // Check that nodes[1] has finished recording the transaction (and updating the vault.. hopefully!).
        // nodes[1] should have four states. The originals, plus the newly created output of type "Regular.State" and the reference state created by nodes[0].
        assertEquals(4, nodes[1].services.vaultService.queryBy<LinearState>(QueryCriteria.VaultQueryCriteria(status = Vault.StateStatus.ALL)).states.size)
        // Now let's find the original reference state on nodes[1].
        val updatedQuery = QueryCriteria.VaultQueryCriteria(stateRefs = listOf(newRefState.ref), status = Vault.StateStatus.ALL)
        val theOriginalReferencedState = nodes[1].services.vaultService.queryBy<RefState.State>(updatedQuery)
        // There should be one result - the original reference state.
        assertEquals(newRefState, theOriginalReferencedState.states.single())
        // The reference state should be consumed.
        assertEquals(Vault.StateStatus.CONSUMED, theOriginalReferencedState.statesMetadata.single().status)
        // nodes[0] should also have the same state.
        val theOriginalReferencedStateOnNodeZero = nodes[0].services.vaultService.queryBy<RefState.State>(updatedQuery)
        assertEquals(newRefState, theOriginalReferencedStateOnNodeZero.states.single())
        assertEquals(Vault.StateStatus.CONSUMED, theOriginalReferencedStateOnNodeZero.statesMetadata.single().status)
    }

    @Test(timeout=300_000)
	fun `check consumed reference state is found if a transaction refers to it`() {
        // 1. Create a state to be used as a reference state. Don't share it.
        val newRefTx = nodes[0].services.startFlow(CreateRefState()).resultFuture.getOrThrow()
        val newRefState = newRefTx.tx.outRefsOfType<RefState.State>().single()

        // 2. Use the "newRefState" in a transaction involving another party (nodes[1]) which creates a new state. They should store the new state and the reference state.
        val newTx = nodes[0].services.startFlow(UseRefState(nodes[1].info.legalIdentities.first(), newRefState.state.data.linearId))
                .resultFuture.getOrThrow()

        // Wait until node 1 stores the new tx.
        nodes[1].services.validatedTransactions.trackTransaction(newTx.id).getOrThrow()
        // Check that nodes[1] has finished recording the transaction (and updating the vault.. hopefully!).
        // nodes[1] should have two states. The newly created output of type "Regular.State" and the reference state created by nodes[0].
        assertEquals(2, nodes[1].services.vaultService.queryBy<LinearState>().states.size)

        // 3. Update the reference state but don't share the update.
        val updatedRefTx = nodes[0].services.startFlow(UpdateRefState(newRefState)).resultFuture.getOrThrow()

        // 4. Now report the transactions that created the two reference states to a third party.
        nodes[0].services.startFlow(ReportTransactionFlow(nodes[2].info.legalIdentities.first(), newRefTx)).resultFuture.getOrThrow()
        nodes[0].services.startFlow(ReportTransactionFlow(nodes[2].info.legalIdentities.first(), updatedRefTx)).resultFuture.getOrThrow()
        // Check that there are two linear states in the vault (note that one is consumed)
        assertEquals(2, nodes[2].services.vaultService.queryBy<LinearState>(QueryCriteria.VaultQueryCriteria(status = Vault.StateStatus.ALL)).states.size)

        // 5. Report the transaction that uses the consumed reference state
        nodes[0].services.startFlow(ReportTransactionFlow(nodes[2].info.legalIdentities.first(), newTx)).resultFuture.getOrThrow()
        // There should be 3 linear states in the vault
        assertEquals(3, nodes[2].services.vaultService.queryBy<LinearState>(QueryCriteria.VaultQueryCriteria(status = Vault.StateStatus.ALL)).states.size)
    }

    // A dummy reference state contract.
    class RefState : Contract {
        companion object {
            val CONTRACT_ID: String = RefState::class.java.name
        }

        override fun verify(tx: LedgerTransaction) = Unit

        data class State(val owner: Party, val version: Int = 0, override val linearId: UniqueIdentifier = UniqueIdentifier()) : LinearState {
            override val participants: List<AbstractParty> get() = listOf(owner)
            fun update() = copy(version = version + 1)
        }

        class Create : CommandData
        class Update : CommandData
    }

    class RegularState : Contract {
        companion object {
            val CONTRACT_ID: String = RegularState::class.java.name
        }

        override fun verify(tx: LedgerTransaction) = Unit

        data class State(val owner: Party, override val linearId: UniqueIdentifier = UniqueIdentifier()) : LinearState {
            override val participants: List<AbstractParty> get() = listOf(owner)
        }

        class Create : CommandData
    }

    // A flow to create a reference state.
    class CreateRefState : FlowLogic<SignedTransaction>() {
        @Suspendable
        override fun call(): SignedTransaction {
            val notary = serviceHub.networkMapCache.notaryIdentities.first()
            val stx = serviceHub.signInitialTransaction(TransactionBuilder(notary = notary).apply {
                addOutputState(RefState.State(ourIdentity), RefState.CONTRACT_ID)
                addCommand(RefState.Create(), listOf(ourIdentity.owningKey))
            })
            return subFlow(FinalityFlow(stx, emptyList()))
        }
    }

    // A flow to update a specific reference state.
    class UpdateRefState(private val stateAndRef: StateAndRef<RefState.State>) : FlowLogic<SignedTransaction>() {
        @Suspendable
        override fun call(): SignedTransaction {
            val notary = serviceHub.networkMapCache.notaryIdentities.first()
            val stx = serviceHub.signInitialTransaction(TransactionBuilder(notary = notary).apply {
                addInputState(stateAndRef)
                addOutputState(stateAndRef.state.data.update(), RefState.CONTRACT_ID)
                addCommand(RefState.Update(), listOf(ourIdentity.owningKey))
            })
            return subFlow(FinalityFlow(stx, emptyList()))
        }
    }

    // A set of flows to share a stateref with all other nodes in the mock network.
    @InitiatingFlow
    class Initiator(private val stateAndRef: StateAndRef<ContractState>) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            val sessions = serviceHub.networkMapCache.allParties.map { initiateFlow(it) }
            val transactionId = stateAndRef.ref.txhash
            val transaction = serviceHub.validatedTransactions.getTransaction(transactionId)
                    ?: throw FlowException("Cannot find $transactionId.")
            sessions.forEach { subFlow(SendTransactionFlow(it, transaction)) }
        }
    }

    @InitiatedBy(Initiator::class)
    class Responder(private val otherSession: FlowSession) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            logger.info("Receiving dependencies.")
            subFlow(ReceiveTransactionFlow(
                    otherSideSession = otherSession,
                    checkSufficientSignatures = true,
                    statesToRecord = StatesToRecord.ALL_VISIBLE
            ))
        }
    }

    // A flow to use a reference state in another transaction.
    @InitiatingFlow
    class UseRefState(private val participant: Party, private val linearId: UniqueIdentifier) : FlowLogic<SignedTransaction>() {
        @Suspendable
        override fun call(): SignedTransaction {
            val notary = serviceHub.networkMapCache.notaryIdentities.first()
            val query = QueryCriteria.LinearStateQueryCriteria(
                    linearId = listOf(linearId),
                    relevancyStatus = Vault.RelevancyStatus.ALL
            )
            val referenceState = serviceHub.vaultService.queryBy<ContractState>(query).states.single()

            val stx = serviceHub.signInitialTransaction(TransactionBuilder(notary = notary).apply {
                addReferenceState(referenceState.referenced())
                addOutputState(RegularState.State(participant), RefState.CONTRACT_ID)
                addCommand(RefState.Create(), listOf(ourIdentity.owningKey))
            })
            return if (participant != ourIdentity) {
                subFlow(FinalityFlow(stx, listOf(initiateFlow(participant))))
            } else {
                subFlow(FinalityFlow(stx, emptyList()))
            }
        }
    }

    @InitiatedBy(UseRefState::class)
    class UseRefStateResponder(val otherSession: FlowSession) : FlowLogic<SignedTransaction>() {
        @Suspendable
        override fun call(): SignedTransaction {
            // This should also store the reference state if one is there.
            return subFlow(ReceiveFinalityFlow(otherSession, statesToRecord = StatesToRecord.ONLY_RELEVANT))
        }
    }

    // A flow to report a transaction to a third party.
    @InitiatingFlow
    @StartableByRPC
    class ReportTransactionFlow(private val reportee: Party,
                                private val signedTx: SignedTransaction) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            val session = initiateFlow(reportee)
            subFlow(SendTransactionFlow(session, signedTx))
            session.receive<Unit>()
        }
    }

    @InitiatedBy(ReportTransactionFlow::class)
    class ReceiveReportedTransactionFlow(private val otherSideSession: FlowSession) : FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            subFlow(ReceiveTransactionFlow(otherSideSession, true, StatesToRecord.ALL_VISIBLE))
            otherSideSession.send(Unit)
        }
    }
}
