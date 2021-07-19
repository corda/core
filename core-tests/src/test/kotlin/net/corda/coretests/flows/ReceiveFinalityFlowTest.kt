package net.corda.coretests.flows

import net.corda.core.contracts.FungibleAsset
import net.corda.core.contracts.TransactionVerificationException
import net.corda.core.flows.StateMachineRunId
import net.corda.core.node.services.queryBy
import net.corda.core.toFuture
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.getOrThrow
import net.corda.finance.GBP
import net.corda.finance.POUNDS
import net.corda.finance.flows.CashIssueAndPaymentFlow
import net.corda.finance.flows.CashPaymentReceiverFlow
import net.corda.finance.workflows.getCashBalance
import net.corda.node.services.statemachine.StaffedFlowHospital.*
import net.corda.node.services.statemachine.StaffedFlowHospital.MedicalRecord.Flow
import net.corda.node.services.statemachine.hospital.external.FinalityDoctor
import net.corda.testing.core.ALICE_NAME
import net.corda.testing.core.BOB_NAME
import net.corda.testing.core.DUMMY_NOTARY_NAME
import net.corda.testing.core.singleIdentity
import net.corda.testing.node.MockNetworkNotarySpec
import net.corda.testing.node.internal.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.After
import org.junit.Test
import rx.Observable

class ReceiveFinalityFlowTest {
    private val mockNet = InternalMockNetwork(notarySpecs = listOf(MockNetworkNotarySpec(DUMMY_NOTARY_NAME, validating = false)))

    @After
    fun cleanUp() {
        mockNet.stopNodes()
    }

    @Test(timeout=300_000)
	fun `sent to flow hospital on error and retry on node restart`() {
        val alice = mockNet.createNode(InternalMockNodeParameters(legalName = ALICE_NAME, additionalCordapps = FINANCE_CORDAPPS))
        // Bob initially does not have the finance contracts CorDapp so that it can throw an exception in ReceiveFinalityFlow when receiving
        // the payment from Alice
        var bob = mockNet.createNode(InternalMockNodeParameters(legalName = BOB_NAME, additionalCordapps = listOf(FINANCE_WORKFLOWS_CORDAPP)))

        val paymentReceiverFuture = bob.smm.track().updates.filter { it.logic is CashPaymentReceiverFlow }.map { it.logic.runId }.toFuture()

        alice.services.startFlow(CashIssueAndPaymentFlow(
                100.POUNDS,
                OpaqueBytes.of(0),
                bob.info.singleIdentity(),
                false,
                mockNet.defaultNotaryIdentity
        ))
        mockNet.runNetwork()

        val paymentReceiverId = paymentReceiverFuture.getOrThrow()
        assertThat(bob.services.vaultService.queryBy<FungibleAsset<*>>().states).isEmpty()
        bob.assertFlowSentForObservationDueToUntrustedAttachmentsException(paymentReceiverId)

        // Restart Bob with the contracts CorDapp so that it can recover from the error
        bob = mockNet.restartNode(bob,
                parameters = InternalMockNodeParameters(additionalCordapps = listOf(FINANCE_CONTRACTS_CORDAPP)),
                nodeFactory = { args -> InternalMockNetwork.MockNode(args, allowAppSchemaUpgradeWithCheckpoints = true) })
        mockNet.runNetwork()
        assertThat(bob.services.getCashBalance(GBP)).isEqualTo(100.POUNDS)
    }

    private inline fun <reified R : MedicalRecord> TestStartedNode.medicalRecordsOfType(): Observable<R> {
        return smm
                .flowHospital
                .track()
                .let { it.updates.startWith(it.snapshot) }
                .ofType(R::class.java)
    }

    private fun TestStartedNode.assertFlowSentForObservationDueToUntrustedAttachmentsException(runId: StateMachineRunId) {
        val observation = medicalRecordsOfType<Flow>()
                .filter { it.flowId == runId }
                .toBlocking()
                .first()
        assertThat(observation.outcome).isEqualTo(Outcome.OVERNIGHT_OBSERVATION)
        assertThat(observation.by).contains(FinalityDoctor)
        val error = observation.errors.single()
        assertThat(error).isInstanceOf(TransactionVerificationException.UntrustedAttachmentsException::class.java)
    }
}
