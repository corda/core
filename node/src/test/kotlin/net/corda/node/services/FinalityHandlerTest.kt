package net.corda.node.services

import net.corda.core.concurrent.CordaFuture
import net.corda.core.contracts.TransactionVerificationException
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.FinalityFlow
import net.corda.core.flows.StateMachineRunId
import net.corda.core.toFuture
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.getOrThrow
import net.corda.finance.POUNDS
import net.corda.finance.contracts.asset.Cash
import net.corda.finance.issuedBy
import net.corda.node.services.statemachine.StaffedFlowHospital.*
import net.corda.node.services.statemachine.hospital.external.FinalityDoctor
import net.corda.testing.core.ALICE_NAME
import net.corda.testing.core.BOB_NAME
import net.corda.testing.core.singleIdentity
import net.corda.testing.node.internal.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.After
import org.junit.Test
import rx.Observable

class FinalityHandlerTest {
    private val mockNet = InternalMockNetwork()

    @After
    fun cleanUp() {
        mockNet.stopNodes()
    }

    @Test(timeout=300_000)
	fun `sent to flow hospital on error and attempted retry on node restart`() {
        // Setup a network where only Alice has the finance CorDapp and it sends a cash tx to Bob who doesn't have the
        // CorDapp. Bob's FinalityHandler will error when validating the tx.
        val alice = mockNet.createNode(InternalMockNodeParameters(legalName = ALICE_NAME,
               additionalCordapps = FINANCE_CORDAPPS + CustomCordapp(targetPlatformVersion = 3, classes = setOf(FinalityFlow::class.java))))

        var bob = mockNet.createNode(InternalMockNodeParameters(
                legalName = BOB_NAME,
                // The node disables the FinalityHandler completely if there are no old CorDapps loaded, so we need to add
                // a token old CorDapp to keep the handler running.
                additionalCordapps = setOf(DUMMY_CONTRACTS_CORDAPP.copy(targetPlatformVersion = 3))
        ))

        val stx = alice.issueCashTo(bob)
        val finalityHandlerId = bob.trackFinalityHandlerId().run {
            alice.finaliseWithOldApi(stx)
            getOrThrow()
        }

        bob.assertFlowSentForObservationDueToUntrustedAttachmentsException(finalityHandlerId)
        assertThat(bob.getTransaction(stx.id)).isNull()

        bob = mockNet.restartNode(bob)
        // Since we've not done anything to fix the orignal error, we expect the finality handler to be sent to the hospital
        // again on restart
        bob.assertFlowSentForObservationDueToUntrustedAttachmentsException(finalityHandlerId)
        assertThat(bob.getTransaction(stx.id)).isNull()
    }

    private fun TestStartedNode.issueCashTo(recipient: TestStartedNode): SignedTransaction {
        return TransactionBuilder(mockNet.defaultNotaryIdentity).let {
            Cash().generateIssue(
                    it,
                    1000.POUNDS.issuedBy(info.singleIdentity().ref(0)),
                    recipient.info.singleIdentity(),
                    mockNet.defaultNotaryIdentity
            )
            services.signInitialTransaction(it)
        }
    }

    private fun TestStartedNode.trackFinalityHandlerId(): CordaFuture<StateMachineRunId> {
        return smm
                .track()
                .updates
                .filter { it.logic is FinalityHandler }
                .map { it.logic.runId }
                .toFuture()
    }

    private fun TestStartedNode.finaliseWithOldApi(stx: SignedTransaction): CordaFuture<SignedTransaction> {
        @Suppress("DEPRECATION")
        return services.startFlow(FinalityFlow(stx)).resultFuture.apply {
                mockNet.runNetwork()
        }
    }

    private inline fun <reified R : MedicalRecord> TestStartedNode.medicalRecordsOfType(): Observable<R> {
        return smm
                .flowHospital
                .track()
                .let { it.updates.startWith(it.snapshot) }
                .ofType(R::class.java)
    }

    private fun TestStartedNode.assertFlowSentForObservationDueToUntrustedAttachmentsException(runId: StateMachineRunId) {
        val observation = medicalRecordsOfType<MedicalRecord.Flow>()
                .filter { it.flowId == runId }
                .toBlocking()
                .first()
        assertThat(observation.outcome).isEqualTo(Outcome.OVERNIGHT_OBSERVATION)
        assertThat(observation.by).contains(FinalityDoctor)
        val error = observation.errors.single()
        assertThat(error).isInstanceOf(TransactionVerificationException.UntrustedAttachmentsException::class.java)
    }

    private fun TestStartedNode.getTransaction(id: SecureHash): SignedTransaction? {
        return services.validatedTransactions.getTransaction(id)
    }
}
