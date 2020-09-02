package net.corda.node.services.statemachine

import net.corda.core.flows.ReceiveFinalityFlow
import net.corda.core.internal.ResolveTransactionsFlow
import net.corda.core.messaging.startFlow
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.seconds
import net.corda.finance.DOLLARS
import net.corda.finance.flows.CashIssueAndPaymentFlow
import net.corda.node.services.api.ServiceHubInternal
import net.corda.testing.core.DUMMY_NOTARY_NAME
import net.corda.testing.core.singleIdentity
import net.corda.testing.node.NotarySpec
import net.corda.testing.node.internal.FINANCE_CORDAPPS
import org.junit.Test
import java.util.concurrent.TimeoutException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

@Suppress("MaxLineLength") // Byteman rules cannot be easily wrapped
class StateMachineFinalityErrorHandlingTest : StateMachineErrorHandlingTest() {

    /**
     * Throws an exception when recoding a transaction inside of [ReceiveFinalityFlow] on the responding
     * flow's node.
     *
     * The flow is kept in for observation.
     *
     * Only the responding node keeps a checkpoint. The initiating flow has completed successfully as it has complete its
     * send to the responding node and the responding node successfully received it.
     */
    @Test(timeout = 300_000)
    fun `error recording a transaction inside of ReceiveFinalityFlow will keep the flow in for observation`() {
        startDriver(notarySpec = NotarySpec(DUMMY_NOTARY_NAME, validating = false)) {
            val (alice, charlie, port) = createNodeAndBytemanNode(FINANCE_CORDAPPS)

            // could not get rule for FinalityDoctor + observation counter to work
            val rules = """
                RULE Set flag when entering receive finality flow
                CLASS ${ReceiveFinalityFlow::class.java.name}
                METHOD call
                AT ENTRY
                IF !flagged("finality_flag")
                DO flag("finality_flag"); traceln("Setting finality flag")
                ENDRULE
                
                RULE Set flag when leaving resolve transactions flow
                CLASS ${ResolveTransactionsFlow::class.java.name}
                METHOD call
                AT EXIT
                IF !flagged("resolve_tx_flag")
                DO flag("resolve_tx_flag"); traceln("Setting resolve tx flag")
                ENDRULE

                RULE Throw exception when recording transaction
                INTERFACE ${ServiceHubInternal::class.java.name}
                METHOD recordTransactions
                AT ENTRY
                IF flagged("finality_flag") && flagged("resolve_tx_flag")
                DO traceln("Throwing exception"); 
                    throw new java.lang.RuntimeException("die dammit die")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            alice.rpc.startFlow(
                ::CashIssueAndPaymentFlow,
                500.DOLLARS,
                OpaqueBytes.of(0x01),
                charlie.nodeInfo.singleIdentity(),
                false,
                defaultNotaryIdentity
            ).returnValue.getOrThrow(30.seconds)

            alice.rpc.assertNumberOfCheckpointsAllZero()
            charlie.rpc.assertNumberOfCheckpoints(hospitalized = 1)
            charlie.rpc.assertHospitalCounts(observation = 1)
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
            assertEquals(1, charlie.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * Throws an exception when resolving a transaction's dependencies inside of [ReceiveFinalityFlow] on the responding
     * flow's node.
     *
     * The flow is kept in for observation.
     *
     * Only the responding node keeps a checkpoint. The initiating flow has completed successfully as it has complete its
     * send to the responding node and the responding node successfully received it.
     */
    @Test(timeout = 300_000)
    fun `error resolving a transaction's dependencies inside of ReceiveFinalityFlow will keep the flow in for observation`() {
        startDriver(notarySpec = NotarySpec(DUMMY_NOTARY_NAME, validating = false)) {
            val (alice, charlie, port) = createNodeAndBytemanNode(FINANCE_CORDAPPS)

            // could not get rule for FinalityDoctor + observation counter to work
            val rules = """
                RULE Set flag when entering receive finality flow
                CLASS ${ReceiveFinalityFlow::class.java.name}
                METHOD call
                AT ENTRY
                IF !flagged("finality_flag")
                DO flag("finality_flag"); traceln("Setting finality flag")
                ENDRULE
                
                RULE Set flag when entering resolve transactions flow
                CLASS ${ResolveTransactionsFlow::class.java.name}
                METHOD call
                AT ENTRY
                IF !flagged("resolve_tx_flag")
                DO flag("resolve_tx_flag"); traceln("Setting resolve tx flag")
                ENDRULE

                RULE Throw exception when recording transaction
                INTERFACE ${ServiceHubInternal::class.java.name}
                METHOD recordTransactions
                AT ENTRY
                IF flagged("finality_flag") && flagged("resolve_tx_flag")
                DO traceln("Throwing exception"); 
                    throw new java.lang.RuntimeException("die dammit die")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            alice.rpc.startFlow(
                ::CashIssueAndPaymentFlow,
                500.DOLLARS,
                OpaqueBytes.of(0x01),
                charlie.nodeInfo.singleIdentity(),
                false,
                defaultNotaryIdentity
            ).returnValue.getOrThrow(30.seconds)

            alice.rpc.assertNumberOfCheckpointsAllZero()
            charlie.rpc.assertNumberOfCheckpoints(hospitalized = 1)
            charlie.rpc.assertHospitalCounts(observation = 1)
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
            assertEquals(1, charlie.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * Throws an exception when executing [Action.CommitTransaction] as part of receiving a transaction to record inside of [ReceiveFinalityFlow] on the responding
     * flow's node.
     *
     * The exception is thrown 3 times.
     *
     * The responding flow is retried 3 times and then completes successfully.
     *
     * The [StaffedFlowHospital.TransitionErrorGeneralPractitioner] catches these errors instead of the [StaffedFlowHospital.FinalityDoctor]. Due to this, the
     * flow is retried instead of moving straight to observation.
     */
    @Test(timeout = 300_000)
    fun `error during transition with CommitTransaction action while receiving a transaction inside of ReceiveFinalityFlow will be retried and complete successfully`() {
        startDriver(notarySpec = NotarySpec(DUMMY_NOTARY_NAME, validating = false)) {
            val (alice, charlie, port) = createNodeAndBytemanNode(FINANCE_CORDAPPS)

            val rules = """
                RULE Create Counter
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF createCounter("counter", $counter)
                DO traceln("Counter created")
                ENDRULE
                
                RULE Set flag when entering receive finality flow
                CLASS ${ReceiveFinalityFlow::class.java.name}
                METHOD call
                AT ENTRY
                IF !flagged("finality_flag")
                DO flag("finality_flag"); traceln("Setting finality flag")
                ENDRULE
                
                RULE Throw exception on executeCommitTransaction action
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF flagged("finality_flag") && readCounter("counter") < 3
                DO incrementCounter("counter"); traceln("Throwing exception"); throw new java.lang.RuntimeException("die dammit die")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            alice.rpc.startFlow(
                ::CashIssueAndPaymentFlow,
                500.DOLLARS,
                OpaqueBytes.of(0x01),
                charlie.nodeInfo.singleIdentity(),
                false,
                defaultNotaryIdentity
            ).returnValue.getOrThrow(30.seconds)

            // This sleep is a bit suspect...
            Thread.sleep(1000)

            alice.rpc.assertNumberOfCheckpointsAllZero()
            charlie.rpc.assertNumberOfCheckpointsAllZero()
            charlie.rpc.assertHospitalCounts(discharged = 3)
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
            assertEquals(0, charlie.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * Throws an exception when executing [Action.CommitTransaction] as part of receiving a transaction to record inside of [ReceiveFinalityFlow] on the responding
     * flow's node.
     *
     * The exception is thrown 4 times.
     *
     * The responding flow is retried 3 times and is then kept in for observation.
     *
     * Both the initiating node and the responding node keep checkpoints for their flows. The initiating node keeps a checkpoint for the original flow that is
     * waiting for the responding flow's receive to complete. The responding flow's checkpoint is kept due to it failing the commit as part of receive.
     *
     * The [StaffedFlowHospital.TransitionErrorGeneralPractitioner] catches these errors instead of the [StaffedFlowHospital.FinalityDoctor]. Due to this, the
     * flow is retried instead of moving straight to observation.
     */
    @Test(timeout = 300_000)
    fun `error during transition with CommitTransaction action while receiving a transaction inside of ReceiveFinalityFlow will be retried and be kept for observation is error persists`() {
        startDriver(notarySpec = NotarySpec(DUMMY_NOTARY_NAME, validating = false)) {
            val (alice, charlie, port) = createNodeAndBytemanNode(FINANCE_CORDAPPS)

            val rules = """
                RULE Create Counter
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF createCounter("counter", $counter)
                DO traceln("Counter created")
                ENDRULE
                
                RULE Set flag when entering receive finality flow
                CLASS ${ReceiveFinalityFlow::class.java.name}
                METHOD call
                AT ENTRY
                IF !flagged("finality_flag")
                DO flag("finality_flag"); traceln("Setting finality flag")
                ENDRULE
                
                RULE Throw exception on executeCommitTransaction action
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF flagged("finality_flag") && readCounter("counter") < 4
                DO incrementCounter("counter"); traceln("Throwing exception"); throw new java.lang.RuntimeException("die dammit die")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            assertFailsWith<TimeoutException> {
                alice.rpc.startFlow(
                    ::CashIssueAndPaymentFlow,
                    500.DOLLARS,
                    OpaqueBytes.of(0x01),
                    charlie.nodeInfo.singleIdentity(),
                    false,
                    defaultNotaryIdentity
                ).returnValue.getOrThrow(30.seconds)
            }

            alice.rpc.assertNumberOfCheckpoints(runnable = 1)
            charlie.rpc.assertNumberOfCheckpoints(hospitalized = 1)
            charlie.rpc.assertHospitalCounts(
                discharged = 3,
                observation = 1
            )
            assertEquals(1, alice.rpc.stateMachinesSnapshot().size)
            assertEquals(1, charlie.rpc.stateMachinesSnapshot().size)
            alice.stop()
            charlie.stop()
        }
    }
}