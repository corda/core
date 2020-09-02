package net.corda.node.services.statemachine

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.FlowSession
import net.corda.core.flows.InitiatedBy
import net.corda.core.flows.InitiatingFlow
import net.corda.core.flows.StartableByRPC
import net.corda.core.identity.Party
import net.corda.core.messaging.startFlow
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.seconds
import net.corda.core.utilities.unwrap
import net.corda.node.services.statemachine.transitions.TopLevelTransition
import net.corda.testing.core.singleIdentity
import org.junit.Test
import kotlin.test.assertEquals

@Suppress("MaxLineLength") // Byteman rules cannot be easily wrapped
class StateMachineSubFlowErrorHandlingTest : StateMachineErrorHandlingTest() {

    /**
     * This test checks that flow calling an initiating subflow will recover correctly.
     *
     * Throws an exception when performing an [Action.CommitTransaction] event during the subflow's first send to a counterparty.
     *
     * The exception is thrown 3 times.
     *
     * This causes the transition to be discharged from the hospital 3 times (retries 3 times). On the final retry the transition
     * succeeds and the flow finishes.
     *
     * Each time the flow retries, it begins from the previous checkpoint where it suspended before failing.
     *
     * 2 of the thrown exceptions are absorbed by the if statement in [TransitionExecutorImpl.executeTransition] that aborts the transition
     * if an error transition moves into another error transition. The flow still recovers from this state. 5 exceptions were thrown to verify
     * that 3 retries are attempted before recovering.
     */
    @Test(timeout = 300_000)
    fun `initiating subflow - error during transition with CommitTransaction action that occurs during the first send will retry and complete successfully`() {
        startDriver {
            val (charlie, alice, port) = createNodeAndBytemanNode()

            val rules = """
                RULE Create Counter
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF createCounter("counter", $counter)
                DO traceln("Counter created")
                ENDRULE
                
                RULE Set flag when entering subflow
                CLASS ${SendAMessageInAnInitiatingSubflowFlow::class.java.name}
                METHOD flag
                AT ENTRY
                IF !flagged("subflow_flag")
                DO flag("subflow_flag"); traceln("Setting subflow flag to true")
                ENDRULE
                
                RULE Set flag when executing first suspend
                CLASS ${TopLevelTransition::class.java.name}
                METHOD suspendTransition
                AT ENTRY
                IF flagged("subflow_flag") && !flagged("suspend_flag")
                DO flag("suspend_flag"); traceln("Setting suspend flag to true")
                ENDRULE

                RULE Throw exception on executeCommitTransaction action after first suspend + commit
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF flagged("subflow_flag") && flagged("suspend_flag") && flagged("commit_flag") && readCounter("counter") < 3
                DO incrementCounter("counter"); traceln("Throwing exception"); throw new java.lang.RuntimeException("die dammit die")
                ENDRULE
                
                RULE Set flag when executing first commit
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF flagged("subflow_flag") && flagged("suspend_flag") && !flagged("commit_flag")
                DO flag("commit_flag"); traceln("Setting commit flag to true")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            alice.rpc.startFlow(
                StateMachineSubFlowErrorHandlingTest::SendAMessageInAnInitiatingSubflowFlow,
                charlie.nodeInfo.singleIdentity()
            ).returnValue.getOrThrow(
                30.seconds
            )

            alice.rpc.assertNumberOfCheckpointsAllZero()
            alice.rpc.assertHospitalCounts(discharged = 3)
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * This test checks that flow calling an initiating subflow will recover correctly.
     *
     * Throws an exception when performing an [Action.CommitTransaction] event during the subflow's first receive from a counterparty.
     *
     * The exception is thrown 3 times.
     *
     * This causes the transition to be discharged from the hospital 3 times (retries 3 times). On the final retry the transition
     * succeeds and the flow finishes.
     *
     * Each time the flow retries, it begins from the previous checkpoint where it suspended before failing.
     *
     * 2 of the thrown exceptions are absorbed by the if statement in [TransitionExecutorImpl.executeTransition] that aborts the transition
     * if an error transition moves into another error transition. The flow still recovers from this state. 5 exceptions were thrown to verify
     * that 3 retries are attempted before recovering.
     */
    @Test(timeout = 300_000)
    fun `initiating subflow - error during transition with CommitTransaction action that occurs after the first receive will retry and complete successfully`() {
        startDriver {
            val (charlie, alice, port) = createNodeAndBytemanNode()

            val rules = """
                RULE Create Counter
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF createCounter("counter", $counter)
                DO traceln("Counter created")
                ENDRULE
                
                RULE Set flag when entering subflow
                CLASS ${SendAMessageInAnInitiatingSubflowFlow::class.java.name}
                METHOD flag
                AT ENTRY
                IF !flagged("subflow_flag")
                DO flag("subflow_flag"); traceln("Setting subflow flag to true")
                ENDRULE
                
                RULE Set flag when executing first suspend
                CLASS ${FlowSessionImpl::class.java.name}
                METHOD receive
                AT ENTRY
                IF flagged("subflow_flag") && !flagged("suspend_flag")
                DO flag("suspend_flag"); traceln("Setting suspend flag to true")
                ENDRULE

                RULE Throw exception on executeCommitTransaction action after first suspend + commit
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF flagged("subflow_flag") && flagged("suspend_flag") && readCounter("counter") < 3
                DO incrementCounter("counter"); traceln("Throwing exception"); throw new java.lang.RuntimeException("die dammit die")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            alice.rpc.startFlow(
                StateMachineSubFlowErrorHandlingTest::SendAMessageInAnInitiatingSubflowFlow,
                charlie.nodeInfo.singleIdentity()
            ).returnValue.getOrThrow(
                30.seconds
            )

            alice.rpc.assertNumberOfCheckpointsAllZero()
            alice.rpc.assertHospitalCounts(discharged = 3)
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * This test checks that flow calling an inline subflow will recover correctly.
     *
     * Throws an exception when performing an [Action.CommitTransaction] event during the subflow's first send to a counterparty.
     *
     * The exception is thrown 3 times.
     *
     * This causes the transition to be discharged from the hospital 3 times (retries 3 times). On the final retry the transition
     * succeeds and the flow finishes.
     *
     * Each time the flow retries, it begins from the previous checkpoint where it suspended before failing.
     *
     * 2 of the thrown exceptions are absorbed by the if statement in [TransitionExecutorImpl.executeTransition] that aborts the transition
     * if an error transition moves into another error transition. The flow still recovers from this state. 5 exceptions were thrown to verify
     * that 3 retries are attempted before recovering.
     */
    @Test(timeout = 300_000)
    fun `inline subflow - error during transition with CommitTransaction action that occurs during the first send will retry and complete successfully`() {
        startDriver {
            val (charlie, alice, port) = createNodeAndBytemanNode()

            val rules = """
                RULE Create Counter
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF createCounter("counter", $counter)
                DO traceln("Counter created")
                ENDRULE
                
                RULE Set flag when entering subflow
                CLASS ${SendAMessageInAnInlineSubflowFlow::class.java.name}
                METHOD flag
                AT ENTRY
                IF !flagged("subflow_flag")
                DO flag("subflow_flag"); traceln("Setting subflow flag to true")
                ENDRULE
                
                RULE Throw exception on executeCommitTransaction action after first suspend + commit
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF flagged("subflow_flag") && readCounter("counter") < 3
                DO incrementCounter("counter"); traceln("Throwing exception"); throw new java.lang.RuntimeException("die dammit die")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            alice.rpc.startFlow(
                StateMachineSubFlowErrorHandlingTest::SendAMessageInAnInlineSubflowFlow,
                charlie.nodeInfo.singleIdentity()
            ).returnValue.getOrThrow(
                30.seconds
            )

            alice.rpc.assertNumberOfCheckpointsAllZero()
            alice.rpc.assertHospitalCounts(discharged = 3)
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * This test checks that flow calling an inline subflow will recover correctly.
     *
     * Throws an exception when performing an [Action.CommitTransaction] event during the subflow's first receive from a counterparty.
     *
     * The exception is thrown 3 times.
     *
     * This causes the transition to be discharged from the hospital 3 times (retries 3 times). On the final retry the transition
     * succeeds and the flow finishes.
     *
     * Each time the flow retries, it begins from the previous checkpoint where it suspended before failing.
     *
     * 2 of the thrown exceptions are absorbed by the if statement in [TransitionExecutorImpl.executeTransition] that aborts the transition
     * if an error transition moves into another error transition. The flow still recovers from this state. 5 exceptions were thrown to verify
     * that 3 retries are attempted before recovering.
     */
    @Test(timeout = 300_000)
    fun `inline subflow - error during transition with CommitTransaction action that occurs during the first receive will retry and complete successfully`() {
        startDriver {
            val (charlie, alice, port) = createNodeAndBytemanNode()

            val rules = """
                RULE Create Counter
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF createCounter("counter", $counter)
                DO traceln("Counter created")
                ENDRULE
                
                RULE Set flag when entering subflow
                CLASS ${SendAMessageInAnInlineSubflowFlow::class.java.name}
                METHOD flag
                AT ENTRY
                IF !flagged("subflow_flag")
                DO flag("subflow_flag"); traceln("Setting subflow flag to true")
                ENDRULE
                
                RULE Throw exception on executeCommitTransaction action after first suspend + commit
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF flagged("subflow_flag") && flagged("commit_flag") && readCounter("counter") < 3
                DO incrementCounter("counter"); traceln("Throwing exception"); throw new java.lang.RuntimeException("die dammit die")
                ENDRULE
                
                RULE Set flag when executing first commit
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF flagged("subflow_flag") && !flagged("commit_flag")
                DO flag("commit_flag"); traceln("Setting commit flag to true")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            alice.rpc.startFlow(
                StateMachineSubFlowErrorHandlingTest::SendAMessageInAnInlineSubflowFlow,
                charlie.nodeInfo.singleIdentity()
            ).returnValue.getOrThrow(
                30.seconds
            )

            alice.rpc.assertNumberOfCheckpointsAllZero()
            alice.rpc.assertHospitalCounts(discharged = 3)
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
        }
    }

    @StartableByRPC
    @InitiatingFlow
    class SendAMessageInAnInitiatingSubflowFlow(private val party: Party) : FlowLogic<String>() {
        @Suspendable
        override fun call(): String {
            val session = initiateFlow(party)
            session.send("hello there from top level flow")
            session.receive<String>().unwrap { it }
            logger.info("entering subflow")
            flag()
            val result = subFlow(InitiatingSendAMessageFlow(party))
            logger.info("Finished sub flow and receive result - $result")
            session.send("another hello there from top level flow")
            session.receive<String>().unwrap { it }
            logger.info("Finished top level flow")
            return "Finished executing test flow - ${this.runId}"
        }

        private fun flag() {
            logger.info("for byteman")
        }
    }

    @InitiatedBy(SendAMessageInAnInitiatingSubflowFlow::class)
    class SendAMessageInAnInitiatingSubflowResponder(private val session: FlowSession) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            session.receive<String>().unwrap { it }
            session.send("reply 1")
            session.receive<String>().unwrap { it }
            session.send("reply 2")
        }
    }

    @StartableByRPC
    @InitiatingFlow
    class InitiatingSendAMessageFlow(private val party: Party) : FlowLogic<String>() {
        @Suspendable
        override fun call(): String {
            val session = initiateFlow(party)
            session.send("hello there")
            session.receive<String>().unwrap { it }
            return "Finished executing test flow - ${this.runId}"
        }
    }

    @InitiatedBy(InitiatingSendAMessageFlow::class)
    class InitiatingSendAMessageResponder(private val session: FlowSession) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            session.receive<String>().unwrap { it }
            session.send("reply 1")
        }
    }

    @StartableByRPC
    @InitiatingFlow
    class SendAMessageInAnInlineSubflowFlow(private val party: Party) : FlowLogic<String>() {
        @Suspendable
        override fun call(): String {
            val session = initiateFlow(party)
            session.send("hello there from top level flow")
            session.receive<String>().unwrap { it }
            logger.info("entering subflow")
            flag()
            val result = subFlow(InlineSendAMessageSubflow(session))
            logger.info("Finished sub flow and receive result - $result")
            session.send("another hello there from top level flow")
            session.receive<String>().unwrap { it }
            logger.info("Finished top level flow")
            return "Finished executing test flow - ${this.runId}"
        }

        private fun flag() {
            logger.info("for byteman")
        }
    }

    @InitiatedBy(SendAMessageInAnInlineSubflowFlow::class)
    class SendAMessageInAnInlineSubflowResponder(private val session: FlowSession) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            session.receive<String>().unwrap { it }
            session.send("reply 1")
            session.receive<String>().unwrap { it }
            session.send("reply 2")
            session.receive<String>().unwrap { it }
            session.send("reply 3")
        }
    }

    class InlineSendAMessageSubflow(private val session: FlowSession) : FlowLogic<String>() {
        @Suspendable
        override fun call(): String {
            session.send("hello there")
            session.receive<String>().unwrap { it }
            return "Finished executing the inline subflow - ${this.runId}"
        }
    }
}