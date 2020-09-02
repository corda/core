package net.corda.node.services.statemachine

import net.corda.core.CordaRuntimeException
import net.corda.core.messaging.startFlow
import net.corda.core.messaging.startFlowWithClientId
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.seconds
import net.corda.node.services.api.CheckpointStorage
import net.corda.nodeapi.internal.persistence.DatabaseTransaction
import net.corda.testing.core.singleIdentity
import org.junit.Test
import java.sql.Connection
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeoutException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

@Suppress("MaxLineLength") // Byteman rules cannot be easily wrapped
class StateMachineFlowInitErrorHandlingTest : StateMachineErrorHandlingTest() {

    private companion object {
        val executor: ExecutorService = Executors.newSingleThreadExecutor()
    }

    /**
     * Throws an exception when performing an [Action.CommitTransaction] event before the flow has initialised and saved its first checkpoint
     * (remains in an unstarted state).
     *
     * The exception is thrown 3 times.
     *
     * This causes the transition to be discharged from the hospital 3 times (retries 3 times). On the final retry the transition
     * succeeds and the flow finishes.
     *
     * Each time the flow retries, it starts from the beginning of the flow (due to being in an unstarted state).
     *
     */
    @Test(timeout = 300_000)
    fun `error during transition with CommitTransaction action that occurs during flow initialisation will retry and complete successfully`() {
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

                RULE Throw exception on executeCommitTransaction action
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF readCounter("counter") < 3
                DO incrementCounter("counter"); traceln("Throwing exception"); throw new java.sql.SQLException("die dammit die", "1")
                ENDRULE
                
                RULE Log external start flow event
                CLASS $stateMachineManagerClassName
                METHOD onExternalStartFlow
                AT ENTRY
                IF true
                DO traceln("External start flow event")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            alice.rpc.startFlow(
                StateMachineErrorHandlingTest::SendAMessageFlow,
                charlie.nodeInfo.singleIdentity()
            ).returnValue.getOrThrow(
                30.seconds
            )

            alice.assertBytemanOutput("External start flow event", 4)
            alice.rpc.assertNumberOfCheckpointsAllZero()
            alice.rpc.assertHospitalCounts(discharged = 3)
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * Throws an exception when calling [FlowStateMachineImpl.processEvent].
     *
     * This is not an expected place for an exception to occur, but allows us to test what happens when a random exception is propagated
     * up to [FlowStateMachineImpl.run] during flow initialisation.
     *
     * A "Transaction context is missing" exception is thrown due to where the exception is thrown (no transaction is created so this is
     * thrown when leaving [FlowStateMachineImpl.processEventsUntilFlowIsResumed] due to the finally block).
     */
    @Test(timeout = 300_000)
    fun `unexpected error during flow initialisation throws exception to client`() {
        startDriver {
            val (charlie, alice, port) = createNodeAndBytemanNode()
            val rules = """
                RULE Create Counter
                CLASS ${FlowStateMachineImpl::class.java.name}
                METHOD processEvent
                AT ENTRY
                IF createCounter("counter", $counter)
                DO traceln("Counter created")
                ENDRULE
                
                RULE Throw exception
                CLASS ${FlowStateMachineImpl::class.java.name}
                METHOD processEvent
                AT ENTRY
                IF readCounter("counter") < 1
                DO incrementCounter("counter"); traceln("Throwing exception"); throw new java.lang.RuntimeException("die dammit die")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            assertFailsWith<CordaRuntimeException> {
                alice.rpc.startFlow(
                    StateMachineErrorHandlingTest::SendAMessageFlow,
                    charlie.nodeInfo.singleIdentity()
                ).returnValue.getOrThrow(30.seconds)
            }

            alice.rpc.assertNumberOfCheckpointsAllZero()
            alice.rpc.assertHospitalCounts(propagated = 1)
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * Throws an exception when performing an [Action.CommitTransaction] event before the flow has initialised and saved its first checkpoint
     * (remains in an unstarted state).
     *
     * A [SQLException] is then thrown when trying to rollback the flow's database transaction.
     *
     * The [SQLException] should be suppressed and the flow should continue to retry and complete successfully.
     */
    @Test(timeout = 300_000)
    fun `error during initialisation when trying to rollback the flow's database transaction the flow is able to retry and complete successfully`() {
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

                RULE Throw exception on executeCommitTransaction action
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF readCounter("counter") == 0
                DO incrementCounter("counter"); traceln("Throwing exception"); throw new java.lang.RuntimeException("die dammit die")
                ENDRULE
                
                RULE Throw exception when rolling back transaction in transition executor
                INTERFACE ${Connection::class.java.name}
                METHOD rollback
                AT ENTRY
                IF readCounter("counter") == 1
                DO incrementCounter("counter"); traceln("Throwing exception in transition executor"); throw new java.sql.SQLException("could not reach db", "1")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            alice.rpc.startFlow(
                StateMachineErrorHandlingTest::SendAMessageFlow,
                charlie.nodeInfo.singleIdentity()
            ).returnValue.getOrThrow(30.seconds)

            alice.rpc.assertNumberOfCheckpointsAllZero()
            alice.rpc.assertHospitalCounts(discharged = 1)
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * Throws an exception when performing an [Action.CommitTransaction] event before the flow has initialised and saved its first checkpoint
     * (remains in an unstarted state).
     *
     * A [SQLException] is then thrown when trying to close the flow's database transaction.
     *
     * The [SQLException] should be suppressed and the flow should continue to retry and complete successfully.
     */
    @Test(timeout = 300_000)
    fun `error during initialisation when trying to close the flow's database transaction the flow is able to retry and complete successfully`() {
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

                RULE Throw exception on executeCommitTransaction action
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF readCounter("counter") == 0
                DO incrementCounter("counter"); traceln("Throwing exception"); throw new java.lang.RuntimeException("die dammit die")
                ENDRULE
                
                RULE Throw exception when rolling back transaction in transition executor
                INTERFACE ${Connection::class.java.name}
                METHOD close
                AT ENTRY
                IF readCounter("counter") == 1
                DO incrementCounter("counter"); traceln("Throwing exception in transition executor"); throw new java.sql.SQLException("could not reach db", "1")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            alice.rpc.startFlow(
                StateMachineErrorHandlingTest::SendAMessageFlow,
                charlie.nodeInfo.singleIdentity()
            ).returnValue.getOrThrow(30.seconds)

            alice.rpc.assertNumberOfCheckpointsAllZero()
            alice.rpc.assertHospitalCounts(discharged = 1)
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * Throws an exception when performing an [Action.CommitTransaction] event before the flow has initialised and saved its first checkpoint
     * (remains in an unstarted state).
     *
     * The exception is thrown 4 times.
     *
     * This causes the transition to be discharged from the hospital 3 times (retries 3 times) and then be kept in for observation.
     *
     * Each time the flow retries, it starts from the beginning of the flow (due to being in an unstarted state).
     */
    @Test(timeout = 450_000)
    fun `error during transition with CommitTransaction action that occurs during flow initialisation will retry and be kept for observation if error persists`() {
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

                RULE Throw exception on executeCommitTransaction action
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF readCounter("counter") < 4
                DO incrementCounter("counter"); traceln("Throwing exception"); throw new java.sql.SQLException("die dammit die", "1")
                ENDRULE
                
                RULE Log external start flow event
                CLASS $stateMachineManagerClassName
                METHOD onExternalStartFlow
                AT ENTRY
                IF true
                DO traceln("External start flow event")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            executor.execute {
                alice.rpc.startFlow(StateMachineErrorHandlingTest::SendAMessageFlow, charlie.nodeInfo.singleIdentity())
            }

            // flow is not signaled as started calls to [getOrThrow] will hang, sleeping instead
            Thread.sleep(30.seconds.toMillis())

            alice.assertBytemanOutput("External start flow event", 4)
            alice.rpc.assertNumberOfCheckpoints(hospitalized = 1)
            alice.rpc.assertHospitalCounts(
                discharged = 3,
                observation = 1
            )
            assertEquals(1, alice.rpc.stateMachinesSnapshot().size)
            alice.stop()
            assertTrue(!alice.running(), "The node must be shutdown before it can be restarted")
            val (alice2, _) = createBytemanNode()
            Thread.sleep(20.seconds.toMillis())
            alice2.rpc.assertNumberOfCheckpointsAllZero()
        }
    }

    /**
     * Throws an exception when performing an [Action.CommitTransaction] event before the flow has initialised and saved its first checkpoint
     * (remains in an unstarted state).
     *
     * An exception is thrown when committing a database transaction during a transition to trigger the retry of the flow. Another
     * exception is then thrown during the retry itself.
     *
     * The flow then retries the retry causing the flow to complete successfully.
     */
    @Test(timeout = 300_000)
    fun `error during retrying a flow that failed when committing its original checkpoint will retry the flow again and complete successfully`() {
        startDriver {
            val (charlie, alice, port) = createNodeAndBytemanNode()

            val rules = """
                RULE Throw exception on executeCommitTransaction action after first suspend + commit
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF !flagged("commit_exception_flag")
                DO flag("commit_exception_flag"); traceln("Throwing exception"); throw new java.sql.SQLException("die dammit die", "1")
                ENDRULE
                
                RULE Throw exception on retry
                CLASS $stateMachineManagerClassName
                METHOD onExternalStartFlow
                AT ENTRY
                IF flagged("commit_exception_flag") && !flagged("retry_exception_flag")
                DO flag("retry_exception_flag"); traceln("Throwing retry exception"); throw new java.lang.RuntimeException("Here we go again")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            alice.rpc.startFlow(
                StateMachineErrorHandlingTest::SendAMessageFlow,
                charlie.nodeInfo.singleIdentity()
            ).returnValue.getOrThrow(
                30.seconds
            )

            alice.rpc.assertNumberOfCheckpointsAllZero()
            alice.rpc.assertHospitalCounts(
                discharged = 1,
                dischargedRetry = 1
            )
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * Throws an exception when after the first [Action.CommitTransaction] event before the flow has initialised (remains in an unstarted state).
     * This is to cover transient issues, where the transaction committed the checkpoint but failed to respond to the node.
     *
     * The exception is thrown when performing [Action.SignalFlowHasStarted], the error won't actually appear here but it makes it easier
     * to test.
     *
     * The exception is thrown 3 times.
     *
     * This causes the transition to be discharged from the hospital 3 times (retries 3 times). On the final retry the transition
     * succeeds and the flow finishes.
     *
     * Each time the flow retries, it starts from the beginning of the flow (due to being in an unstarted state).
     *
     * The first retry will load the checkpoint that the flow doesn't know exists ([StateMachineState.isAnyCheckpointPersisted] is false
     * at this point). The flag gets switched to true after this first retry and the flow has now returned to an expected state.
     *
     */
    @Test(timeout = 300_000)
    fun `error during transition when checkpoint commits but transient db exception is thrown during flow initialisation will retry and complete successfully`() {
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

                RULE Flag when commit transaction reached
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF true
                DO flag("commit")
                ENDRULE
                
                RULE Throw exception on executeSignalFlowHasStarted action
                CLASS ${DatabaseTransaction::class.java.name}
                METHOD commit
                AT EXIT
                IF readCounter("counter") < 3 && flagged("commit")
                DO incrementCounter("counter"); clear("commit"); traceln("Throwing exception"); throw new java.sql.SQLException("you thought it worked didnt you!", "1")
                ENDRULE
                
                RULE Log external start flow event
                CLASS $stateMachineManagerClassName
                METHOD onExternalStartFlow
                AT ENTRY
                IF true
                DO traceln("External start flow event")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            alice.rpc.startFlow(
                StateMachineErrorHandlingTest::SendAMessageFlow,
                charlie.nodeInfo.singleIdentity()
            ).returnValue.getOrThrow(
                30.seconds
            )

            alice.assertBytemanOutput("External start flow event", 1)
            alice.rpc.assertNumberOfCheckpointsAllZero()
            alice.rpc.assertHospitalCounts(discharged = 3)
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * Throws an exception when performing an [Action.CommitTransaction] event before the flow has initialised and saved its first checkpoint
     * (remains in an unstarted state).
     *
     * The exception is thrown 3 times.
     *
     * This causes the transition to be discharged from the hospital 3 times (retries 3 times). On the final retry the transition
     * succeeds and the flow finishes.
     *
     * Each time the flow retries, it starts from the beginning of the flow (due to being in an unstarted state).
     *
     */
    @Test(timeout = 300_000)
    fun `with client id - error during transition with CommitTransaction action that occurs during flow initialisation will retry and complete successfully`() {
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

                RULE Throw exception on executeCommitTransaction action
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF readCounter("counter") < 3
                DO incrementCounter("counter"); traceln("Throwing exception"); throw new java.sql.SQLException("die dammit die", "1")
                ENDRULE
                
                RULE Log external start flow event
                CLASS $stateMachineManagerClassName
                METHOD onExternalStartFlow
                AT ENTRY
                IF true
                DO traceln("External start flow event")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            alice.rpc.startFlowWithClientId(
                "here is my client id",
                StateMachineErrorHandlingTest::SendAMessageFlow,
                charlie.nodeInfo.singleIdentity()
            ).returnValue.getOrThrow(
                30.seconds
            )

            alice.assertBytemanOutput("External start flow event", 4)
            alice.rpc.assertNumberOfCheckpoints(completed = 1)
            alice.rpc.assertHospitalCounts(discharged = 3)
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * Throws an exception when calling [FlowStateMachineImpl.processEvent].
     *
     * This is not an expected place for an exception to occur, but allows us to test what happens when a random exception is propagated
     * up to [FlowStateMachineImpl.run] during flow initialisation.
     *
     * A "Transaction context is missing" exception is thrown due to where the exception is thrown (no transaction is created so this is
     * thrown when leaving [FlowStateMachineImpl.processEventsUntilFlowIsResumed] due to the finally block).
     */
    @Test(timeout = 300_000)
    fun `with client id - unexpected error during flow initialisation throws exception to client`() {
        startDriver {
            val (charlie, alice, port) = createNodeAndBytemanNode()
            val rules = """
                RULE Create Counter
                CLASS ${FlowStateMachineImpl::class.java.name}
                METHOD processEvent
                AT ENTRY
                IF createCounter("counter", $counter)
                DO traceln("Counter created")
                ENDRULE
                
                RULE Throw exception
                CLASS ${FlowStateMachineImpl::class.java.name}
                METHOD processEvent
                AT ENTRY
                IF readCounter("counter") < 1
                DO incrementCounter("counter"); traceln("Throwing exception"); throw new java.lang.RuntimeException("die dammit die")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            assertFailsWith<CordaRuntimeException> {
                alice.rpc.startFlowWithClientId(
                    "give me all of your client ids, or else",
                    StateMachineErrorHandlingTest::SendAMessageFlow,
                    charlie.nodeInfo.singleIdentity()
                ).returnValue.getOrThrow(30.seconds)
            }

            alice.rpc.assertNumberOfCheckpoints(failed = 1)
            alice.rpc.assertHospitalCounts(propagated = 1)
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * Throws an exception when performing an [Action.CommitTransaction] event before the flow has initialised and saved its first checkpoint
     * (remains in an unstarted state).
     *
     * The exception is thrown 4 times.
     *
     * This causes the transition to be discharged from the hospital 3 times (retries 3 times) and then be kept in for observation.
     *
     * Each time the flow retries, it starts from the beginning of the flow (due to being in an unstarted state).
     */
    @Test(timeout = 450_000)
    fun `with client id - error during transition with CommitTransaction action that occurs during flow initialisation will retry and be kept for observation if error persists`() {
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

                RULE Throw exception on executeCommitTransaction action
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF readCounter("counter") < 4
                DO incrementCounter("counter"); traceln("Throwing exception"); throw new java.sql.SQLException("die dammit die", "1")
                ENDRULE
                
                RULE Log external start flow event
                CLASS $stateMachineManagerClassName
                METHOD onExternalStartFlow
                AT ENTRY
                IF true
                DO traceln("External start flow event")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            executor.execute {
                alice.rpc.startFlowWithClientId(
                    "please sir, can i have a client id?",
                    StateMachineErrorHandlingTest::SendAMessageFlow,
                    charlie.nodeInfo.singleIdentity()
                )
            }

            // flow is not signaled as started calls to [getOrThrow] will hang, sleeping instead
            Thread.sleep(30.seconds.toMillis())

            alice.assertBytemanOutput("External start flow event", 4)
            alice.rpc.assertNumberOfCheckpoints(hospitalized = 1)
            alice.rpc.assertHospitalCounts(
                discharged = 3,
                observation = 1
            )
            assertEquals(1, alice.rpc.stateMachinesSnapshot().size)
            alice.stop()
            val (alice2, _) = createBytemanNode()
            Thread.sleep(20.seconds.toMillis())
            alice2.rpc.assertNumberOfCheckpoints(completed = 1)
        }
    }

    /**
     * Throws an exception when performing an [Action.CommitTransaction] event before the flow has initialised and saved its first checkpoint
     * (remains in an unstarted state).
     *
     * An exception is thrown when committing a database transaction during a transition to trigger the retry of the flow. Another
     * exception is then thrown during the retry itself.
     *
     * The flow then retries the retry causing the flow to complete successfully.
     */
    @Test(timeout = 300_000)
    fun `with client id - error during retrying a flow that failed when committing its original checkpoint will retry the flow again and complete successfully`() {
        startDriver {
            val (charlie, alice, port) = createNodeAndBytemanNode()

            val rules = """
                RULE Throw exception on executeCommitTransaction action after first suspend + commit
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF !flagged("commit_exception_flag")
                DO flag("commit_exception_flag"); traceln("Throwing exception"); throw new java.sql.SQLException("die dammit die", "1")
                ENDRULE
                
                RULE Throw exception on retry
                CLASS $stateMachineManagerClassName
                METHOD onExternalStartFlow
                AT ENTRY
                IF flagged("commit_exception_flag") && !flagged("retry_exception_flag")
                DO flag("retry_exception_flag"); traceln("Throwing retry exception"); throw new java.lang.RuntimeException("Here we go again")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            alice.rpc.startFlowWithClientId(
                "hi, i'd like to be your client id",
                StateMachineErrorHandlingTest::SendAMessageFlow,
                charlie.nodeInfo.singleIdentity()
            ).returnValue.getOrThrow(
                30.seconds
            )

            alice.rpc.assertNumberOfCheckpoints(completed = 1)
            alice.rpc.assertHospitalCounts(
                discharged = 1,
                dischargedRetry = 1
            )
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * Throws an exception when after the first [Action.CommitTransaction] event before the flow has initialised (remains in an unstarted state).
     * This is to cover transient issues, where the transaction committed the checkpoint but failed to respond to the node.
     *
     * The exception is thrown when performing [Action.SignalFlowHasStarted], the error won't actually appear here but it makes it easier
     * to test.
     *
     * The exception is thrown 3 times.
     *
     * This causes the transition to be discharged from the hospital 3 times (retries 3 times). On the final retry the transition
     * succeeds and the flow finishes.
     *
     * Each time the flow retries, it starts from the beginning of the flow (due to being in an unstarted state).
     *
     * The first retry will load the checkpoint that the flow doesn't know exists ([StateMachineState.isAnyCheckpointPersisted] is false
     * at this point). The flag gets switched to true after this first retry and the flow has now returned to an expected state.
     *
     */
    @Test(timeout = 300_000)
    fun `with client id - error during transition when checkpoint commits but transient db exception is thrown during flow initialisation will retry and complete successfully`() {
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

                RULE Flag when commit transaction reached
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF true
                DO flag("commit")
                ENDRULE
                
                RULE Throw exception on executeSignalFlowHasStarted action
                CLASS ${DatabaseTransaction::class.java.name}
                METHOD commit
                AT EXIT
                IF readCounter("counter") < 3 && flagged("commit")
                DO incrementCounter("counter"); clear("commit"); traceln("Throwing exception"); throw new java.sql.SQLException("you thought it worked didnt you!", "1")
                ENDRULE
                
                RULE Log external start flow event
                CLASS $stateMachineManagerClassName
                METHOD onExternalStartFlow
                AT ENTRY
                IF true
                DO traceln("External start flow event")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            alice.rpc.startFlowWithClientId(
                "hello im a client id",
                StateMachineErrorHandlingTest::SendAMessageFlow,
                charlie.nodeInfo.singleIdentity()
            ).returnValue.getOrThrow(
                30.seconds
            )

            alice.assertBytemanOutput("External start flow event", 1)
            alice.rpc.assertNumberOfCheckpoints(completed = 1)
            alice.rpc.assertHospitalCounts(discharged = 3)
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * Throws an exception when performing an [Action.CommitTransaction] event on a responding node before the flow has initialised and
     * saved its first checkpoint (remains in an unstarted state).
     *
     * The exception is thrown 3 times.
     *
     * This causes the transition to be discharged from the hospital 3 times (retries 3 times). On the final retry the transition
     * succeeds and the flow finishes.
     *
     * Each time the flow retries, it starts from the beginning of the flow (due to being in an unstarted state).
     */
    @Test(timeout = 300_000)
    fun `responding flow - error during transition with CommitTransaction action that occurs during flow initialisation will retry and complete successfully`() {
        startDriver {
            val (alice, charlie, port) = createNodeAndBytemanNode()

            val rules = """
                RULE Create Counter
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF createCounter("counter", $counter)
                DO traceln("Counter created")
                ENDRULE

                RULE Throw exception on executeCommitTransaction action
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF readCounter("counter") < 3
                DO incrementCounter("counter"); traceln("Throwing exception"); throw new java.sql.SQLException("die dammit die", "1")
                ENDRULE
                
                RULE Log session init event
                CLASS $stateMachineManagerClassName
                METHOD onSessionInit
                AT ENTRY
                IF true
                DO traceln("On session init event")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            alice.rpc.startFlow(
                StateMachineErrorHandlingTest::SendAMessageFlow,
                charlie.nodeInfo.singleIdentity()
            ).returnValue.getOrThrow(
                30.seconds
            )

            alice.rpc.assertNumberOfCheckpointsAllZero()
            charlie.assertBytemanOutput("On session init event", 4)
            charlie.rpc.assertNumberOfCheckpointsAllZero()
            charlie.rpc.assertHospitalCounts(discharged = 3)
        }
    }

    /**
     * Throws an exception when performing an [Action.CommitTransaction] event on a responding node before the flow has initialised and
     * saved its first checkpoint (remains in an unstarted state).
     *
     * The exception is thrown 4 times.
     *
     * This causes the transition to be discharged from the hospital 3 times (retries 3 times) and then be kept in for observation.
     *
     * Each time the flow retries, it starts from the beginning of the flow (due to being in an unstarted state).
     */
    @Test(timeout = 450_000)
    fun `responding flow - error during transition with CommitTransaction action that occurs during flow initialisation will retry and be kept for observation if error persists`() {
        startDriver {
            val (alice, charlie, port) = createNodeAndBytemanNode()

            val rules = """
                RULE Create Counter
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF createCounter("counter", $counter)
                DO traceln("Counter created")
                ENDRULE

                RULE Throw exception on executeCommitTransaction action
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF readCounter("counter") < 4
                DO incrementCounter("counter"); traceln("Throwing exception"); throw new java.sql.SQLException("die dammit die", "1")
                ENDRULE
                
                RULE Log session init event
                CLASS $stateMachineManagerClassName
                METHOD onSessionInit
                AT ENTRY
                IF true
                DO traceln("On session init event")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            executor.execute {
                alice.rpc.startFlow(StateMachineErrorHandlingTest::SendAMessageFlow, charlie.nodeInfo.singleIdentity())
            }

            // flow is not signaled as started calls to [getOrThrow] will hang, sleeping instead
            Thread.sleep(30.seconds.toMillis())

            alice.rpc.assertNumberOfCheckpoints(runnable = 1)
            charlie.assertBytemanOutput("On session init event", 4)
            charlie.rpc.assertNumberOfCheckpoints(hospitalized = 1)
            charlie.rpc.assertHospitalCounts(
                discharged = 3,
                observation = 1
            )
            assertEquals(1, alice.rpc.stateMachinesSnapshot().size)
            assertEquals(1, charlie.rpc.stateMachinesSnapshot().size)
            charlie.stop()
            assertTrue(!charlie.running(), "The node must be shutdown before it can be restarted")
            val (charlie2, _) = createBytemanNode()
            Thread.sleep(10.seconds.toMillis())
            alice.rpc.assertNumberOfCheckpointsAllZero()
            charlie2.rpc.assertNumberOfCheckpointsAllZero()
        }
    }

    /**
     * Throws an exception when performing an [Action.CommitTransaction] event before the flow has suspended (remains in an unstarted
     * state) on a responding node.
     *
     * The exception is thrown 3 times.
     *
     * An exception is also thrown from [CheckpointStorage.getCheckpoint].
     *
     * This test is to prevent a regression, where a transient database connection error can be thrown retrieving a flow's checkpoint when
     * retrying the flow after it failed to commit it's original checkpoint.
     *
     * This causes the transition to be discharged from the hospital 3 times (retries 3 times). On the final retry the transition
     * succeeds and the flow finishes.
     */
    @Test(timeout = 300_000)
    fun `responding flow - session init can be retried when there is a transient connection error to the database`() {
        startDriver {
            val (alice, charlie, port) = createNodeAndBytemanNode()

            val rules = """
                RULE Create Counter
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF createCounter("counter", $counter) && createCounter("counter_2", $counter) 
                DO traceln("Counter created")
                ENDRULE

                RULE Throw exception on executeCommitTransaction action
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF readCounter("counter") < 3
                DO incrementCounter("counter"); traceln("Throwing exception"); throw new java.lang.RuntimeException("die dammit die")
                ENDRULE
                
                RULE Throw exception on getCheckpoint
                INTERFACE ${CheckpointStorage::class.java.name}
                METHOD getCheckpoint
                AT ENTRY
                IF readCounter("counter_2") < 3
                DO incrementCounter("counter_2"); traceln("Throwing exception getting checkpoint"); throw new java.sql.SQLTransientConnectionException("Connection is not available")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            alice.rpc.startFlow(
                StateMachineErrorHandlingTest::SendAMessageFlow,
                charlie.nodeInfo.singleIdentity()
            ).returnValue.getOrThrow(
                30.seconds
            )

            alice.rpc.assertNumberOfCheckpointsAllZero()
            charlie.rpc.assertHospitalCounts(
                discharged = 3,
                dischargedRetry = 1
            )
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
            assertEquals(0, charlie.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * Throws an exception when performing an [Action.CommitTransaction] event before the flow has suspended (remains in an unstarted
     * state) on a responding node.
     *
     * The exception is thrown 4 times.
     *
     * An exception is also thrown from [CheckpointStorage.getCheckpoint].
     *
     * This test is to prevent a regression, where a transient database connection error can be thrown retrieving a flow's checkpoint when
     * retrying the flow after it failed to commit it's original checkpoint.
     *
     * This causes the transition to be discharged from the hospital 3 times (retries 3 times). On the final retry the transition
     * fails and is kept for in for observation.
     */
    @Test(timeout = 300_000)
    fun `responding flow - session init can be retried when there is a transient connection error to the database goes to observation if error persists`() {
        startDriver {
            val (alice, charlie, port) = createNodeAndBytemanNode()

            val rules = """
                RULE Create Counter
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF createCounter("counter", $counter) && createCounter("counter_2", $counter) 
                DO traceln("Counter created")
                ENDRULE

                RULE Throw exception on executeCommitTransaction action
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF readCounter("counter") < 4
                DO incrementCounter("counter"); traceln("Throwing exception"); throw new java.lang.RuntimeException("die dammit die")
                ENDRULE
                
                RULE Throw exception on getCheckpoint
                INTERFACE ${CheckpointStorage::class.java.name}
                METHOD getCheckpoint
                AT ENTRY
                IF readCounter("counter_2") < 3
                DO incrementCounter("counter_2"); traceln("Throwing exception getting checkpoint"); throw new java.sql.SQLTransientConnectionException("Connection is not available")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            assertFailsWith<TimeoutException> {
                alice.rpc.startFlow(
                    StateMachineErrorHandlingTest::SendAMessageFlow,
                    charlie.nodeInfo.singleIdentity()
                ).returnValue.getOrThrow(
                    30.seconds
                )
            }

            charlie.rpc.assertNumberOfCheckpoints(hospitalized = 1)
            charlie.rpc.assertHospitalCounts(
                discharged = 3,
                observation = 1,
                dischargedRetry = 1
            )
            assertEquals(1, alice.rpc.stateMachinesSnapshot().size)
            assertEquals(1, charlie.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * Throws an exception when after the first [Action.CommitTransaction] event before the flow has initialised (remains in an unstarted state).
     * This is to cover transient issues, where the transaction committed the checkpoint but failed to respond to the node.
     *
     * The exception is thrown when performing [Action.SignalFlowHasStarted], the error won't actually appear here but it makes it easier
     * to test.
     *
     * The exception is thrown 3 times.
     *
     * This causes the transition to be discharged from the hospital 3 times (retries 3 times). On the final retry the transition
     * succeeds and the flow finishes.
     *
     * Each time the flow retries, it starts from the beginning of the flow (due to being in an unstarted state).
     *
     * The first retry will load the checkpoint that the flow doesn't know exists ([StateMachineState.isAnyCheckpointPersisted] is false
     * at this point). The flag gets switched to true after this first retry and the flow has now returned to an expected state.
     *
     */
    @Test(timeout = 300_000)
    fun `responding flow - error during transition when checkpoint commits but transient db exception is thrown during flow initialisation will retry and complete successfully`() {
        startDriver {
            val (alice, charlie, port) = createNodeAndBytemanNode()

            val rules = """
                RULE Create Counter
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF createCounter("counter", $counter)
                DO traceln("Counter created")
                ENDRULE

               RULE Flag when commit transaction reached
                CLASS $actionExecutorClassName
                METHOD executeCommitTransaction
                AT ENTRY
                IF true
                DO flag("commit")
                ENDRULE
                
                RULE Throw exception on executeSignalFlowHasStarted action
                CLASS ${DatabaseTransaction::class.java.name}
                METHOD commit
                AT EXIT
                IF readCounter("counter") < 3 && flagged("commit")
                DO incrementCounter("counter"); clear("commit"); traceln("Throwing exception"); throw new java.sql.SQLException("you thought it worked didnt you!", "1")
                ENDRULE
                
                RULE Log session init event
                CLASS $stateMachineManagerClassName
                METHOD onSessionInit
                AT ENTRY
                IF true
                DO traceln("On session init event")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            alice.rpc.startFlow(
                StateMachineErrorHandlingTest::SendAMessageFlow,
                charlie.nodeInfo.singleIdentity()
            ).returnValue.getOrThrow(
                30.seconds
            )

            alice.rpc.assertNumberOfCheckpointsAllZero()
            charlie.assertBytemanOutput("On session init event", 1)
            charlie.rpc.assertNumberOfCheckpointsAllZero()
            charlie.rpc.assertHospitalCounts(discharged = 3)
        }
    }
}