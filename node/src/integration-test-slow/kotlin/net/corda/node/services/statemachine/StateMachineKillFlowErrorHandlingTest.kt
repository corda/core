package net.corda.node.services.statemachine

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.KilledFlowException
import net.corda.core.flows.StartableByRPC
import net.corda.core.messaging.startFlow
import net.corda.core.messaging.startTrackedFlow
import net.corda.core.utilities.ProgressTracker
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.seconds
import net.corda.testing.core.singleIdentity
import org.junit.Test
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeoutException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

@Suppress("MaxLineLength") // Byteman rules cannot be easily wrapped
class StateMachineKillFlowErrorHandlingTest : StateMachineErrorHandlingTest() {

    /**
     * Triggers `killFlow` while the flow is suspended causing a [InterruptedException] to be thrown and passed through the hospital.
     *
     * The flow terminates and is not retried.
     *
     * No pass through the hospital is recorded. As the flow is marked as `isRemoved`.
     */
    @Test(timeout = 300_000)
    fun `error during transition due to killing a flow will terminate the flow`() {
        startDriver {
            val alice = createNode()

            val flow = alice.rpc.startTrackedFlow(StateMachineKillFlowErrorHandlingTest::SleepFlow)

            var flowKilled = false
            flow.progress.subscribe {
                if (it == SleepFlow.STARTED.label) {
                    Thread.sleep(5000)
                    flowKilled = alice.rpc.killFlow(flow.id)
                }
            }

            assertFailsWith<KilledFlowException> { flow.returnValue.getOrThrow(20.seconds) }

            assertTrue(flowKilled)
            alice.rpc.assertNumberOfCheckpointsAllZero()
            alice.rpc.assertHospitalCountsAllZero()
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * Triggers `killFlow` during user application code.
     *
     * The user application code is mimicked by a [Thread.sleep] which is importantly not placed inside the [Suspendable]
     * call function. Placing it inside a [Suspendable] function causes quasar to behave unexpectedly.
     *
     * Although the call to kill the flow is made during user application code. It will not be removed / stop processing
     * until the next suspension point is reached within the flow.
     *
     * The flow terminates and is not retried.
     *
     * No pass through the hospital is recorded. As the flow is marked as `isRemoved`.
     */
    @Test(timeout = 300_000)
    fun `flow killed during user code execution stops and removes the flow correctly`() {
        startDriver {
            val alice = createNode()

            val flow = alice.rpc.startTrackedFlow(StateMachineKillFlowErrorHandlingTest::ThreadSleepFlow)

            var flowKilled = false
            flow.progress.subscribe {
                if (it == ThreadSleepFlow.STARTED.label) {
                    Thread.sleep(5000)
                    flowKilled = alice.rpc.killFlow(flow.id)
                }
            }

            assertFailsWith<KilledFlowException> { flow.returnValue.getOrThrow(30.seconds) }

            assertTrue(flowKilled)
            alice.rpc.assertNumberOfCheckpointsAllZero()
            alice.rpc.assertHospitalCountsAllZero()
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
        }
    }

    /**
     * Triggers `killFlow` after the flow has already been sent to observation. The flow is not running at this point and
     * all that remains is its checkpoint in the database.
     *
     * The flow terminates and is not retried.
     *
     * Killing the flow does not lead to any passes through the hospital. All the recorded passes through the hospital are
     * from the original flow that was put in for observation.
     */
    @Test(timeout = 300_000)
    fun `flow killed when it is in the flow hospital for observation is removed correctly`() {
        startDriver {
            val (charlie, alice, port) = createNodeAndBytemanNode()

            val rules = """
                RULE Create Counter
                CLASS $actionExecutorClassName
                METHOD executeSendMultiple
                AT ENTRY
                IF createCounter("counter", $counter)
                DO traceln("Counter created")
                ENDRULE

                RULE Throw exception on executeSendMultiple action
                CLASS $actionExecutorClassName
                METHOD executeSendMultiple
                AT ENTRY
                IF readCounter("counter") < 4
                DO incrementCounter("counter"); traceln("Throwing exception"); throw new java.lang.RuntimeException("die dammit die")
                ENDRULE
            """.trimIndent()

            submitBytemanRules(rules, port)

            val flow = alice.rpc.startFlow(StateMachineErrorHandlingTest::SendAMessageFlow, charlie.nodeInfo.singleIdentity())

            assertFailsWith<TimeoutException> { flow.returnValue.getOrThrow(20.seconds) }

            alice.rpc.killFlow(flow.id)

            alice.rpc.assertNumberOfCheckpointsAllZero()
            alice.rpc.assertHospitalCounts(
                discharged = 3,
                observation = 1
            )
            assertEquals(0, alice.rpc.stateMachinesSnapshot().size)
        }
    }

    @StartableByRPC
    class SleepFlow : FlowLogic<Unit>() {

        object STARTED : ProgressTracker.Step("I am ready to die")

        override val progressTracker = ProgressTracker(STARTED)

        @Suspendable
        override fun call() {
            sleep(Duration.of(1, ChronoUnit.SECONDS))
            progressTracker.currentStep = STARTED
            sleep(Duration.of(2, ChronoUnit.MINUTES))
        }
    }

    @StartableByRPC
    class ThreadSleepFlow : FlowLogic<Unit>() {

        object STARTED : ProgressTracker.Step("I am ready to die")

        override val progressTracker = ProgressTracker(STARTED)

        @Suspendable
        override fun call() {
            sleep(Duration.of(1, ChronoUnit.SECONDS))
            progressTracker.currentStep = STARTED
            logger.info("Starting ${ThreadSleepFlow::class.qualifiedName} application sleep")
            sleep()
            logger.info("Finished ${ThreadSleepFlow::class.qualifiedName} application sleep")
            sleep(Duration.of(2, ChronoUnit.MINUTES))
        }

        // Sleep is moved outside of `@Suspendable` function to prevent issues with Quasar
        private fun sleep() {
            Thread.sleep(20000)
        }
    }
}