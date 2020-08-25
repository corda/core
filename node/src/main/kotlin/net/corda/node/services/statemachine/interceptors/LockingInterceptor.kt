package net.corda.node.services.statemachine.interceptors

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.concurrent.CordaFuture
import net.corda.core.flows.StateMachineRunId
import net.corda.core.internal.concurrent.OpenFuture
import net.corda.core.utilities.contextLogger
import net.corda.node.services.statemachine.ActionExecutor
import net.corda.node.services.statemachine.Checkpoint
import net.corda.node.services.statemachine.ErrorState
import net.corda.node.services.statemachine.Event
import net.corda.node.services.statemachine.FlowFiber
import net.corda.node.services.statemachine.StaffedFlowHospital
import net.corda.node.services.statemachine.StateMachineInnerState
import net.corda.node.services.statemachine.StateMachineState
import net.corda.node.services.statemachine.TransitionExecutor
import net.corda.node.services.statemachine.transitions.FlowContinuation
import net.corda.node.services.statemachine.transitions.TransitionResult
import java.util.concurrent.Semaphore
import kotlin.reflect.full.functions

/**
 * This interceptor notifies the passed in [flowHospital] in case a flow went through a clean->errored or a errored->clean
 * transition.
 */
internal class LockingInterceptor(
    private val locks: MutableMap<StateMachineRunId, Pair<Checkpoint.FlowStatus, OpenFuture<Checkpoint.FlowStatus>>>,
    private val delegate: TransitionExecutor
) : TransitionExecutor {

    private companion object {
        val log = contextLogger()
    }

    @Suspendable
    override fun executeTransition(
        fiber: FlowFiber,
        previousState: StateMachineState,
        event: Event,
        transition: TransitionResult,
        actionExecutor: ActionExecutor
    ): Pair<FlowContinuation, StateMachineState> {

        val (continuation, nextState) = delegate.executeTransition(fiber, previousState, event, transition, actionExecutor)


        locks[fiber.id]?.let { (status, future) ->
            if (status == nextState.checkpoint.status) {
                log.info("Draining permits for flow ${fiber.id} / available: ${nextState.lock.availablePermits()}")
                nextState.lock.drainPermits()
                val method = Semaphore::class.java.getDeclaredMethod("reducePermits", Int::class.java)
                method.isAccessible = true
                method.invoke(nextState.lock, 1)
                future.set(status)
            }
        }

        return Pair(continuation, nextState)
    }
}
