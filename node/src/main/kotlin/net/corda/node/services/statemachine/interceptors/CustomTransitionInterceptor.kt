package net.corda.node.services.statemachine.interceptors

import net.corda.node.services.statemachine.ActionExecutor
import net.corda.node.services.statemachine.Event
import net.corda.node.services.statemachine.FlowFiber
import net.corda.node.services.statemachine.StateMachineState
import net.corda.node.services.statemachine.TransitionExecutor
import net.corda.node.services.statemachine.transitions.FlowContinuation
import net.corda.node.services.statemachine.transitions.TransitionResult

interface CustomTransitionInterceptor {
    fun intercept(
        fiber: FlowFiber,
        previousState: StateMachineState,
        event: Event,
        transition: TransitionResult,
        nextState: StateMachineState,
        continuation: FlowContinuation
    )
}

internal class CustomTransitionInterceptorHolder(
    private val delegate: TransitionExecutor,
    private val interceptor: CustomTransitionInterceptor
) : TransitionExecutor {

    override fun executeTransition(
        fiber: FlowFiber,
        previousState: StateMachineState,
        event: Event,
        transition: TransitionResult,
        actionExecutor: ActionExecutor
    ): Pair<FlowContinuation, StateMachineState> {
        return delegate.executeTransition(
            fiber,
            previousState,
            event,
            transition,
            actionExecutor
        ).also { (continuation, nextState) ->
            interceptor.intercept(fiber, previousState, event, transition, nextState, continuation)
        }
    }
}