package net.corda.node.services.statemachine.transitions

import net.corda.node.services.statemachine.ErrorState
import net.corda.node.services.statemachine.FlowState
import net.corda.node.services.statemachine.StateMachineState

/**
 * This transition checks the current state of the flow and determines whether anything needs to be done.
 */
class DoRemainingWorkTransition(
    override val context: TransitionContext,
    override val startingState: StateMachineState
) : Transition {
    override fun transition(): TransitionResult {
        val checkpoint = startingState.checkpoint
        // If the flow is removed or has been resumed don't do work.
        if (startingState.isFlowResumed || startingState.isRemoved) {
            return TransitionResult(startingState)
        }
        // Check whether the flow is errored
        return when (checkpoint.errorState) {
            is ErrorState.Clean -> cleanTransition()
            is ErrorState.Errored -> builder {
                // Being in an error state when processing this event is not expected but should not stop the flow from continuing
                FlowContinuation.ProcessEvents
            }
        }
    }

    // If the flow is clean check the FlowState
    private fun cleanTransition(): TransitionResult {
        val flowState = startingState.checkpoint.flowState
        return when (flowState) {
            is FlowState.Unstarted -> UnstartedFlowTransition(context, startingState, flowState).transition()
            is FlowState.Started -> StartedFlowTransition(context, startingState, flowState).transition()
            is FlowState.Completed -> throw IllegalStateException("Cannot transition a state with completed flow state.")
            is FlowState.Paused -> throw IllegalStateException("Cannot transition a state with paused flow state.")
        }
    }
}
