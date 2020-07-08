package net.corda.node.services.statemachine.transitions

import net.corda.core.flows.StateMachineRunId
import net.corda.node.services.statemachine.StateMachineState
import java.security.SecureRandom

/**
 * An interface used to separate out different parts of the state machine transition function.
 */
interface Transition {
    /** The context of the transition. */
    val context: TransitionContext
    /** The state the transition is starting in. */
    val startingState: StateMachineState

    /** The (almost) pure transition function. The only side-effect we allow is random number generation. */
    fun transition(): TransitionResult

    /**
     * A helper
     */
    fun builder(build: TransitionBuilder.() -> FlowContinuation): TransitionResult {
        val builder = TransitionBuilder(context, startingState)
        val continuation = build(builder)
        return TransitionResult(builder.currentState, builder.actions, continuation)
    }

    fun propagateErrorBuilder(build: PropagateErrorTransitionBuilder.() -> FlowContinuation): TransitionResult {
        val builder = PropagateErrorTransitionBuilder(context, startingState)
        val continuation = build(builder)
        return TransitionResult(builder.currentState, builder.actions, continuation)
    }
}

class TransitionContext(
        val id: StateMachineRunId,
        val secureRandom: SecureRandom
)
