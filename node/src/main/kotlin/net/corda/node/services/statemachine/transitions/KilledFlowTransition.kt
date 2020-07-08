package net.corda.node.services.statemachine.transitions

import net.corda.core.flows.KilledFlowException
import net.corda.node.services.statemachine.Action
import net.corda.node.services.statemachine.Event
import net.corda.node.services.statemachine.FlowError
import net.corda.node.services.statemachine.StateMachineState

/**
 * [KilledFlowTransition] is the transition that is performed when a flow is killed. Logically it is almost identical to the error
 * propgation transition (found in [ErrorFlowTransition]).
 *
 * After completing the transition returned from [KilledFlowTransition], the flow will terminate.
 */
class KilledFlowTransition(
    override val context: TransitionContext,
    override val startingState: StateMachineState,
    val event: Event
) : Transition {

    override fun transition(): TransitionResult {
        return propagateErrorBuilder {

            val killedFlowError = createKilledFlowError()
            val killedFlowErrorMessage = createErrorMessageFromError(killedFlowError)
            val errorMessages = listOf(killedFlowErrorMessage)

            val (initiatedSessions, newSessions) = bufferErrorMessagesInInitiatingSessions(
                startingState.checkpoint.checkpointState.sessions,
                errorMessages
            )
            val newCheckpoint = startingState.checkpoint.setSessions(sessions = newSessions)
            currentState = currentState.copy(checkpoint = newCheckpoint)
            actions += Action.PropagateErrors(
                errorMessages,
                initiatedSessions,
                startingState.senderUUID
            )

            if (!startingState.isFlowResumed) {
                actions += Action.CreateTransaction
            }
            // The checkpoint and soft locks are also removed directly in [StateMachineManager.killFlow]
            if (startingState.isAnyCheckpointPersisted) {
                actions.add(Action.RemoveCheckpoint(context.id))
            }

            currentState = currentState.copy(
                pendingDeduplicationHandlers = emptyList(),
                isRemoved = true
            )

            addCleanupActions(listOf(killedFlowError))

            FlowContinuation.Abort
        }
    }

    private fun createKilledFlowError(): FlowError {
        val exception = when (event) {
            is Event.Error -> event.exception
            else -> KilledFlowException(context.id)
        }
        return FlowError(context.secureRandom.nextLong(), exception)
    }
}