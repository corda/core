package net.corda.node.services.statemachine.transitions

import net.corda.node.services.statemachine.Action
import net.corda.node.services.statemachine.Checkpoint
import net.corda.node.services.statemachine.ErrorSessionMessage
import net.corda.node.services.statemachine.ErrorState
import net.corda.node.services.statemachine.Event
import net.corda.node.services.statemachine.Event.ErrorOutcomeEvent.OvernightObservation
import net.corda.node.services.statemachine.Event.ErrorOutcomeEvent.RetryFlowFromSafePoint
import net.corda.node.services.statemachine.Event.ErrorOutcomeEvent.StartErrorPropagation
import net.corda.node.services.statemachine.FlowError
import net.corda.node.services.statemachine.StateMachineState

/**
 * This transition defines what should happen when a flow has errored.
 *
 * In general there are two flow-level error conditions:
 *
 *  - Internal exceptions. These may arise due to problems in the flow framework or errors during state machine
 *    transitions e.g. network or database failure.
 *  - User-raised exceptions. These are exceptions that are (re)raised in user code, allowing the user to catch them.
 *    These may come from illegal flow API calls, and FlowExceptions or other counterparty failures that are re-raised
 *    when the flow tries to use the corresponding sessions.
 *
 * Both internal exceptions and uncaught user-raised exceptions cause the flow to be errored. This flags the flow as
 *   unable to be resumed. When a flow is in this state an external source (e.g. Flow hospital) may decide to
 *
 *   1. Retry it. This throws away the errored state and re-tries from the last clean checkpoint.
 *   2. Start error propagation. This seals the flow as errored permanently and propagates the associated error(s) to
 *   all live sessions. This causes these sessions to errored on the other side, which may in turn cause the
 *   counter-flows themselves to errored. This also updates the flow's checkpoint's status to [Checkpoint.FlowStatus.FAILED].
 *   3. Keep the flow in for observation, to be retried at a later time. The flow will wait for new events to be passed
 *   to it from an external source to start up again. This also updates the flow's checkpoint's status to
 *   [Checkpoint.FlowStatus.HOSPITALIZED].
 *
 * See [net.corda.node.services.statemachine.interceptors.HospitalisingInterceptor] for how to detect flow errors.
 *
 * Note that in general we handle multiple errors at a time as several error conditions may arise at the same time and
 *   new errors may arise while the flow is in the errored state already.
 */
class ErrorFlowTransition(
    override val context: TransitionContext,
    override val startingState: StateMachineState,
    private val event: Event.ErrorOutcomeEvent
) : Transition {
    override fun transition(): TransitionResult {
        return when (event) {
            OvernightObservation -> overnightObservationTransition()
            RetryFlowFromSafePoint -> retryFlowFromSafePointTransition()
            StartErrorPropagation -> startErrorPropagationTransition()
        }
    }

    private fun overnightObservationTransition(): TransitionResult {
        return builder {
            val newCheckpoint = startingState.checkpoint.copy(status = Checkpoint.FlowStatus.HOSPITALIZED)
            actions += Action.CreateTransaction
            actions += Action.PersistCheckpoint(context.id, newCheckpoint, isCheckpointUpdate = currentState.isAnyCheckpointPersisted)
            actions += Action.CommitTransaction
            currentState = currentState.copy(checkpoint = newCheckpoint)
            FlowContinuation.ProcessEvents
        }
    }

    private fun retryFlowFromSafePointTransition(): TransitionResult {
        return builder {
            // Need to create a flow from the prior checkpoint or flow initiation.
            actions += Action.CreateTransaction
            actions += Action.RetryFlowFromSafePoint(startingState)
            actions += Action.CommitTransaction
            FlowContinuation.Abort
        }
    }

    private fun startErrorPropagationTransition(): TransitionResult {
        return propagateErrorBuilder {
            val errorState = currentState.checkpoint.errorState
            when (errorState) {
                ErrorState.Clean -> {
                    freshErrorTransition(UnexpectedEventInState())
                    FlowContinuation.ProcessEvents
                }
                is ErrorState.Errored -> {
                    val allErrors: List<FlowError> = errorState.errors
                    val remainingErrorsToPropagate: List<FlowError> = allErrors.subList(errorState.propagatedIndex, allErrors.size)
                    val errorMessages: List<ErrorSessionMessage> = remainingErrorsToPropagate.map(::createErrorMessageFromError)
                    if (remainingErrorsToPropagate.isNotEmpty()) {
                        val (initiatedSessions, newSessions) = bufferErrorMessagesInInitiatingSessions(
                            startingState.checkpoint.checkpointState.sessions,
                            errorMessages
                        )
                        val newCheckpoint = startingState.checkpoint.copy(
                            errorState = errorState.copy(propagatedIndex = allErrors.size),
                            checkpointState = startingState.checkpoint.checkpointState.copy(sessions = newSessions)
                        )
                        currentState = currentState.copy(checkpoint = newCheckpoint)
                        actions += Action.PropagateErrors(errorMessages, initiatedSessions, startingState.senderUUID)
                    }
                    if (!currentState.isRemoved) {
                        val newCheckpoint = startingState.checkpoint.copy(status = Checkpoint.FlowStatus.FAILED)

                        currentState = currentState.copy(
                            checkpoint = newCheckpoint,
                            pendingDeduplicationHandlers = emptyList(),
                            isRemoved = true
                        )

                        actions += Action.CreateTransaction
                        actions += Action.PersistCheckpoint(
                            context.id,
                            newCheckpoint,
                            isCheckpointUpdate = currentState.isAnyCheckpointPersisted
                        )

                        addCleanupActions(allErrors)

                        FlowContinuation.Abort
                    } else {
                        // Otherwise keep processing events. This branch happens when there are some outstanding initiating
                        // sessions that prevent the removal of the flow.
                        FlowContinuation.ProcessEvents
                    }
                }
            }
        }
    }
}
