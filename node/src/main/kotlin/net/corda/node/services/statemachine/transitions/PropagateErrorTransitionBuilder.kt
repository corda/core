package net.corda.node.services.statemachine.transitions

import net.corda.core.flows.FlowException
import net.corda.node.services.statemachine.Action
import net.corda.node.services.statemachine.DeduplicationId
import net.corda.node.services.statemachine.ErrorSessionMessage
import net.corda.node.services.statemachine.FlowError
import net.corda.node.services.statemachine.FlowRemovalReason
import net.corda.node.services.statemachine.SessionId
import net.corda.node.services.statemachine.SessionState
import net.corda.node.services.statemachine.StateMachineState

/**
 * [TransitionBuilder] that contains functions for performing error propagation.
 */
class PropagateErrorTransitionBuilder(context: TransitionContext, initialState: StateMachineState) : TransitionBuilder(context, initialState) {

    fun addCleanupActions(errors: List<FlowError>) {
        actions.addAll(
            arrayOf(
                Action.PersistDeduplicationFacts(currentState.pendingDeduplicationHandlers),
                Action.ReleaseSoftLocks(context.id.uuid),
                Action.CommitTransaction,
                Action.AcknowledgeMessages(currentState.pendingDeduplicationHandlers),
                Action.RemoveSessionBindings(currentState.checkpoint.checkpointState.sessions.keys),
                Action.RemoveFlow(context.id, FlowRemovalReason.ErrorFinish(errors), currentState)
            )
        )
    }

    fun createErrorMessageFromError(error: FlowError): ErrorSessionMessage {
        val exception = error.exception
        // If the exception doesn't contain an originalErrorId that means it's a fresh FlowException that should
        // propagate to the neighbouring flows. If it has the ID filled in that means it's a rethrown FlowException and
        // shouldn't be propagated.
        return if (exception is FlowException && exception.originalErrorId == null) {
            ErrorSessionMessage(flowException = exception, errorId = error.errorId)
        } else {
            ErrorSessionMessage(flowException = null, errorId = error.errorId)
        }
    }

    // Buffer error messages in Initiating sessions, return the initialised ones.
    fun bufferErrorMessagesInInitiatingSessions(
        sessions: Map<SessionId, SessionState>,
        errorMessages: List<ErrorSessionMessage>
    ): Pair<List<SessionState.Initiated>, Map<SessionId, SessionState>> {
        val newSessions = sessions.mapValues { (sourceSessionId, sessionState) ->
            if (sessionState is SessionState.Initiating && sessionState.rejectionError == null) {
                // *prepend* the error messages in order to error the other sessions ASAP. The other messages will
                // be delivered all the same, they just won't trigger flow resumption because of dirtiness.
                val errorMessagesWithDeduplication = errorMessages.map {
                    DeduplicationId.createForError(it.errorId, sourceSessionId) to it
                }
                sessionState.copy(bufferedMessages = errorMessagesWithDeduplication + sessionState.bufferedMessages)
            } else {
                sessionState
            }
        }
        val initiatedSessions = sessions.values.mapNotNull { session ->
            if (session is SessionState.Initiated && session.errors.isEmpty()) {
                session
            } else {
                null
            }
        }
        return Pair(initiatedSessions, newSessions)
    }
}