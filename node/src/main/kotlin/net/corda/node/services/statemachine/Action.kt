package net.corda.node.services.statemachine

import net.corda.core.flows.Destination
import net.corda.core.flows.StateMachineRunId
import net.corda.core.identity.Party
import net.corda.core.internal.FlowAsyncOperation
import net.corda.node.services.messaging.DeduplicationHandler
import java.time.Instant
import java.util.*

/**
 * [Action]s are reified IO actions to execute as part of state machine transitions.
 */
sealed class Action {

    /**
     * Send an initial session message to [destination].
     */
    data class SendInitial(
            val destination: Destination,
            val initialise: InitialSessionMessage,
            val deduplicationId: SenderDeduplicationId
    ) : Action()

    /**
     * Send a session message to a [peerParty] with which we have an established session.
     */
    data class SendExisting(
            val peerParty: Party,
            val message: ExistingSessionMessage,
            val deduplicationId: SenderDeduplicationId
    ) : Action()

    /**
     * Send session messages to multiple destinations.
     *
     * @property sendInitial session messages to send in order to establish a session.
     * @property sendExisting session messages to send to existing sessions.
     */
    data class SendMultiple(
            val sendInitial: List<SendInitial>,
            val sendExisting: List<SendExisting>
    ): Action() {
        init {
            check(sendInitial.isNotEmpty() || sendExisting.isNotEmpty()) { "At least one of the lists with initial or existing session messages should contain items." }
        }
    }

    /**
     * Persist the specified [checkpoint].
     */
    data class PersistCheckpoint(val id: StateMachineRunId, val checkpoint: Checkpoint, val isCheckpointUpdate: Boolean) : Action()

    /**
     * Update only the [status] of the checkpoint with [id].
     */
    data class UpdateFlowStatus(val id: StateMachineRunId, val status: Checkpoint.FlowStatus): Action()

    /**
     * Remove the checkpoint corresponding to [id]. [mayHavePersistentResults] denotes that at the time of injecting a [RemoveCheckpoint]
     * the flow could have persisted its database result or exception.
     * For more information see [CheckpointStorage.removeCheckpoint].
     */
    data class RemoveCheckpoint(val id: StateMachineRunId, val mayHavePersistentResults: Boolean = false) : Action()

    /**
     * Remove a flow's exception from the database.
     *
     * @param id The id of the flow
     */
    data class RemoveFlowException(val id: StateMachineRunId) : Action()

    /**
     * Persist an exception to the database for the related flow.
     *
     * @param id The id of the flow
     * @param exception The exception to persist
     */
    data class AddFlowException(val id: StateMachineRunId, val exception: Throwable) : Action()

    /**
     * Persist the deduplication facts of [deduplicationHandlers].
     */
    data class PersistDeduplicationFacts(val deduplicationHandlers: List<DeduplicationHandler>) : Action()

    /**
     * Acknowledge messages in [deduplicationHandlers].
     */
    data class AcknowledgeMessages(val deduplicationHandlers: List<DeduplicationHandler>) : Action()

    /**
     * Propagate [errorMessages] to [sessions].
     * @param sessions a map from source session IDs to initiated sessions.
     */
    data class PropagateErrors(
            val errorMessages: List<ErrorSessionMessage>,
            val sessions: List<SessionState.Initiated>,
            val senderUUID: String?
    ) : Action()

    /**
     * Create a session binding from [sessionId] to [flowId] to allow routing of incoming messages.
     */
    data class AddSessionBinding(val flowId: StateMachineRunId, val sessionId: SessionId) : Action()

    /**
     * Remove the session bindings corresponding to [sessionIds].
     */
    data class RemoveSessionBindings(val sessionIds: Set<SessionId>) : Action()

    /**
     * Signal that the flow corresponding to [flowId] is considered started.
     */
    data class SignalFlowHasStarted(val flowId: StateMachineRunId) : Action()

    /**
     * Remove the flow corresponding to [flowId].
     */
    data class RemoveFlow(
            val flowId: StateMachineRunId,
            val removalReason: FlowRemovalReason,
            val lastState: StateMachineState
    ) : Action()

    /**
     * Move the flow corresponding to [flowId] to paused.
     */
    data class MoveFlowToPaused(val currentState: StateMachineState) : Action()

    /**
     * Schedule [event] to self.
     */
    data class ScheduleEvent(val event: Event) : Action()

    /**
     * Sleep until [time].
     */
    data class SleepUntil(val currentState: StateMachineState, val time: Instant) : Action()

    /**
     * Create a new database transaction.
     */
    object CreateTransaction : Action() {
        override fun toString() = "CreateTransaction"
    }

    /**
     * Roll back the current database transaction.
     */
    object RollbackTransaction : Action() {
        override fun toString() = "RollbackTransaction"
    }

    /**
     * Commit the current database transaction.
     */
    data class CommitTransaction(val currentState: StateMachineState) : Action() {
        override fun toString() = "CommitTransaction"
    }

    /**
     * Execute the specified [operation].
     */
    data class ExecuteAsyncOperation(
        val deduplicationId: String,
        val operation: FlowAsyncOperation<*>,
        val currentState: StateMachineState
    ) : Action()

    /**
     * Release soft locks associated with given ID (currently the flow ID).
     */
    data class ReleaseSoftLocks(val uuid: UUID?) : Action()

    /**
     * Retry a flow from the last checkpoint, or if there is no checkpoint, restart the flow with the same invocation details.
     */
    data class RetryFlowFromSafePoint(val currentState: StateMachineState) : Action()

    /**
     * Schedule the flow [flowId] to be retried if it does not complete within the timeout period specified in the configuration.
     *
     * Note that this only works with [TimedFlow].
     */
    data class ScheduleFlowTimeout(val flowId: StateMachineRunId) : Action()

    /**
     * Cancel the retry timeout for flow [flowId]. This must be called when a timed flow completes to prevent
     * unnecessary additional invocations.
     */
    data class CancelFlowTimeout(val flowId: StateMachineRunId) : Action()
}

/**
 * Reason for flow removal.
 */
sealed class FlowRemovalReason {
    data class OrderlyFinish(val flowReturnValue: Any?) : FlowRemovalReason()
    data class ErrorFinish(val flowErrors: List<FlowError>) : FlowRemovalReason()
    object SoftShutdown : FlowRemovalReason() {
        override fun toString() = "SoftShutdown"
    }
    // TODO Should we remove errored flows? How will the flow hospital work? Perhaps keep them in memory for a while, flush
    // them after a timeout, reload them on flow hospital request. In any case if we ever want to remove them
    // (e.g. temporarily) then add a case for that here.
}
