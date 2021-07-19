package net.corda.node.services.statemachine

import co.paralleluniverse.fibers.Suspendable
import com.codahale.metrics.Gauge
import com.codahale.metrics.Reservoir
import net.corda.core.serialization.SerializedBytes
import net.corda.core.serialization.internal.CheckpointSerializationContext
import net.corda.core.serialization.internal.checkpointSerialize
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.trace
import net.corda.node.services.api.CheckpointStorage
import net.corda.node.services.api.ServiceHubInternal
import net.corda.nodeapi.internal.persistence.contextDatabase
import net.corda.nodeapi.internal.persistence.contextTransaction
import net.corda.nodeapi.internal.persistence.contextTransactionOrNull
import java.sql.SQLException

/**
 * This is the bottom execution engine of flow side-effects.
 */
internal class ActionExecutorImpl(
    private val services: ServiceHubInternal,
    private val checkpointStorage: CheckpointStorage,
    private val flowMessaging: FlowMessaging,
    private val stateMachineManager: StateMachineManagerInternal,
    private val actionFutureExecutor: ActionFutureExecutor,
    private val checkpointSerializationContext: CheckpointSerializationContext
) : ActionExecutor {

    private companion object {
        val log = contextLogger()
    }

    /**
     * This [Gauge] just reports the sum of the bytes checkpointed during the last second.
     */
    private class LatchedGauge(private val reservoir: Reservoir) : Gauge<Long> {
        override fun getValue(): Long {
            return reservoir.snapshot.values.sum()
        }
    }

    @Suspendable
    override fun executeAction(fiber: FlowFiber, action: Action) {
        log.trace { "Flow ${fiber.id} executing $action" }
        return when (action) {
            is Action.PersistCheckpoint -> executePersistCheckpoint(action)
            is Action.PersistDeduplicationFacts -> executePersistDeduplicationIds(action)
            is Action.AcknowledgeMessages -> executeAcknowledgeMessages(action)
            is Action.PropagateErrors -> executePropagateErrors(action)
            is Action.ScheduleEvent -> executeScheduleEvent(fiber, action)
            is Action.SleepUntil -> executeSleepUntil(fiber, action)
            is Action.RemoveCheckpoint -> executeRemoveCheckpoint(action)
            is Action.SendInitial -> executeSendInitial(action)
            is Action.SendExisting -> executeSendExisting(action)
            is Action.SendMultiple -> executeSendMultiple(action)
            is Action.AddSessionBinding -> executeAddSessionBinding(action)
            is Action.RemoveSessionBindings -> executeRemoveSessionBindings(action)
            is Action.SignalFlowHasStarted -> executeSignalFlowHasStarted(action)
            is Action.RemoveFlow -> executeRemoveFlow(action)
            is Action.CreateTransaction -> executeCreateTransaction()
            is Action.RollbackTransaction -> executeRollbackTransaction()
            is Action.CommitTransaction -> executeCommitTransaction(action)
            is Action.ExecuteAsyncOperation -> executeAsyncOperation(fiber, action)
            is Action.ReleaseSoftLocks -> executeReleaseSoftLocks(action)
            is Action.RetryFlowFromSafePoint -> executeRetryFlowFromSafePoint(action)
            is Action.ScheduleFlowTimeout -> scheduleFlowTimeout(action)
            is Action.CancelFlowTimeout -> cancelFlowTimeout(action)
            is Action.MoveFlowToPaused -> executeMoveFlowToPaused(action)
            is Action.UpdateFlowStatus -> executeUpdateFlowStatus(action)
            is Action.RemoveFlowException -> executeRemoveFlowException(action)
            is Action.AddFlowException -> executeAddFlowException(action)
        }
    }
    private fun executeReleaseSoftLocks(action: Action.ReleaseSoftLocks) {
        if (action.uuid != null) services.vaultService.softLockRelease(action.uuid)
    }

    @Suspendable
    private fun executePersistCheckpoint(action: Action.PersistCheckpoint) {
        val checkpoint = action.checkpoint
        val flowState = checkpoint.flowState
        val serializedFlowState = when(flowState) {
            FlowState.Finished -> null
            // upon implementing CORDA-3816: If we have errored or hospitalized then we don't need to serialize the flowState as it will not get saved in the DB
            else -> flowState.checkpointSerialize(checkpointSerializationContext)
        }
        // upon implementing CORDA-3816: If we have errored or hospitalized then we don't need to serialize the serializedCheckpointState as it will not get saved in the DB
        val serializedCheckpointState: SerializedBytes<CheckpointState> = checkpoint.checkpointState.checkpointSerialize(checkpointSerializationContext)
        if (action.isCheckpointUpdate) {
            checkpointStorage.updateCheckpoint(action.id, checkpoint, serializedFlowState, serializedCheckpointState)
        } else {
            checkpointStorage.addCheckpoint(action.id, checkpoint, serializedFlowState, serializedCheckpointState)
        }
    }

    @Suspendable
    private fun executeUpdateFlowStatus(action: Action.UpdateFlowStatus) {
        checkpointStorage.updateStatus(action.id, action.status)
    }
    
    @Suspendable
    private fun executePersistDeduplicationIds(action: Action.PersistDeduplicationFacts) {
        for (handle in action.deduplicationHandlers) {
            handle.insideDatabaseTransaction()
        }
    }

    @Suppress("TooGenericExceptionCaught") // this is fully intentional here, see comment in the catch clause
    @Suspendable
    private fun executeAcknowledgeMessages(action: Action.AcknowledgeMessages) {
        action.deduplicationHandlers.forEach {
            try {
                it.afterDatabaseTransaction()
            } catch (e: Exception) {
                // Catch all exceptions that occur in the [DeduplicationHandler]s (although errors should be unlikely)
                // It is deemed safe for errors to occur here
                // Therefore the current transition should not fail if something does go wrong
                log.info(
                        "An error occurred executing a deduplication post-database commit handler. Continuing, as it is safe to do so.",
                        e
                )
            }
        }
    }

    @Suspendable
    private fun executePropagateErrors(action: Action.PropagateErrors) {
        action.errorMessages.forEach { (exception) ->
            log.warn("Propagating error", exception)
        }
        for (sessionState in action.sessions) {
            // Don't propagate errors to the originating session
            for (errorMessage in action.errorMessages) {
                val sinkSessionId = sessionState.peerSinkSessionId
                val existingMessage = ExistingSessionMessage(sinkSessionId, errorMessage)
                val deduplicationId = DeduplicationId.createForError(errorMessage.errorId, sinkSessionId)
                flowMessaging.sendSessionMessage(sessionState.peerParty, existingMessage, SenderDeduplicationId(deduplicationId, action.senderUUID))
            }
        }
    }

    @Suspendable
    private fun executeScheduleEvent(fiber: FlowFiber, action: Action.ScheduleEvent) {
        fiber.scheduleEvent(action.event)
    }

    private fun executeSleepUntil(fiber: FlowFiber, action: Action.SleepUntil) {
        actionFutureExecutor.sleep(fiber, action)
    }

    @Suspendable
    private fun executeRemoveCheckpoint(action: Action.RemoveCheckpoint) {
        checkpointStorage.removeCheckpoint(action.id, action.mayHavePersistentResults)
    }

    @Suspendable
    private fun executeSendInitial(action: Action.SendInitial) {
        flowMessaging.sendSessionMessage(action.destination, action.initialise, action.deduplicationId)
    }

    @Suspendable
    private fun executeSendExisting(action: Action.SendExisting) {
        flowMessaging.sendSessionMessage(action.peerParty, action.message, action.deduplicationId)
    }

    @Suspendable
    private fun executeSendMultiple(action: Action.SendMultiple) {
        val messages = action.sendInitial.map { Message(it.destination, it.initialise, it.deduplicationId) } +
                action.sendExisting.map { Message(it.peerParty, it.message, it.deduplicationId) }
        flowMessaging.sendSessionMessages(messages)
    }

    @Suspendable
    private fun executeAddSessionBinding(action: Action.AddSessionBinding) {
        stateMachineManager.addSessionBinding(action.flowId, action.sessionId)
    }

    @Suspendable
    private fun executeRemoveSessionBindings(action: Action.RemoveSessionBindings) {
        stateMachineManager.removeSessionBindings(action.sessionIds)
    }

    @Suspendable
    private fun executeSignalFlowHasStarted(action: Action.SignalFlowHasStarted) {
        stateMachineManager.signalFlowHasStarted(action.flowId)
    }

    @Suspendable
    private fun executeRemoveFlow(action: Action.RemoveFlow) {
        stateMachineManager.removeFlow(action.flowId, action.removalReason, action.lastState)
    }

    @Suspendable
    private fun executeMoveFlowToPaused(action: Action.MoveFlowToPaused) {
        stateMachineManager.moveFlowToPaused(action.currentState)
    }

    @Suspendable
    @Throws(SQLException::class)
    private fun executeCreateTransaction() {
        if (contextTransactionOrNull != null) {
            throw IllegalStateException("Refusing to create a second transaction")
        }
        contextDatabase.newTransaction()
    }

    @Suspendable
    private fun executeRollbackTransaction() {
        contextTransactionOrNull?.run {
            rollback()
            close()
        }
    }

    @Suspendable
    @Throws(SQLException::class)
    private fun executeCommitTransaction(action: Action.CommitTransaction) {
        try {
            contextTransaction.commit()
        } finally {
            contextTransaction.close()
            contextTransactionOrNull = null
        }
        action.currentState.run { numberOfCommits = checkpoint.checkpointState.numberOfCommits }
    }

    @Suppress("TooGenericExceptionCaught")
    @Suspendable
    private fun executeAsyncOperation(fiber: FlowFiber, action: Action.ExecuteAsyncOperation) {
        try {
            actionFutureExecutor.awaitAsyncOperation(fiber, action)
        } catch (e: Exception) {
            // Catch and wrap any unexpected exceptions from the async operation
            // Wrapping the exception allows it to be better handled by the flow hospital
            throw AsyncOperationTransitionException(e)
        }
    }

    private fun executeRetryFlowFromSafePoint(action: Action.RetryFlowFromSafePoint) {
        stateMachineManager.retryFlowFromSafePoint(action.currentState)
    }

    private fun cancelFlowTimeout(action: Action.CancelFlowTimeout) {
        stateMachineManager.cancelFlowTimeout(action.flowId)
    }

    private fun scheduleFlowTimeout(action: Action.ScheduleFlowTimeout) {
        stateMachineManager.scheduleFlowTimeout(action.flowId)
    }

    private fun executeRemoveFlowException(action: Action.RemoveFlowException) {
        checkpointStorage.removeFlowException(action.id)
    }

    private fun executeAddFlowException(action: Action.AddFlowException) {
        checkpointStorage.addFlowException(action.id, action.exception)
    }
}
