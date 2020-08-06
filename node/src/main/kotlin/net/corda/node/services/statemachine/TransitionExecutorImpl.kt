package net.corda.node.services.statemachine

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.utilities.contextLogger
import net.corda.node.services.statemachine.transitions.FlowContinuation
import net.corda.node.services.statemachine.transitions.TransitionResult
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.nodeapi.internal.persistence.DatabaseTransactionException
import net.corda.nodeapi.internal.persistence.contextDatabase
import net.corda.nodeapi.internal.persistence.contextTransactionOrNull
import java.security.SecureRandom
import java.sql.SQLException
import javax.persistence.OptimisticLockException

/**
 * This [TransitionExecutor] runs the transition actions using the passed in [ActionExecutor] and manually dirties the
 * state on failure.
 *
 * If a failure happens when we're already transitioning into a errored state then the transition and the flow fiber is
 * completely aborted to avoid error loops.
 */
class TransitionExecutorImpl(
    val secureRandom: SecureRandom,
    val database: CordaPersistence
) : TransitionExecutor {

    private companion object {
        val log = contextLogger()
    }

    @Suppress("NestedBlockDepth", "ReturnCount")
    @Suspendable
    override fun executeTransition(
        fiber: FlowFiber,
        previousState: StateMachineState,
        event: Event,
        transition: TransitionResult,
        actionExecutor: ActionExecutor
    ): Pair<FlowContinuation, StateMachineState> {
        contextDatabase = database
        for (action in transition.actions) {
            try {
                actionExecutor.executeAction(fiber, action)
            } catch (exception: Exception) {
                rollbackTransactionOnError()
                if (transition.newState.checkpoint.errorState is ErrorState.Errored) {
                    log.warn("Error while executing $action, with error event $event, updating errored state", exception)

                    val newState = previousState.copy(
                        checkpoint = previousState.checkpoint.copy(
                            errorState = previousState.checkpoint.errorState.addErrors(
                                listOf(
                                    FlowError(
                                        secureRandom.nextLong(),
                                        ErrorStateTransitionException(exception, event)
                                    )
                                )
                            )
                        ),
                        isFlowResumed = false
                    )

                    return Pair(FlowContinuation.ProcessEvents, newState)
                } else {
                    // Otherwise error the state manually keeping the old flow state and schedule a DoRemainingWork
                    // to trigger error propagation
                    if (log.isDebugEnabled && previousState.isRemoved && exception is OptimisticLockException) {
                        log.debug(
                            "Flow has been killed and the following error is likely due to the flow's checkpoint being deleted. " +
                                    "Occurred while executing $action, with event $event", exception
                        )
                    } else {
                        log.info("Error while executing $action, with event $event, erroring state", exception)
                    }

                    // distinguish between a DatabaseTransactionException and an actual StateTransitionException
                    val stateTransitionOrDatabaseTransactionException =
                        if (exception is DatabaseTransactionException) {
                            // if the exception is a DatabaseTransactionException then it is not really a StateTransitionException
                            // it is actually an exception that previously broke a DatabaseTransaction and was suppressed by user code
                            // it was rethrown on [DatabaseTransaction.commit]. Unwrap the original exception and pass it to flow hospital
                            exception.cause
                        } else {
                            // Wrap the exception with [StateTransitionException] for handling by the flow hospital
                            StateTransitionException(action, event, exception)
                        }

                    val newState = previousState.copy(
                        checkpoint = previousState.checkpoint.copy(
                            errorState = previousState.checkpoint.errorState.addErrors(
                                listOf(FlowError(secureRandom.nextLong(), stateTransitionOrDatabaseTransactionException))
                            )
                        ),
                        isFlowResumed = false
                    )
                    fiber.scheduleEvent(Event.DoRemainingWork)
                    return Pair(FlowContinuation.ProcessEvents, newState)
                }
            }
        }
        return Pair(transition.continuation, transition.newState)
    }

    private fun rollbackTransactionOnError() {
        contextTransactionOrNull?.run {
            try {
                rollback()
            } catch (rollbackException: SQLException) {
                log.info(
                    "Error rolling back database transaction from a previous error, continuing error handling for the original error",
                    rollbackException
                )
            }
            try {
                close()
            } catch (rollbackException: SQLException) {
                log.info(
                    "Error closing database transaction from a previous error, continuing error handling for the original error",
                    rollbackException
                )
            }
        }
    }
}