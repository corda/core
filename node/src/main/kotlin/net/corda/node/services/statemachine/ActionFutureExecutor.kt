package net.corda.node.services.statemachine

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.internal.concurrent.thenMatch
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.debug
import net.corda.node.services.api.ServiceHubInternal
import java.time.Duration
import java.util.concurrent.CancellationException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

internal class ActionFutureExecutor(
    private val innerState: StateMachineInnerState,
    private val services: ServiceHubInternal,
    private val scheduledExecutor: ScheduledExecutorService
) {

    private companion object {
        val log = contextLogger()
    }

    /**
     * Put a flow to sleep for the duration specified in [action].
     *
     * @param fiber The [FlowFiber] that will be woken up after sleeping
     * @param action The [Action.SleepUntil] to create a future from
     */
    fun sleep(fiber: FlowFiber, action: Action.SleepUntil) {
        cancelFutureIfRunning(fiber, action.currentState)
        val instance = fiber.instanceId
        val duration = Duration.between(services.clock.instant(), action.time)
        log.debug { "Putting flow ${instance.runId} to sleep for $duration" }
        val future = scheduledExecutor.schedule<Unit>(
            {
                log.debug { "Scheduling flow wake up event for flow ${instance.runId}" }
                scheduleWakeUpEvent(instance, Event.WakeUpFromSleep)
            },
            duration.toMillis(), TimeUnit.MILLISECONDS
        )
        action.currentState.future = future
    }

    /**
     * Suspend a flow until its async operation specified in [action] is completed.
     *
     * @param fiber The [FlowFiber] to resume after completing the async operation
     * @param action The [Action.ExecuteAsyncOperation] to create a future from
     */
    @Suspendable
    fun awaitAsyncOperation(fiber: FlowFiber, action: Action.ExecuteAsyncOperation) {
        cancelFutureIfRunning(fiber, action.currentState)
        val instance = fiber.instanceId
        log.debug { "Suspending flow ${instance.runId} until its async operation has completed" }
        val future = action.operation.execute(action.deduplicationId)
        future.thenMatch(
            success = { result -> scheduleWakeUpEvent(instance, Event.AsyncOperationCompletion(result)) },
            failure = { exception ->
                // If canceling the future happens from within the state machine (i.e. on a fiber), then there is no reason signaling the fiber
                // with a [Event.AsyncOperationThrows] as we are already aware of when we cancel it. In addition it gets the statemachine
                // into an erroneous state as it will throw an extra exception (CancellationException).
                if (exception !is CancellationException || FlowStateMachineImpl.currentStateMachine() == null) {
                    scheduleWakeUpEvent(instance, Event.AsyncOperationThrows(exception))
                }
            }
        )
        action.currentState.future = future
    }

    private fun cancelFutureIfRunning(fiber: FlowFiber, currentState: StateMachineState) {
        // No other future should be running, cancel it if there is
        currentState.future?.run {
            log.debug { "Cancelling existing future for flow ${fiber.id}" }
            if (!isDone) cancel(true)
        }
    }

    private fun scheduleWakeUpEvent(instance: StateMachineInstanceId, event: Event) {
        innerState.withLock {
            flows[instance.runId]?.let { flow ->
                // Only schedule a wake up event if the fiber the flow is executing on has not changed
                if (flow.fiber.instanceId == instance) {
                    flow.fiber.scheduleEvent(event)
                }
            }
        }
    }
}