package net.corda.node.services.statemachine

import net.corda.core.concurrent.CordaFuture
import net.corda.core.context.InvocationContext
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.StateMachineRunId
import net.corda.core.internal.FlowStateMachine
import net.corda.core.internal.FlowStateMachineHandle
import net.corda.core.messaging.DataFeed
import net.corda.core.messaging.FlowHandleWithClientId
import net.corda.core.messaging.RPCOps
import net.corda.core.utilities.Try
import net.corda.node.services.messaging.DeduplicationHandler
import net.corda.node.services.messaging.ReceivedMessage
import rx.Observable

/**
 * A StateMachineManager is responsible for coordination and persistence of multiple [FlowStateMachine] objects.
 * Each such object represents an instantiation of a (two-party) flow that has reached a particular point.
 *
 * An implementation of this interface will persist state machines to long term storage so they can survive process
 * restarts and, if run with a single-threaded executor, will ensure no two state machines run concurrently with each
 * other (bad for performance, good for programmer mental health!).
 *
 * A flow is a class with a single call method. The call method and any others it invokes are rewritten by a bytecode
 * rewriting engine called Quasar, to ensure the code can be suspended and resumed at any point.
 *
 * TODO: Consider the issue of continuation identity more deeply: is it a safe assumption that a serialised continuation is always unique?
 * TODO: Think about how to bring the system to a clean stop so it can be upgraded without any serialised stacks on disk
 * TODO: Timeouts
 * TODO: Surfacing of exceptions via an API and/or management UI
 * TODO: Don't store all active flows in memory, load from the database on demand.
 */
interface StateMachineManager : LockingRpc {

    enum class StartMode {
        ExcludingPaused, // Resume all flows except paused flows.
        Safe // Mark all flows as paused.
    }

    /**
     * Starts the state machine manager, loading and starting the state machines in storage.
     *
     * @return `Future` which completes when SMM is fully started
     */
    fun start(tokenizableServices: List<Any>, startMode: StartMode = StartMode.ExcludingPaused) : CordaFuture<Unit>

    /**
     * Stops the state machine manager gracefully, waiting until all but [allowedUnsuspendedFiberCount] flows reach the
     * next checkpoint.
     */
    fun stop(allowedUnsuspendedFiberCount: Int)

    /**
     * Represents an addition/removal of a state machine.
     */
    sealed class Change {
        abstract val logic: FlowLogic<*>

        data class Add(override val logic: FlowLogic<*>) : Change()
        data class Removed(override val logic: FlowLogic<*>, val result: Try<*>) : Change()
    }

    /**
     * Returns the list of live state machines and a stream of subsequent additions/removals of them.
     */
    fun track(): DataFeed<List<FlowLogic<*>>, Change>

    /**
     * The stream of additions/removals of flows.
     */
    val changes: Observable<Change>

    /**
     * Returns the currently live flows of type [flowClass], and their corresponding result future.
     */
    fun <A : FlowLogic<*>> findStateMachines(flowClass: Class<A>): List<Pair<A, CordaFuture<*>>>

    /**
     * Returns all currently live flows.
     */
    val allStateMachines: List<FlowLogic<*>>

    /**
     * Attempts to kill a flow. This is not a clean termination and should be reserved for exceptional cases such as stuck fibers.
     *
     * @return whether the flow existed and was killed.
     */
    fun killFlow(id: StateMachineRunId): Boolean

    /**
     * Deliver an external event to the state machine.  Such an event might be a new P2P message, or a request to start a flow.
     * The event may be replayed if a flow fails and attempts to retry.
     */
    fun deliverExternalEvent(event: ExternalEvent)

    val flowHospital: StaffedFlowHospital

    /**
     * Returns a snapshot of all [FlowStateMachineImpl]s currently managed.
     */
    fun snapshot(): Set<FlowStateMachineImpl<*>>

    /**
     * Reattach to an existing flow that was started with [startFlowDynamicWithClientId] and has a [clientId].
     *
     * If there is a flow matching the [clientId] then its result or exception is returned.
     *
     * When there is no flow matching the [clientId] then [null] is returned directly (not a future/[FlowHandleWithClientId]).
     *
     * Calling [reattachFlowWithClientId] after [removeClientId] with the same [clientId] will cause the function to return [null] as
     * the result/exception of the flow will no longer be available.
     *
     * @param clientId The client id relating to an existing flow
     */
    fun <T> reattachFlowWithClientId(clientId: String): FlowStateMachineHandle<T>?

    /**
     * Removes a flow's [clientId] to result/ exception mapping.
     *
     * @return whether the mapping was removed.
     */
    fun removeClientId(clientId: String): Boolean
}

interface LockingRpc : RPCOps {
    fun await(id: StateMachineRunId, status: Checkpoint.FlowStatus): CordaFuture<Checkpoint.FlowStatus>
    fun release(id: StateMachineRunId): Boolean
}

// These must be idempotent! A later failure in the state transition may error the flow state, and a replay may call
// these functions again
internal interface StateMachineManagerInternal {
    fun signalFlowHasStarted(flowId: StateMachineRunId)
    fun addSessionBinding(flowId: StateMachineRunId, sessionId: SessionId)
    fun removeSessionBindings(sessionIds: Set<SessionId>)
    fun removeFlow(flowId: StateMachineRunId, removalReason: FlowRemovalReason, lastState: StateMachineState)
    fun moveFlowToPaused(currentState: StateMachineState)
    fun retryFlowFromSafePoint(currentState: StateMachineState)
    fun scheduleFlowTimeout(flowId: StateMachineRunId)
    fun cancelFlowTimeout(flowId: StateMachineRunId)
}

/**
 * Represents an external event that can be injected into the state machine and that might need to be replayed if
 * a flow retries.  They always have de-duplication handlers to assist with the at-most once logic where required.
 */
interface ExternalEvent {
    val deduplicationHandler: DeduplicationHandler

    /**
     * An external P2P message event.
     */
    interface ExternalMessageEvent : ExternalEvent {
        val flowId: StateMachineRunId
        val receivedMessage: ReceivedMessage
    }

    /**
     * An external request to start a flow, from the scheduler for example.
     */
    interface ExternalStartFlowEvent<T> : ExternalEvent {
        val flowId: StateMachineRunId
        val flowLogic: FlowLogic<T>
        val context: InvocationContext

        /**
         * A callback for the state machine to pass back the [CordaFuture] associated with the flow start to the submitter.
         */
        fun wireUpFuture(flowFuture: CordaFuture<out FlowStateMachineHandle<T>>)

        /**
         * The future representing the flow start, passed back from the state machine to the submitter of this event.
         */
        val future: CordaFuture<out FlowStateMachineHandle<T>>
    }
}
