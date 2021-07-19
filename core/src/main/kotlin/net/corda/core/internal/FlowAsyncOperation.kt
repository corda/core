package net.corda.core.internal

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.concurrent.CordaFuture
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.WrappedFlowExternalAsyncOperation
import net.corda.core.flows.WrappedFlowExternalOperation
import net.corda.core.serialization.CordaSerializable

/**
 * Interface for arbitrary operations that can be invoked in a flow asynchronously - the flow will suspend until the
 * operation completes. Operation parameters are expected to be injected via constructor.
 */
@CordaSerializable
interface FlowAsyncOperation<R : Any> {

    val collectErrorsFromSessions: Boolean
        get() = false

    /**
     * Performs the operation in a non-blocking fashion.
     * @param deduplicationId  If the flow restarts from a checkpoint (due to node restart, or via a visit to the flow
     * hospital following an error) the execute method might be called more than once by the Corda flow state machine.
     * For each duplicate call, the deduplicationId is guaranteed to be the same allowing duplicate requests to be
     * de-duplicated if necessary inside the execute method.
     */
    fun execute(deduplicationId: String): CordaFuture<R>
}

/** Executes the specified [operation] and suspends until operation completion. */
@Deprecated(
    "This has been replaced by [FlowLogic.await] that provides an improved and public API",
    ReplaceWith("net.corda.core.flows.FlowLogic.await")
)
@Suspendable
fun <T, R : Any> FlowLogic<T>.executeAsync(operation: FlowAsyncOperation<R>, maySkipCheckpoint: Boolean = false): R {
    val request = FlowIORequest.ExecuteAsyncOperation(operation)
    return stateMachine.suspend(request, maySkipCheckpoint)
}

/**
 * Unwraps an external operation implementation that can be wrapped by [WrappedFlowExternalAsyncOperation] or
 * [WrappedFlowExternalOperation].
 */
fun FlowAsyncOperation<*>.unwrap(): Any =
    when (this) {
        is WrappedFlowExternalAsyncOperation<*> -> operation
        is WrappedFlowExternalOperation<*> -> operation
        else -> this
}