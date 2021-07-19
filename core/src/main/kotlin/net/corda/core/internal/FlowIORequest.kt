package net.corda.core.internal

import net.corda.core.DeleteForDJVM
import net.corda.core.flows.FlowInfo
import net.corda.core.flows.FlowSession
import net.corda.core.serialization.SerializedBytes
import net.corda.core.utilities.NonEmptySet
import java.time.Instant

// DOCSTART FlowIORequest
/**
 * A [FlowIORequest] represents an IO request of a flow when it suspends. It is persisted in checkpoints.
 */
@DeleteForDJVM
sealed class FlowIORequest<out R : Any> {
    /**
     * Send messages to sessions.
     *
     * @property sessionToMessage a map from session to message-to-be-sent.
     * @property shouldRetrySend specifies whether the send should be retried.
     */
    data class Send(
            val sessionToMessage: Map<FlowSession, SerializedBytes<Any>>
    ) : FlowIORequest<Unit>() {
        override fun toString() = "Send(sessionToMessage=${sessionToMessage.mapValues { it.value.hash }})"
    }

    /**
     * Receive messages from sessions.
     *
     * @property sessions the sessions to receive messages from.
     * @return a map from session to received message.
     */
    data class Receive(
            val sessions: NonEmptySet<FlowSession>
    ) : FlowIORequest<Map<FlowSession, SerializedBytes<Any>>>()

    /**
     * Send and receive messages from the specified sessions.
     *
     * @property sessionToMessage a map from session to message-to-be-sent. The keys also specify which sessions to
     *     receive from.
     * @property shouldRetrySend specifies whether the send should be retried.
     * @return a map from session to received message.
     */
    //net.corda.core.internal.FlowIORequest.SendAndReceive
    data class SendAndReceive(
            val sessionToMessage: Map<FlowSession, SerializedBytes<Any>>,
            val shouldRetrySend: Boolean
    ) : FlowIORequest<Map<FlowSession, SerializedBytes<Any>>>() {
        override fun toString() = "SendAndReceive(${sessionToMessage.mapValues { (key, value) ->
            "$key=${value.hash}"
        }}, shouldRetrySend=$shouldRetrySend)"
    }

    /**
     * Closes the specified sessions.
     *
     * @property sessions the sessions to be closed.
     */
    data class CloseSessions(val sessions: NonEmptySet<FlowSession>): FlowIORequest<Unit>()

    /**
     * Get the FlowInfo of the specified sessions.
     *
     * @property sessions the sessions to get the FlowInfo of.
     * @return a map from session to FlowInfo.
     */
    data class GetFlowInfo(val sessions: NonEmptySet<FlowSession>) : FlowIORequest<Map<FlowSession, FlowInfo>>()

    /**
     * Suspend the flow until the specified time.
     *
     * @property wakeUpAfter the time to sleep until.
     */
    data class Sleep(val wakeUpAfter: Instant) : FlowIORequest<Unit>()

    /**
     * Suspend the flow until all Initiating sessions are confirmed.
     */
    class WaitForSessionConfirmations : FlowIORequest<Unit>() {
        override fun equals(other: Any?): Boolean {
            return this === other
        }

        override fun hashCode(): Int {
            return System.identityHashCode(this)
        }
    }

    /**
     * Execute the specified [operation], suspend the flow until completion.
     */
    data class ExecuteAsyncOperation<T : Any>(val operation: FlowAsyncOperation<T>) : FlowIORequest<T>()

    /**
     * Indicates that no actual IO request occurred, and the flow should be resumed immediately.
     * This is used for performing explicit checkpointing anywhere in a flow.
     */
    // TODO: consider using an empty FlowAsyncOperation instead
    object ForceCheckpoint : FlowIORequest<Unit>()
}
// DOCSEND FlowIORequest
