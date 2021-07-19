package net.corda.node.services.statemachine

import net.corda.core.flows.StateMachineRunId
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.FlowIORequest
import net.corda.core.internal.FlowStateMachine
import net.corda.core.internal.unwrap
import java.time.Clock
import java.time.Duration
import java.time.Instant

/**
 * Defines criteria to get waiting flows
 */
data class WaitingFlowQuery(
        val flowIds: MutableList<StateMachineRunId> = mutableListOf(),
        val onlyIfSuspendedLongerThan: Duration = Duration.ZERO,
        val waitingSources: MutableList<WaitingSource> = mutableListOf(),
        val counterParties: MutableList<CordaX500Name> = mutableListOf()
) {
    fun isDefined() = flowIds.isNotEmpty()
            || waitingSources.isNotEmpty()
            || counterParties.isNotEmpty()
            || onlyIfSuspendedLongerThan > Duration.ZERO
}

/**
 * Information about a flow which is waiting to be resumed
 * The flow is considered to be waiting if:
 *  - It's started.
 *  - Is not admitted to the hospital
 *  - And it's in the suspended state for an IO request.
 */
data class WaitingFlowInfo(
        val id: StateMachineRunId,
        val suspendedTimestamp: Instant,
        val source: WaitingSource,
        val waitingForParties: List<Party>,
        val externalOperationImplName: String? = null
)

/**
 * Stage in which the flow is suspended
 */
enum class WaitingSource {
    SEND,
    RECEIVE,
    SEND_AND_RECEIVE,
    CLOSE_SESSIONS,
    GET_FLOW_INFO,
    SLEEP,
    WAIT_FOR_SESSIONS_CONFIRMATIONS,
    EXTERNAL_OPERATION
}

/**
 * Read operations that extract information about the flows running in the node.
 */
interface FlowReadOperations {

    /**
     * Returns waiting flows for a specified query.
     */
    fun queryWaitingFlows(query: WaitingFlowQuery): Set<WaitingFlowInfo>

    /**
     * Returns waiting flows for a specified query grouped by the party.
     */
    fun queryFlowsCurrentlyWaitingForPartiesGrouped(query: WaitingFlowQuery): Map<Party, List<WaitingFlowInfo>>

    /**
     * Returns all waiting flow state machines.
     */
    fun getAllWaitingFlows(): Sequence<FlowStateMachineImpl<*>>
}

/**
 * Write operations that interact with the flows running in the node.
 */
interface FlowWriteOperations

/**
 * Implements flow operators
 * @see FlowReadOperations
 * @see FlowWriteOperations
 */
class FlowOperator(private val smm: StateMachineManager, private val clock: Clock) : FlowReadOperations, FlowWriteOperations {

    override fun queryWaitingFlows(query: WaitingFlowQuery): Set<WaitingFlowInfo> {
        var sequence = getAllWaitingFlows()
        if (query.flowIds.isNotEmpty()) {
            sequence = sequence.filter { it.id in query.flowIds }
        }
        if (query.counterParties.isNotEmpty()) {
            sequence = sequence.filter { it.isWaitingForParties(query.counterParties) }
        }
        if (query.waitingSources.isNotEmpty()) {
            sequence = sequence.filter { it.waitingSource() in query.waitingSources }
        }
        if (query.onlyIfSuspendedLongerThan > Duration.ZERO) {
            val now = clock.instant()
            sequence = sequence.filter { flow -> flow.ongoingDuration(now) >= query.onlyIfSuspendedLongerThan }
        }
        val result = LinkedHashSet<WaitingFlowInfo>()
        sequence.forEach { flow ->
            val waitingParties = flow.waitingFlowInfo()
            if (waitingParties != null) {
                result.add(waitingParties)
            }
        }
        return result
    }

    override fun queryFlowsCurrentlyWaitingForPartiesGrouped(query: WaitingFlowQuery): Map<Party, List<WaitingFlowInfo>> {
        return queryWaitingFlows(query)
                .flatMap { info -> info.waitingForParties.map { it to info } }
                .groupBy({ it.first }) { it.second }
    }

    override fun getAllWaitingFlows(): Sequence<FlowStateMachineImpl<*>> {
        return smm.snapshot()
                .asSequence()
                .filter { flow -> flow !in smm.flowHospital && flow.isStarted() && flow.isSuspended() }
    }
}

private fun FlowStateMachineImpl<*>.isWaitingForParties(parties: List<CordaX500Name>): Boolean {
    return ioRequest()?.let { request ->
        when (request) {
            is FlowIORequest.GetFlowInfo -> request.sessions.any { it.counterparty.name in parties }
            is FlowIORequest.Receive -> request.sessions.any { it.counterparty.name in parties }
            is FlowIORequest.Send -> request.sessionToMessage.keys.any { it.counterparty.name in parties }
            is FlowIORequest.SendAndReceive -> request.sessionToMessage.keys.any { it.counterparty.name in parties }
            else -> false
        }
    } ?: false
}

@Suppress("ComplexMethod")
private fun FlowStateMachineImpl<*>.waitingSource(): WaitingSource? {
    return ioRequest()?.let { request ->
        when (request) {
            is FlowIORequest.Send -> WaitingSource.SEND
            is FlowIORequest.Receive -> WaitingSource.RECEIVE
            is FlowIORequest.SendAndReceive -> WaitingSource.SEND_AND_RECEIVE
            is FlowIORequest.CloseSessions -> WaitingSource.CLOSE_SESSIONS
            is FlowIORequest.GetFlowInfo -> WaitingSource.GET_FLOW_INFO
            is FlowIORequest.Sleep -> WaitingSource.SLEEP
            is FlowIORequest.WaitForSessionConfirmations -> WaitingSource.WAIT_FOR_SESSIONS_CONFIRMATIONS
            is FlowIORequest.ExecuteAsyncOperation -> WaitingSource.EXTERNAL_OPERATION
            else -> null
        }
    }
}

@Suppress("ComplexMethod")
private fun FlowStateMachineImpl<*>.waitingFlowInfo(): WaitingFlowInfo? {
    return ioRequest()?.let { request ->
        when (request) {
            is FlowIORequest.Send -> flowInfoOf(
                    WaitingSource.SEND,
                    request.sessionToMessage.map { it.key.counterparty }
            )
            is FlowIORequest.Receive -> flowInfoOf(
                    WaitingSource.RECEIVE,
                    request.sessions.map { it.counterparty }
            )
            is FlowIORequest.SendAndReceive -> flowInfoOf(
                    WaitingSource.SEND_AND_RECEIVE,
                    request.sessionToMessage.map { it.key.counterparty }
            )
            is FlowIORequest.CloseSessions -> flowInfoOf(
                    WaitingSource.CLOSE_SESSIONS,
                    request.sessions.map { it.counterparty }
            )
            is FlowIORequest.GetFlowInfo -> flowInfoOf(
                    WaitingSource.GET_FLOW_INFO,
                    request.sessions.map { it.counterparty }
            )
            is FlowIORequest.Sleep -> flowInfoOf(
                    WaitingSource.SLEEP,
                    listOf()
            )
            is FlowIORequest.WaitForSessionConfirmations -> flowInfoOf(
                    WaitingSource.WAIT_FOR_SESSIONS_CONFIRMATIONS,
                    listOf()
            )
            is FlowIORequest.ExecuteAsyncOperation -> flowInfoOf(
                    WaitingSource.EXTERNAL_OPERATION,
                    listOf(),
                    request.operation.unwrap().javaClass.canonicalName
            )
            else -> null
        }
    }
}

private operator fun StaffedFlowHospital.contains(flow: FlowStateMachine<*>) = contains(flow.id)

private fun FlowStateMachineImpl<*>.flowInfoOf(
        source: WaitingSource,
        waitingForParties: List<Party>,
        externalOperationName: String? = null
): WaitingFlowInfo =
        WaitingFlowInfo(
                id,
                suspendedTimestamp(),
                source,
                waitingForParties,
                externalOperationName)
