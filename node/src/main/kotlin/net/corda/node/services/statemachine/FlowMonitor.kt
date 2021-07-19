package net.corda.node.services.statemachine

import net.corda.core.flows.FlowSession
import net.corda.core.internal.FlowIORequest
import net.corda.core.internal.FlowStateMachine
import net.corda.core.internal.VisibleForTesting
import net.corda.core.internal.unwrap
import net.corda.core.utilities.loggerFor
import net.corda.node.internal.LifecycleSupport
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

internal class FlowMonitor(
    private val flowOperator: FlowOperator,
    private val monitoringPeriod: Duration,
    private val suspensionLoggingThreshold: Duration,
    private var scheduler: ScheduledExecutorService? = null
) : LifecycleSupport {

    private companion object {
        private fun defaultScheduler(): ScheduledExecutorService {
            return Executors.newSingleThreadScheduledExecutor()
        }

        private val logger = loggerFor<FlowMonitor>()
    }

    override var started = false

    private var shutdownScheduler = false

    override fun start() {
        synchronized(this) {
            if (scheduler == null) {
                scheduler = defaultScheduler()
                shutdownScheduler = true
            }
            scheduler!!.scheduleAtFixedRate({ logFlowsWaitingForParty() }, 0, monitoringPeriod.toMillis(), TimeUnit.MILLISECONDS)
            started = true
        }
    }

    override fun stop() {
        synchronized(this) {
            if (shutdownScheduler) {
                scheduler!!.shutdown()
            }
            started = false
        }
    }

    private fun logFlowsWaitingForParty() {
        for ((flow, suspensionDuration) in waitingFlowDurations(suspensionLoggingThreshold)) {
            flow.ioRequest()?.let { request -> logger.info(warningMessageForFlowWaitingOnIo(request, flow, suspensionDuration)) }
        }
    }

    @VisibleForTesting
    fun waitingFlowDurations(suspensionLoggingThreshold: Duration): Sequence<Pair<FlowStateMachineImpl<*>, Duration>> {
        val now = Instant.now()
        return flowOperator.getAllWaitingFlows()
                .map { flow -> flow to flow.ongoingDuration(now) }
                .filter { (_, suspensionDuration) -> suspensionDuration >= suspensionLoggingThreshold }
    }

    private fun warningMessageForFlowWaitingOnIo(request: FlowIORequest<*>,
                                                 flow: FlowStateMachineImpl<*>,
                                                 suspensionDuration: Duration): String {
        val message = StringBuilder("Flow with id ${flow.id.uuid} has been waiting for ${suspensionDuration.toMillis() / 1000} seconds ")
        message.append(
            when (request) {
                is FlowIORequest.Send -> "to send a message to parties ${request.sessionToMessage.keys.partiesInvolved()}"
                is FlowIORequest.Receive -> "to receive messages from parties ${request.sessions.partiesInvolved()}"
                is FlowIORequest.SendAndReceive -> "to send and receive messages from parties ${request.sessionToMessage.keys.partiesInvolved()}"
                is FlowIORequest.CloseSessions -> "to close sessions: ${request.sessions}"
                is FlowIORequest.GetFlowInfo -> "to get flow information from parties ${request.sessions.partiesInvolved()}"
                is FlowIORequest.Sleep -> "to wake up from sleep ending at ${LocalDateTime.ofInstant(request.wakeUpAfter, ZoneId.systemDefault())}"
                is FlowIORequest.WaitForSessionConfirmations -> "for sessions to be confirmed"
                is FlowIORequest.ExecuteAsyncOperation -> "for asynchronous operation of type (${request.operation.unwrap()}) to complete"
                FlowIORequest.ForceCheckpoint -> "for forcing a checkpoint at an arbitrary point in a flow"
            }
        )
        message.append(".")
        return message.toString()
    }

    private fun Iterable<FlowSession>.partiesInvolved() = map { it.counterparty }.joinToString(", ", "[", "]")

    private operator fun StaffedFlowHospital.contains(flow: FlowStateMachine<*>) = contains(flow.id)
}
