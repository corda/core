package net.corda.node.services.statemachine.hospital.external

import net.corda.core.flows.FlowException
import net.corda.core.flows.ReceiveFinalityFlow
import net.corda.core.flows.ReceiveTransactionFlow
import net.corda.core.flows.UnexpectedFlowEndException
import net.corda.core.identity.Party
import net.corda.core.internal.DeclaredField
import net.corda.node.services.FinalityHandler
import net.corda.node.services.statemachine.Diagnosis
import net.corda.node.services.statemachine.FlowFiber
import net.corda.node.services.statemachine.FlowMedicalHistory
import net.corda.node.services.statemachine.Staff
import net.corda.node.services.statemachine.StateMachineState
import org.slf4j.LoggerFactory

// [FinalityDoctor] should be moved in a module along with [FinalityFlow] (corda transactions module?).

object FinalityDoctor : Staff {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun consult(flowFiber: FlowFiber, currentState: StateMachineState, newError: Throwable, history: FlowMedicalHistory): Diagnosis {
        return if (currentState.flowLogic is FinalityHandler) {
            log.warn("Flow ${flowFiber.id} failed to be finalised. Manual intervention may be required before retrying " +
                    "the flow by re-starting the node. State machine state: $currentState", newError)
            Diagnosis.OVERNIGHT_OBSERVATION
        } else if (isFromReceiveFinalityFlow(newError)) {
            if (isErrorPropagatedFromCounterparty(newError) && isErrorThrownDuringReceiveFinality(newError)) {
                // no need to keep around the flow, since notarisation has already failed at the counterparty.
                Diagnosis.NOT_MY_SPECIALTY
            } else {
                log.warn("Flow ${flowFiber.id} failed to be finalised. Manual intervention may be required before retrying " +
                        "the flow by re-starting the node. State machine state: $currentState", newError)
                Diagnosis.OVERNIGHT_OBSERVATION
            }
        } else {
            Diagnosis.NOT_MY_SPECIALTY
        }
    }

    private fun isFromReceiveFinalityFlow(throwable: Throwable): Boolean {
        return throwable.stackTrace.any { it.className == ReceiveFinalityFlow::class.java.name }
    }

    private fun isErrorPropagatedFromCounterparty(error: Throwable): Boolean {
        return when (error) {
            is UnexpectedFlowEndException -> {
                val peer = DeclaredField<Party?>(UnexpectedFlowEndException::class.java, "peer", error).value
                peer != null
            }
            is FlowException -> {
                val peer = DeclaredField<Party?>(FlowException::class.java, "peer", error).value
                peer != null
            }
            else -> false
        }
    }

    /**
     * This method will return true if [ReceiveTransactionFlow] is at the top of the stack during the error.
     * As a result, if the failure happened during a sub-flow invoked from [ReceiveTransactionFlow], the method will return false.
     *
     * This is because in the latter case, the transaction might have already been finalised and deleting the flow
     * would introduce risk for inconsistency between nodes.
     */
    private fun isErrorThrownDuringReceiveFinality(error: Throwable): Boolean {
        val strippedStacktrace = error.stackTrace
                .filterNot { it?.className?.contains("counter-flow exception from peer") ?: false }
                .filterNot { it?.className?.startsWith("net.corda.node.services.statemachine.") ?: false }
        return strippedStacktrace.isNotEmpty()
                && strippedStacktrace.first().className.startsWith(ReceiveTransactionFlow::class.qualifiedName!!)
    }
}