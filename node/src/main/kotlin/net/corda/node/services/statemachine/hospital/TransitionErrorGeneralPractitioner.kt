package net.corda.node.services.statemachine.hospital

import net.corda.core.utilities.debug
import net.corda.node.services.statemachine.AsyncOperationTransitionException
import net.corda.node.services.statemachine.Diagnosis
import net.corda.node.services.statemachine.FlowFiber
import net.corda.node.services.statemachine.FlowMedicalHistory
import net.corda.node.services.statemachine.ReloadFlowFromCheckpointException
import net.corda.node.services.statemachine.Staff
import net.corda.node.services.statemachine.StateMachineState
import net.corda.node.services.statemachine.StateTransitionException
import net.corda.node.services.statemachine.mentionsThrowable
import org.slf4j.LoggerFactory

/**
 * Handles exceptions from internal state transitions that are not dealt with by the rest of the staff.
 *
 * [InterruptedException]s are diagnosed as [Diagnosis.TERMINAL] so they are never retried
 * (can occur when a flow is killed - `killFlow`).
 * [AsyncOperationTransitionException]s ares ignored as the error is likely to have originated in user async code rather than inside
 * of a transition.
 * All other exceptions are retried a maximum of 3 times before being kept in for observation.
 */
object TransitionErrorGeneralPractitioner : Staff {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun consult(
            flowFiber: FlowFiber,
            currentState: StateMachineState,
            newError: Throwable,
            history: FlowMedicalHistory
    ): Diagnosis {
        return if (newError.mentionsThrowable(StateTransitionException::class.java)) {
            when {
                newError.mentionsThrowable(InterruptedException::class.java) -> Diagnosis.TERMINAL
                newError.mentionsThrowable(ReloadFlowFromCheckpointException::class.java) -> Diagnosis.OVERNIGHT_OBSERVATION
                newError.mentionsThrowable(AsyncOperationTransitionException::class.java) -> Diagnosis.NOT_MY_SPECIALTY
                history.notDischargedForTheSameThingMoreThan(2, this, currentState) -> Diagnosis.DISCHARGE
                else -> Diagnosis.OVERNIGHT_OBSERVATION
            }.also { logDiagnosis(it, newError, flowFiber, history) }
        } else {
            Diagnosis.NOT_MY_SPECIALTY
        }
    }

    private fun logDiagnosis(diagnosis: Diagnosis, newError: Throwable, flowFiber: FlowFiber, history: FlowMedicalHistory) {
        if (diagnosis != Diagnosis.NOT_MY_SPECIALTY) {
            log.debug {
                """
                        Flow ${flowFiber.id} given $diagnosis diagnosis due to a transition error
                        - Exception: ${newError.message}
                        - History: $history
                        ${(newError as? StateTransitionException)?.transitionAction?.let { "- Action: $it" }}
                        ${(newError as? StateTransitionException)?.transitionEvent?.let { "- Event: $it" }}
                        """.trimIndent()
            }
        }
    }
}