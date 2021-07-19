package net.corda.node.services.statemachine.hospital

import net.corda.core.flows.HospitalizeFlowException
import net.corda.node.services.statemachine.Diagnosis
import net.corda.node.services.statemachine.FlowFiber
import net.corda.node.services.statemachine.FlowMedicalHistory
import net.corda.node.services.statemachine.Staff
import net.corda.node.services.statemachine.StateMachineState
import net.corda.node.services.statemachine.mentionsThrowable

/**
 * Keeps the flow in for overnight observation if [HospitalizeFlowException] is received.
 */
object SedationNurse : Staff {
    override fun consult(
            flowFiber: FlowFiber,
            currentState: StateMachineState,
            newError: Throwable,
            history: FlowMedicalHistory
    ): Diagnosis {
        return if (newError.mentionsThrowable(HospitalizeFlowException::class.java)) {
            Diagnosis.OVERNIGHT_OBSERVATION
        } else {
            Diagnosis.NOT_MY_SPECIALTY
        }
    }
}