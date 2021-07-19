package net.corda.node.services.statemachine.hospital.external

import net.corda.node.services.statemachine.Diagnosis
import net.corda.node.services.statemachine.FlowFiber
import net.corda.node.services.statemachine.FlowMedicalHistory
import net.corda.node.services.statemachine.Staff
import net.corda.node.services.statemachine.StateMachineState
import net.corda.node.services.statemachine.mentionsThrowable
import org.hibernate.exception.ConstraintViolationException

// [DuplicateInsertSpecialist] could go in corda transactions module?

/**
 * Primary key violation detection for duplicate inserts.  Will detect other constraint violations too.
 */
object DuplicateInsertSpecialist : Staff {
    override fun consult(flowFiber: FlowFiber, currentState: StateMachineState, newError: Throwable, history: FlowMedicalHistory): Diagnosis {
        return if (newError.mentionsThrowable(ConstraintViolationException::class.java)
                && history.notDischargedForTheSameThingMoreThan(2, this, currentState)) {
            Diagnosis.DISCHARGE
        } else {
            Diagnosis.NOT_MY_SPECIALTY
        }
    }
}