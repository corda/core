package net.corda.node.services.statemachine.hospital.external

import net.corda.node.services.statemachine.Chronic
import net.corda.node.services.statemachine.Diagnosis
import net.corda.node.services.statemachine.FlowFiber
import net.corda.node.services.statemachine.FlowMedicalHistory
import net.corda.node.services.statemachine.Staff
import net.corda.node.services.statemachine.StateMachineState
import net.corda.node.services.statemachine.mentionsThrowable
import java.sql.SQLException

// [DeadlockNurse] could go in corda transactions module?

/**
 * SQL Deadlock detection.
 */
object DeadlockNurse : Staff, Chronic {
    override fun consult(flowFiber: FlowFiber, currentState: StateMachineState, newError: Throwable, history: FlowMedicalHistory): Diagnosis {
        return if (mentionsDeadlock(newError)) {
            Diagnosis.DISCHARGE
        } else {
            Diagnosis.NOT_MY_SPECIALTY
        }
    }

    private fun mentionsDeadlock(exception: Throwable?): Boolean {
        return exception.mentionsThrowable(SQLException::class.java, "deadlock")
    }
}