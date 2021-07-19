package net.corda.node.services.statemachine.hospital

import net.corda.node.services.statemachine.Diagnosis
import net.corda.node.services.statemachine.FlowFiber
import net.corda.node.services.statemachine.FlowMedicalHistory
import net.corda.node.services.statemachine.Staff
import net.corda.node.services.statemachine.StateMachineState
import net.corda.node.services.statemachine.mentionsThrowable
import java.sql.SQLTransientConnectionException

/**
 * [SQLTransientConnectionException] detection that arise from failing to connect the underlying database/datasource
 */
object TransientConnectionCardiologist : Staff {
    override fun consult(
            flowFiber: FlowFiber,
            currentState: StateMachineState,
            newError: Throwable,
            history: FlowMedicalHistory
    ): Diagnosis {
        return if (mentionsTransientConnection(newError)) {
            if (history.notDischargedForTheSameThingMoreThan(2, this, currentState)) {
                Diagnosis.DISCHARGE
            } else {
                Diagnosis.OVERNIGHT_OBSERVATION
            }
        } else {
            Diagnosis.NOT_MY_SPECIALTY
        }
    }

    private fun mentionsTransientConnection(exception: Throwable?): Boolean {
        return exception.mentionsThrowable(SQLTransientConnectionException::class.java, "connection is not available")
    }
}