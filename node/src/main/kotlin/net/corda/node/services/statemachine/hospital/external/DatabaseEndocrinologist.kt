package net.corda.node.services.statemachine.hospital.external

import net.corda.core.internal.VisibleForTesting
import net.corda.node.services.statemachine.Diagnosis
import net.corda.node.services.statemachine.FlowFiber
import net.corda.node.services.statemachine.FlowMedicalHistory
import net.corda.node.services.statemachine.Staff
import net.corda.node.services.statemachine.StateMachineState
import java.sql.SQLException
import javax.persistence.PersistenceException

// [DatabaseEndocrinologist] could go in corda transactions module?

/**
 * Hospitalise any database (SQL and Persistence) exception that wasn't handled otherwise, unless on the configurable whitelist
 * Note that retry decisions from other specialists will not be affected as retries take precedence over hospitalisation.
 */
object DatabaseEndocrinologist : Staff {
    override fun consult(
            flowFiber: FlowFiber,
            currentState: StateMachineState,
            newError: Throwable,
            history: FlowMedicalHistory
    ): Diagnosis {
        return if ((newError is SQLException || newError is PersistenceException) && !customConditions.any { it(newError) }) {
            Diagnosis.OVERNIGHT_OBSERVATION
        } else {
            Diagnosis.NOT_MY_SPECIALTY
        }
    }

    @VisibleForTesting
    val customConditions = mutableSetOf<(t: Throwable) -> Boolean>()
}