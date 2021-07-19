package net.corda.node.services.statemachine.hospital.external

import net.corda.core.flows.NotaryError
import net.corda.core.flows.NotaryException
import net.corda.node.services.statemachine.Chronic
import net.corda.node.services.statemachine.Diagnosis
import net.corda.node.services.statemachine.FlowFiber
import net.corda.node.services.statemachine.FlowMedicalHistory
import net.corda.node.services.statemachine.Staff
import net.corda.node.services.statemachine.StateMachineState

// [NotaryDoctor] should be included in notary nodes only.

/**
 * Retry notarisation if the flow errors with a [NotaryError.General]. Notary flows are idempotent and only success or conflict
 * responses should be returned to the client.
 */
object NotaryDoctor : Staff, Chronic {
    override fun consult(flowFiber: FlowFiber,
                         currentState: StateMachineState,
                         newError: Throwable,
                         history: FlowMedicalHistory): Diagnosis {
        if (newError is NotaryException && newError.error is NotaryError.General) {
            return Diagnosis.DISCHARGE
        }
        return Diagnosis.NOT_MY_SPECIALTY
    }
}