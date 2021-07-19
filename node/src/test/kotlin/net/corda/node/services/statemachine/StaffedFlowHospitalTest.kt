package net.corda.node.services.statemachine

import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.whenever
import net.corda.core.flows.HospitalizeFlowException
import net.corda.core.flows.StateMachineRunId
import net.corda.core.utilities.seconds
import net.corda.node.services.statemachine.hospital.SedationNurse
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.time.Clock
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class StaffedFlowHospitalTest {

    private val clock = mock<Clock>()
    private val flowMessaging = mock<FlowMessaging>()
    private val ourSenderUUID = "78d12c2c-12cc-11eb-adc1-0242ac120002"

    private val id = StateMachineRunId.createRandom()
    private val fiber = mock<FlowFiber>()
    private val currentState = mock<StateMachineState>()
    private val checkpoint = mock<Checkpoint>()
    private val checkpointState = mock<CheckpointState>()

    private var flowHospital: StaffedFlowHospital? = null

    @Before
    fun setUp() {
        doReturn(id).whenever(fiber).id
        doReturn(Instant.now()).whenever(clock).instant()
        doReturn(1).whenever(checkpointState).numberOfSuspends
        doReturn(checkpoint).whenever(currentState).checkpoint
        doReturn(checkpointState).whenever(checkpoint).checkpointState
    }

    @After
    fun cleanUp() {
        flowHospital = null
    }

    @Test(timeout = 300_000)
    fun `Hospital gives a Diagnosis_TERMINAL if not injected with any staff members`() {
        flowHospital = StaffedFlowHospital(flowMessaging, clock, ourSenderUUID)

        val (event, backOffForChronicCondition) = flowHospital!!.admit(
            fiber,
            currentState,
            listOf(HospitalizeFlowException())
        )

        assertTrue(event is Event.StartErrorPropagation)
        assertEquals(0.seconds, backOffForChronicCondition)
    }

    @Test(timeout = 300_000)
    fun `Hospital gives a Diagnosis_OVERNIGHT_OBSERVATION for HospitalizeFlowException if injected with SedationNurse`() {
        flowHospital = StaffedFlowHospital(flowMessaging, clock, ourSenderUUID, listOf(SedationNurse))

        val (event, backOffForChronicCondition) = flowHospital!!.admit(
            fiber,
            currentState,
            listOf(HospitalizeFlowException())
        )

        assertTrue(event is Event.OvernightObservation)
        assertEquals(0.seconds, backOffForChronicCondition)
    }
}