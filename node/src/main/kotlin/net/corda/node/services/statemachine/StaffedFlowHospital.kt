package net.corda.node.services.statemachine

import net.corda.core.crypto.newSecureRandom
import net.corda.core.flows.StateMachineRunId
import net.corda.core.identity.Party
import net.corda.core.internal.ThreadBox
import net.corda.core.internal.VisibleForTesting
import net.corda.core.internal.bufferUntilSubscribed
import net.corda.core.messaging.DataFeed
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.minutes
import net.corda.core.utilities.seconds
import rx.subjects.PublishSubject
import java.io.Closeable
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.HashMap
import kotlin.concurrent.timerTask
import kotlin.math.pow

/**
 * This hospital consults "staff" to see if they can automatically diagnose and treat flows.
 */
@Suppress("TooManyFunctions")
class StaffedFlowHospital(private val flowMessaging: FlowMessaging,
                          private val clock: Clock,
                          private val ourSenderUUID: String,
                          private val staff: List<Staff> = listOf()) : Closeable {
    companion object {
        val log = contextLogger()

        private const val MAX_BACKOFF_TIME = 110.0 // Totals to 2 minutes when calculating the backoff time

        @VisibleForTesting
        val onFlowKeptForOvernightObservation = mutableListOf<(id: StateMachineRunId, by: List<String>) -> Unit>()

        @VisibleForTesting
        val onFlowDischarged = mutableListOf<(id: StateMachineRunId, by: List<String>) -> Unit>()

        @VisibleForTesting
        val onFlowErrorPropagated = mutableListOf<(id: StateMachineRunId, by: List<String>) -> Unit>()

        @VisibleForTesting
        val onFlowResuscitated = mutableListOf<(id: StateMachineRunId, by: List<String>, outcome: Outcome) -> Unit>()

        @VisibleForTesting
        val onFlowAdmitted = mutableListOf<(id: StateMachineRunId) -> Unit>()

        val EMPTY_DIAGNOSES = mapOf<Diagnosis, List<Staff>>(Diagnosis.TERMINAL to listOf())
    }

    private val hospitalJobTimer = Timer("FlowHospitalJobTimer", true)

    init {
        // Register a task to log (at intervals) flows that are kept in hospital for overnight observation.
        hospitalJobTimer.scheduleAtFixedRate(timerTask {
            mutex.locked {
                if (flowsInHospital.isNotEmpty()) {
                    // Get patients whose last record in their medical records is Outcome.OVERNIGHT_OBSERVATION.
                    val patientsUnderOvernightObservation =
                            flowsInHospital.filter { flowPatients[it.key]?.records?.last()?.outcome == Outcome.OVERNIGHT_OBSERVATION }
                    if (patientsUnderOvernightObservation.isNotEmpty())
                        log.warn("There are ${patientsUnderOvernightObservation.count()} flows kept for overnight observation. " +
                                "Affected flow ids: ${patientsUnderOvernightObservation.map { it.key.uuid.toString() }.joinToString()}")
                }
                if (treatableSessionInits.isNotEmpty()) {
                    log.warn("There are ${treatableSessionInits.count()} erroneous session initiations kept for overnight observation. " +
                            "Erroneous session initiation ids: ${treatableSessionInits.map { it.key.toString() }.joinToString()}")
                }
            }
        }, 1.minutes.toMillis(), 1.minutes.toMillis())
    }

    /**
     * Represents the flows that have been admitted to the hospital for treatment.
     * Flows should be removed from [flowsInHospital] when they have completed a successful transition.
     */
    private val flowsInHospital = ConcurrentHashMap<StateMachineRunId, FlowFiber>()

    /**
     * Returns true if the flow is currently being treated in the hospital.
     * The differs to flows with a medical history (which can accessed via [StaffedFlowHospital.contains]).
     */
    @VisibleForTesting
    internal fun flowInHospital(runId: StateMachineRunId): Boolean {
        // The .keys avoids https://youtrack.jetbrains.com/issue/KT-18053
        return runId in flowsInHospital.keys
    }

    private val mutex = ThreadBox(object {
        /**
         * Contains medical history of every flow (a patient) that has entered the hospital. A flow can leave the hospital,
         * but their medical history will be retained.
         *
         * Flows should be removed from [flowPatients] when they have completed successfully. Upon successful completion,
         * the medical history of a flow is no longer relevant as that flow has been completely removed from the
         * statemachine.
         */
        val flowPatients = HashMap<StateMachineRunId, FlowMedicalHistory>()
        val treatableSessionInits = HashMap<StateMachineRunId, InternalSessionInitRecord>()
        val recordsPublisher = PublishSubject.create<MedicalRecord>()
    })
    private val secureRandom = newSecureRandom()

    /**
     * The node was unable to initiate the [InitialSessionMessage] from [sender].
     */
    fun sessionInitErrored(sessionMessage: InitialSessionMessage, sender: Party, event: ExternalEvent.ExternalMessageEvent, error: Throwable) {
        val id = event.flowId
        val time = clock.instant()
        val outcome = if (error is SessionRejectException.UnknownClass) {
            // We probably don't have the CorDapp installed so let's pause the message in the hopes that the CorDapp is
            // installed on restart, at which point the message will be able proceed as normal. If not then it will need
            // to be dropped manually.
            Outcome.OVERNIGHT_OBSERVATION
        } else {
            Outcome.UNTREATABLE
        }

        val record = sessionMessage.run { MedicalRecord.SessionInit(id, time, outcome, initiatorFlowClassName, flowVersion, appName, sender, error) }
        mutex.locked {
            if (outcome != Outcome.UNTREATABLE) {
                treatableSessionInits[id] = InternalSessionInitRecord(sessionMessage, event, record)
                log.warn("$sender has sent a flow request for an unknown flow ${sessionMessage.initiatorFlowClassName}. Install the missing " +
                        "CorDapp this flow belongs to and restart.")
                log.warn("If you know it's safe to ignore this flow request then it can be deleted permanently using the killFlow RPC and " +
                        "the UUID $id (from the node shell you can run 'flow kill $id'). BE VERY CAUTIOUS OF THIS SECOND APPROACH AS THE " +
                        "REQUEST MAY CONTAIN A NOTARISED TRANSACTION THAT NEEDS TO BE RECORDED IN YOUR VAULT.")
            }
            recordsPublisher.onNext(record)
        }

        if (outcome == Outcome.UNTREATABLE) {
            sendBackError(error, sessionMessage, sender, event)
        }
    }

    private fun sendBackError(error: Throwable, sessionMessage: InitialSessionMessage, sender: Party, event: ExternalEvent.ExternalMessageEvent) {
        val message = (error as? SessionRejectException)?.message ?: "Unable to establish session"
        val payload = RejectSessionMessage(message, secureRandom.nextLong())
        val replyError = ExistingSessionMessage(sessionMessage.initiatorSessionId, payload)

        log.info("Sending session initiation error back to $sender", error)

        flowMessaging.sendSessionMessage(sender, replyError, SenderDeduplicationId(DeduplicationId.createRandom(secureRandom), ourSenderUUID))
        event.deduplicationHandler.afterDatabaseTransaction()
    }

    /**
     * Drop the errored session-init message with the given ID ([MedicalRecord.SessionInit.id]). This will cause the node
     * to send back the relevant session error to the initiator party and acknowledge its receipt from the message broker
     * so that it never gets redelivered.
     */
    fun dropSessionInit(id: StateMachineRunId): Boolean {
        val (sessionMessage, event, publicRecord) = mutex.locked {
            treatableSessionInits.remove(id) ?: return false
        }
        log.info("Errored session-init permanently dropped: $publicRecord")
        sendBackError(publicRecord.error, sessionMessage, publicRecord.sender, event)
        return true
    }

    /**
     * Forces the flow to be kept in for overnight observation by the hospital.
     *
     * @param currentState The [StateMachineState] of the flow that is being forced into observation
     * @param errors The errors to include in the new medical record
     * @param by The staff member(s) that will give the new medical record
     */
    fun forceIntoOvernightObservation(currentState: StateMachineState, errors: List<Throwable>, by: List<Staff>) {
        mutex.locked {
            val id = currentState.flowLogic.runId
            val medicalHistory = flowPatients.computeIfAbsent(id) { FlowMedicalHistory() }
            val record = MedicalRecord.Flow(
                time = clock.instant(),
                flowId = id,
                suspendCount = currentState.checkpoint.checkpointState.numberOfSuspends,
                errors = errors,
                by = by,
                outcome = Outcome.OVERNIGHT_OBSERVATION
            )

            medicalHistory.records += record

            onFlowKeptForOvernightObservation.forEach { hook -> hook.invoke(id, record.by.map { it.toString() }) }
            recordsPublisher.onNext(record)
        }
    }

    /**
     * Request treatment for the [flowFiber].
     */
    fun requestTreatment(flowFiber: FlowFiber, currentState: StateMachineState, errors: List<Throwable>) {
        if (!currentState.isRemoved) {
            flowsInHospital[flowFiber.id] = flowFiber
            val (event, backOffForChronicCondition) = admit(flowFiber, currentState, errors)

            if (backOffForChronicCondition.isZero) {
                flowFiber.scheduleEvent(event)
            } else {
                hospitalJobTimer.schedule(timerTask {
                    flowFiber.scheduleEvent(event)
                }, backOffForChronicCondition.toMillis())
            }
        }
    }

    @Suppress("ComplexMethod")
    @VisibleForTesting
    fun admit(flowFiber: FlowFiber, currentState: StateMachineState, errors: List<Throwable>): Pair<Event, Duration> {
        val time = clock.instant()
        log.info("Flow ${flowFiber.id} admitted to hospital in state $currentState")
        onFlowAdmitted.forEach { it.invoke(flowFiber.id) }

        return mutex.locked {
            val medicalHistory = flowPatients.computeIfAbsent(flowFiber.id) { FlowMedicalHistory() }

            val report = consultStaff(flowFiber, currentState, errors, medicalHistory)

            val (outcome, event, backOffForChronicCondition) = when (report.diagnosis) {
                Diagnosis.RESUSCITATE -> {
                    // reschedule the last outcome as it failed to process it
                    // do a 0.seconds backoff in dev mode? / when coming from the driver? make it configurable?
                    val outcome = medicalHistory.records.last().outcome
                    val backOff = calculateBackOffForResuscitation(medicalHistory, currentState, outcome)
                    log.info("Flow error to be resuscitated, rescheduling previous outcome - $outcome (delay ${backOff.seconds}s) by ${report.by} (error was ${report.error.message})")
                    onFlowResuscitated.forEach { hook -> hook.invoke(flowFiber.id, report.by.map { it.toString() }, outcome) }
                    Triple(outcome, outcome.event, backOff)
                }
                Diagnosis.DISCHARGE -> {
                    val backOff = calculateBackOffForChronicCondition(report, medicalHistory, currentState)
                    log.info("Flow error discharged from hospital (delay ${backOff.seconds}s) by ${report.by} (error was ${report.error.message})")
                    onFlowDischarged.forEach { hook -> hook.invoke(flowFiber.id, report.by.map { it.toString() }) }
                    Triple(Outcome.DISCHARGE, Event.RetryFlowFromSafePoint, backOff)
                }
                Diagnosis.OVERNIGHT_OBSERVATION -> {
                    log.info("Flow error kept for overnight observation by ${report.by} (error was ${report.error.message})")
                    // We don't schedule a next event for the flow - it will automatically retry from its checkpoint on node restart
                    onFlowKeptForOvernightObservation.forEach { hook -> hook.invoke(flowFiber.id, report.by.map { it.toString() }) }
                    Triple(Outcome.OVERNIGHT_OBSERVATION, Event.OvernightObservation, 0.seconds)
                }
                Diagnosis.NOT_MY_SPECIALTY, Diagnosis.TERMINAL -> {
                    // None of the staff care for these errors, or someone decided it is a terminal condition, so we let them propagate
                    log.info("Flow error allowed to propagate", report.error)
                    onFlowErrorPropagated.forEach { hook -> hook.invoke(flowFiber.id, report.by.map { it.toString() }) }
                    Triple(Outcome.UNTREATABLE, Event.StartErrorPropagation, 0.seconds)
                }
            }

            val record = MedicalRecord.Flow(
                time,
                flowFiber.id,
                currentState.checkpoint.checkpointState.numberOfSuspends,
                errors,
                report.by,
                outcome,
                report.diagnosis == Diagnosis.RESUSCITATE
            )
            medicalHistory.records += record
            recordsPublisher.onNext(record)
            Pair(event, backOffForChronicCondition)
        }
    }

    private fun calculateBackOffForChronicCondition(
        report: ConsultationReport,
        medicalHistory: FlowMedicalHistory,
        currentState: StateMachineState
    ): Duration {
        return report.by.firstOrNull { it is Chronic }?.let { staff ->
            calculateBackOff(medicalHistory.timesDischargedForTheSameThing(staff, currentState))
        } ?: 0.seconds
    }

    private fun calculateBackOffForResuscitation(
        medicalHistory: FlowMedicalHistory,
        currentState: StateMachineState,
        outcome: Outcome
    ): Duration = calculateBackOff(medicalHistory.timesResuscitatedForTheSameOutcome(currentState, outcome))

    private fun calculateBackOff(timesDiagnosisGiven: Int): Duration {
        return if (timesDiagnosisGiven == 0) {
            0.seconds
        } else {
            maxOf(10, (10 + (Math.random()) * minOf(MAX_BACKOFF_TIME, (10 * 1.5.pow(timesDiagnosisGiven)) / 2)).toInt()).seconds
        }
    }

    private fun consultStaff(flowFiber: FlowFiber,
                             currentState: StateMachineState,
                             errors: List<Throwable>,
                             medicalHistory: FlowMedicalHistory): ConsultationReport {
        return errors
                .asSequence()
                .mapIndexed { index, error ->
                    // Rely on the logging context to print details of the flow ID.
                    log.info("Error ${index + 1} of ${errors.size}:", error)
                    val diagnoses: Map<Diagnosis, List<Staff>> =
                        staff.run {
                            if (isEmpty()) {
                                log.info("Staff flow hospital has no staff members.")
                                EMPTY_DIAGNOSES
                            } else {
                                this.groupBy { it.consult(flowFiber, currentState, error, medicalHistory) }
                            }
                        }
                    // We're only interested in the highest priority diagnosis for the error
                    val (diagnosis, by) = diagnoses.entries.minBy { it.key }!!
                    ConsultationReport(error, diagnosis, by)
                }
                // And we're only interested in the error with the highest priority diagnosis
                .minBy { it.diagnosis }!!
    }

    private data class ConsultationReport(val error: Throwable, val diagnosis: Diagnosis, val by: List<Staff>)

    /**
     * Remove the flow's medical history from the hospital.
     */
    fun removeMedicalHistory(flowId: StateMachineRunId) {
        mutex.locked { flowPatients.remove(flowId) }
    }

    /**
     * Remove the flow from the hospital as it is not currently being treated.
     */
    fun leave(id: StateMachineRunId) {
        flowsInHospital.remove(id)
    }

    // TODO MedicalRecord subtypes can expose the Staff class, something which we probably don't want when wiring this method to RPC
    /** Returns a stream of medical records as flows pass through the hospital. */
    fun track(): DataFeed<List<MedicalRecord>, MedicalRecord> {
        return mutex.locked {
            val snapshot = (flowPatients.values.flatMap { it.records } + treatableSessionInits.values.map { it.publicRecord }).sortedBy { it.time }
            DataFeed(snapshot, recordsPublisher.bufferUntilSubscribed())
        }
    }

    operator fun contains(flowId: StateMachineRunId) = mutex.locked { flowId in flowPatients }

    override fun close() {
        hospitalJobTimer.cancel()
    }

    private data class InternalSessionInitRecord(val sessionMessage: InitialSessionMessage,
                                                 val event: ExternalEvent.ExternalMessageEvent,
                                                 val publicRecord: MedicalRecord.SessionInit)

    sealed class MedicalRecord {
        abstract val time: Instant
        abstract val outcome: Outcome
        abstract val errors: List<Throwable>

        /** Medical record for a flow that has errored. */
        data class Flow(override val time: Instant,
                        val flowId: StateMachineRunId,
                        val suspendCount: Int,
                        override val errors: List<Throwable>,
                        val by: List<Staff>,
                        override val outcome: Outcome,
                        val resuscitated: Boolean = false) : MedicalRecord()

        /** Medical record for a session initiation that was unsuccessful. */
        data class SessionInit(val id: StateMachineRunId,
                               override val time: Instant,
                               override val outcome: Outcome,
                               val initiatorFlowClassName: String,
                               val flowVersion: Int,
                               val appName: String,
                               val sender: Party,
                               val error: Throwable) : MedicalRecord() {
            override val errors: List<Throwable> get() = listOf(error)
        }
    }

    enum class Outcome(val event: Event) {
        DISCHARGE(Event.RetryFlowFromSafePoint),
        OVERNIGHT_OBSERVATION(Event.OvernightObservation),
        UNTREATABLE(Event.StartErrorPropagation)
    }
}

// The below classes, interfaces ,helper method should be moved under :core to be available for Cordapp developers
// to define their own staff members if they wanted to
class FlowMedicalHistory {
    internal val records: MutableList<StaffedFlowHospital.MedicalRecord.Flow> = mutableListOf()

    fun notDischargedForTheSameThingMoreThan(max: Int, by: Staff, currentState: StateMachineState): Boolean {
        return timesDischargedForTheSameThing(by, currentState) <= max
    }

    fun timesDischargedForTheSameThing(by: Staff, currentState: StateMachineState): Int {
        val lastAdmittanceSuspendCount = currentState.checkpoint.checkpointState.numberOfSuspends
        return records.count { it.outcome == StaffedFlowHospital.Outcome.DISCHARGE && by in it.by && it.suspendCount == lastAdmittanceSuspendCount }
    }

    fun timesResuscitatedForTheSameOutcome(currentState: StateMachineState, outcome: StaffedFlowHospital.Outcome): Int {
        val lastAdmittanceSuspendCount = currentState.checkpoint.checkpointState.numberOfSuspends
        return records.count { it.resuscitated && it.outcome == outcome && it.suspendCount == lastAdmittanceSuspendCount }
    }

    override fun toString(): String = "${this.javaClass.simpleName}(records = $records)"
}

/** The order of the enum values are in priority order. */
enum class Diagnosis {
    /** Retry the last outcome/diagnosis **/
    RESUSCITATE,
    /** The flow should not see other staff members */
    TERMINAL,
    /** Retry from last safe point. */
    DISCHARGE,
    /** Park and await intervention. */
    OVERNIGHT_OBSERVATION,
    /** Please try another member of staff. */
    NOT_MY_SPECIALTY
}

// Since Flow hospital stays in :node, then everything that staff members need, need to move under :core.
interface Staff {
    fun consult(flowFiber: FlowFiber, currentState: StateMachineState, newError: Throwable, history: FlowMedicalHistory): Diagnosis
}

/**
 * The [Chronic] interface relates to [Staff] that return diagnoses that can be constantly be diagnosed if the flow keeps returning to
 * the hospital. [Chronic] diagnoses apply a backoff before scheduling a new [Event], this prevents a flow from constantly retrying
 * without a chance for the underlying issue to resolve itself.
 */
interface Chronic

fun <T : Throwable> Throwable?.mentionsThrowable(exceptionType: Class<T>, errorMessage: String? = null): Boolean {
    if (this == null) {
        return false
    }
    val containsMessage = if (errorMessage != null) {
        message?.toLowerCase()?.contains(errorMessage) ?: false
    } else {
        true
    }
    return (exceptionType.isAssignableFrom(this::class.java) && containsMessage) || cause.mentionsThrowable(exceptionType, errorMessage)
}