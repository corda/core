package net.corda.node.services.persistence

import net.corda.core.context.InvocationContext
import net.corda.core.context.InvocationOrigin
import net.corda.core.flows.StateMachineRunId
import net.corda.core.internal.PLATFORM_VERSION
import net.corda.core.internal.VisibleForTesting
import net.corda.core.internal.uncheckedCast
import net.corda.core.serialization.SerializationDefaults
import net.corda.core.serialization.SerializedBytes
import net.corda.core.serialization.serialize
import net.corda.core.utilities.contextLogger
import net.corda.node.services.api.CheckpointStorage
import net.corda.node.services.statemachine.Checkpoint
import net.corda.node.services.statemachine.Checkpoint.FlowStatus
import net.corda.node.services.statemachine.CheckpointState
import net.corda.node.services.statemachine.ErrorState
import net.corda.node.services.statemachine.FlowState
import net.corda.node.services.statemachine.SubFlowVersion
import net.corda.nodeapi.internal.persistence.NODE_DATABASE_PREFIX
import net.corda.nodeapi.internal.persistence.currentDBSession
import org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY
import org.apache.commons.lang3.exception.ExceptionUtils
import org.hibernate.annotations.Type
import java.sql.Connection
import java.sql.SQLException
import java.time.Clock
import java.time.Instant
import java.util.*
import java.util.stream.Stream
import javax.persistence.Column
import javax.persistence.Entity
import javax.persistence.FetchType
import javax.persistence.Id
import javax.persistence.OneToOne
import javax.persistence.PrimaryKeyJoinColumn

/**
 * Simple checkpoint key value storage in DB.
 */
@Suppress("TooManyFunctions")
class DBCheckpointStorage(
    private val checkpointPerformanceRecorder: CheckpointPerformanceRecorder,
    private val clock: Clock
) : CheckpointStorage {

    companion object {
        val log = contextLogger()

        private const val HMAC_SIZE_BYTES = 16

        @VisibleForTesting
        const val MAX_STACKTRACE_LENGTH = 2000
        private const val MAX_EXC_MSG_LENGTH = 2000
        private const val MAX_EXC_TYPE_LENGTH = 256
        private const val MAX_FLOW_NAME_LENGTH = 128
        private const val MAX_PROGRESS_STEP_LENGTH = 256

        private val RUNNABLE_CHECKPOINTS = setOf(FlowStatus.RUNNABLE, FlowStatus.HOSPITALIZED)

        /**
         * This needs to run before Hibernate is initialised.
         *
         * No need to set up [DBCheckpointStorage] fully for this function
         *
         * @param connection The SQL Connection.
         * @return the number of checkpoints stored in the database.
         */
        fun getCheckpointCount(connection: Connection): Long {
            // No need to set up [DBCheckpointStorage] fully for this function
            return try {
                connection.prepareStatement("select count(*) from node_checkpoints").use { ps ->
                    ps.executeQuery().use { rs ->
                        rs.next()
                        rs.getLong(1)
                    }
                }
            } catch (e: SQLException) {
                // Happens when the table was not created yet.
                0L
            }
        }
    }

    enum class StartReason {
        RPC, SERVICE, SCHEDULED, INITIATED
    }

    @Entity
    @javax.persistence.Table(name = "${NODE_DATABASE_PREFIX}checkpoints")
    class DBFlowCheckpoint(
        @Id
        @Column(name = "flow_id", length = 64, nullable = false)
        var flowId: String,

        @OneToOne(fetch = FetchType.LAZY, optional = true)
        @PrimaryKeyJoinColumn
        var blob: DBFlowCheckpointBlob?,

        @OneToOne(fetch = FetchType.LAZY, optional = true)
        @PrimaryKeyJoinColumn
        var result: DBFlowResult?,

        @OneToOne(fetch = FetchType.LAZY, optional = true)
        @PrimaryKeyJoinColumn
        var exceptionDetails: DBFlowException?,

        @OneToOne(fetch = FetchType.LAZY)
        @PrimaryKeyJoinColumn
        var flowMetadata: DBFlowMetadata,

        @Column(name = "status", nullable = false)
        var status: FlowStatus,

        @Column(name = "compatible", nullable = false)
        var compatible: Boolean,

        @Column(name = "progress_step")
        var progressStep: String?,

        @Column(name = "flow_io_request")
        var ioRequestType: String?,

        @Column(name = "timestamp", nullable = false)
        var checkpointInstant: Instant
    )

    @Entity
    @javax.persistence.Table(name = "${NODE_DATABASE_PREFIX}checkpoint_blobs")
    class DBFlowCheckpointBlob(
        @Id
        @Column(name = "flow_id", length = 64, nullable = false)
        var flowId: String,

        @Type(type = "corda-blob")
        @Column(name = "checkpoint_value", nullable = false)
        var checkpoint: ByteArray = EMPTY_BYTE_ARRAY,

        @Type(type = "corda-blob")
        @Column(name = "flow_state")
        var flowStack: ByteArray?,

        @Type(type = "corda-wrapper-binary")
        @Column(name = "hmac")
        var hmac: ByteArray,

        @Column(name = "timestamp")
        var persistedInstant: Instant
    )

    @Entity
    @javax.persistence.Table(name = "${NODE_DATABASE_PREFIX}flow_results")
    class DBFlowResult(
        @Id
        @Column(name = "flow_id", length = 64, nullable = false)
        var flow_id: String,

        @Type(type = "corda-blob")
        @Column(name = "result_value", nullable = false)
        var value: ByteArray = EMPTY_BYTE_ARRAY,

        @Column(name = "timestamp")
        val persistedInstant: Instant
    )

    @Entity
    @javax.persistence.Table(name = "${NODE_DATABASE_PREFIX}flow_exceptions")
    class DBFlowException(
        @Id
        @Column(name = "flow_id", length = 64, nullable = false)
        var flow_id: String,

        @Column(name = "type", nullable = false)
        var type: String,

        @Column(name = "exception_message")
        var message: String? = null,

        @Column(name = "stack_trace", nullable = false)
        var stackTrace: String,

        @Type(type = "corda-blob")
        @Column(name = "exception_value")
        var value: ByteArray? = null,

        @Column(name = "timestamp")
        val persistedInstant: Instant
    )

    @Entity
    @javax.persistence.Table(name = "${NODE_DATABASE_PREFIX}flow_metadata")
    class DBFlowMetadata(
        @Id
        @Column(name = "flow_id", length = 64, nullable = false)
        var flowId: String,

        @Column(name = "invocation_id", nullable = false)
        var invocationId: String,

        @Column(name = "flow_name", nullable = false)
        var flowName: String,

        @Column(name = "flow_identifier", nullable = true)
        var userSuppliedIdentifier: String?,

        @Column(name = "started_type", nullable = false)
        var startType: StartReason,

        @Type(type = "corda-blob")
        @Column(name = "flow_parameters", nullable = false)
        var initialParameters: ByteArray = EMPTY_BYTE_ARRAY,

        @Column(name = "cordapp_name", nullable = false)
        var launchingCordapp: String,

        @Column(name = "platform_version", nullable = false)
        var platformVersion: Int,

        @Column(name = "started_by", nullable = false)
        var startedBy: String,

        @Column(name = "invocation_time", nullable = false)
        var invocationInstant: Instant,

        @Column(name = "start_time", nullable = true)
        var startInstant: Instant,

        @Column(name = "finish_time", nullable = true)
        var finishInstant: Instant?
    ) {
        @Suppress("ComplexMethod")
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as DBFlowMetadata

            if (flowId != other.flowId) return false
            if (invocationId != other.invocationId) return false
            if (flowName != other.flowName) return false
            if (userSuppliedIdentifier != other.userSuppliedIdentifier) return false
            if (startType != other.startType) return false
            if (!initialParameters.contentEquals(other.initialParameters)) return false
            if (launchingCordapp != other.launchingCordapp) return false
            if (platformVersion != other.platformVersion) return false
            if (startedBy != other.startedBy) return false
            if (invocationInstant != other.invocationInstant) return false
            if (startInstant != other.startInstant) return false
            if (finishInstant != other.finishInstant) return false

            return true
        }

        override fun hashCode(): Int {
            var result = flowId.hashCode()
            result = 31 * result + invocationId.hashCode()
            result = 31 * result + flowName.hashCode()
            result = 31 * result + (userSuppliedIdentifier?.hashCode() ?: 0)
            result = 31 * result + startType.hashCode()
            result = 31 * result + initialParameters.contentHashCode()
            result = 31 * result + launchingCordapp.hashCode()
            result = 31 * result + platformVersion
            result = 31 * result + startedBy.hashCode()
            result = 31 * result + invocationInstant.hashCode()
            result = 31 * result + startInstant.hashCode()
            result = 31 * result + (finishInstant?.hashCode() ?: 0)
            return result
        }
    }

    override fun addCheckpoint(
        id: StateMachineRunId,
        checkpoint: Checkpoint,
        serializedFlowState: SerializedBytes<FlowState>,
        serializedCheckpointState: SerializedBytes<CheckpointState>
    ) {
        val now = clock.instant()
        val flowId = id.uuid.toString()

        checkpointPerformanceRecorder.record(serializedCheckpointState, serializedFlowState)

        val blob = createDBCheckpointBlob(
            flowId,
            serializedCheckpointState,
            serializedFlowState,
            now
        )

        val metadata = createDBFlowMetadata(flowId, checkpoint)

        // Most fields are null as they cannot have been set when creating the initial checkpoint
        val dbFlowCheckpoint = DBFlowCheckpoint(
            flowId = flowId,
            blob = blob,
            result = null,
            exceptionDetails = null,
            flowMetadata = metadata,
            status = checkpoint.status,
            compatible = checkpoint.compatible,
            progressStep = null,
            ioRequestType = null,
            checkpointInstant = now
        )

        currentDBSession().save(dbFlowCheckpoint)
        currentDBSession().save(blob)
        currentDBSession().save(metadata)
    }

    override fun updateCheckpoint(
        id: StateMachineRunId, checkpoint: Checkpoint, serializedFlowState: SerializedBytes<FlowState>?,
        serializedCheckpointState: SerializedBytes<CheckpointState>
    ) {
        val now = clock.instant()
        val flowId = id.uuid.toString()

        // Do not update in DB [Checkpoint.checkpointState] or [Checkpoint.flowState] if flow failed or got hospitalized
        val blob = if (checkpoint.status == FlowStatus.FAILED || checkpoint.status == FlowStatus.HOSPITALIZED) {
            null
        } else {
            checkpointPerformanceRecorder.record(serializedCheckpointState, serializedFlowState)
            createDBCheckpointBlob(
                flowId,
                serializedCheckpointState,
                serializedFlowState,
                now
            )
        }

        //This code needs to be added back in when we want to persist the result. For now this requires the result to be @CordaSerializable.
        //val result = updateDBFlowResult(entity, checkpoint, now)
        val exceptionDetails = updateDBFlowException(flowId, checkpoint, now)

        val metadata = createDBFlowMetadata(flowId, checkpoint)

        val dbFlowCheckpoint = DBFlowCheckpoint(
            flowId = flowId,
            blob = blob,
            result = null,
            exceptionDetails = exceptionDetails,
            flowMetadata = metadata,
            status = checkpoint.status,
            compatible = checkpoint.compatible,
            progressStep = checkpoint.progressStep?.take(MAX_PROGRESS_STEP_LENGTH),
            ioRequestType = checkpoint.flowIoRequest,
            checkpointInstant = now
        )

        currentDBSession().update(dbFlowCheckpoint)
        blob?.let { currentDBSession().update(it) }
        if (checkpoint.isFinished()) {
            metadata.finishInstant = now
            currentDBSession().update(metadata)
        }
    }

    override fun markAllPaused() {
        val session = currentDBSession()
        val runnableOrdinals = RUNNABLE_CHECKPOINTS.map { "${it.ordinal}" }.joinToString { it }
        val sqlQuery = "Update ${NODE_DATABASE_PREFIX}checkpoints set status = ${FlowStatus.PAUSED.ordinal} " +
                "where status in ($runnableOrdinals)"
        val query = session.createNativeQuery(sqlQuery)
        query.executeUpdate()
    }

    // DBFlowResult and DBFlowException to be integrated with rest of schema
    @Suppress("MagicNumber")
    override fun removeCheckpoint(id: StateMachineRunId): Boolean {
        var deletedRows = 0
        val flowId = id.uuid.toString()
        deletedRows += deleteRow(DBFlowMetadata::class.java, DBFlowMetadata::flowId.name, flowId)
        deletedRows += deleteRow(DBFlowCheckpointBlob::class.java, DBFlowCheckpointBlob::flowId.name, flowId)
        deletedRows += deleteRow(DBFlowCheckpoint::class.java, DBFlowCheckpoint::flowId.name, flowId)
//        resultId?.let { deletedRows += deleteRow(DBFlowResult::class.java, DBFlowResult::flow_id.name, it.toString()) }
//        exceptionId?.let { deletedRows += deleteRow(DBFlowException::class.java, DBFlowException::flow_id.name, it.toString()) }
        return deletedRows == 3
    }

    private fun <T> deleteRow(clazz: Class<T>, pk: String, value: String): Int {
        val session = currentDBSession()
        val criteriaBuilder = session.criteriaBuilder
        val delete = criteriaBuilder.createCriteriaDelete(clazz)
        val root = delete.from(clazz)
        delete.where(criteriaBuilder.equal(root.get<String>(pk), value))
        return session.createQuery(delete).executeUpdate()
    }

    @Throws(SQLException::class)
    override fun getCheckpoint(id: StateMachineRunId): Checkpoint.Serialized? {
        return getDBCheckpoint(id)?.toSerializedCheckpoint()
    }

    override fun getCheckpoints(statuses: Collection<FlowStatus>): Stream<Pair<StateMachineRunId, Checkpoint.Serialized>> {
        val session = currentDBSession()
        val criteriaBuilder = session.criteriaBuilder
        val criteriaQuery = criteriaBuilder.createQuery(DBFlowCheckpoint::class.java)
        val root = criteriaQuery.from(DBFlowCheckpoint::class.java)
        criteriaQuery.select(root)
            .where(criteriaBuilder.isTrue(root.get<FlowStatus>(DBFlowCheckpoint::status.name).`in`(statuses)))
        return session.createQuery(criteriaQuery).stream().map {
            StateMachineRunId(UUID.fromString(it.flowId)) to it.toSerializedCheckpoint()
        }
    }

    override fun getCheckpointsToRun(): Stream<Pair<StateMachineRunId, Checkpoint.Serialized>> {
        return getCheckpoints(RUNNABLE_CHECKPOINTS)
    }

    @VisibleForTesting
    internal fun getDBCheckpoint(id: StateMachineRunId): DBFlowCheckpoint? {
        return currentDBSession().find(DBFlowCheckpoint::class.java, id.uuid.toString())
    }

    override fun getPausedCheckpoints(): Stream<Pair<StateMachineRunId, Checkpoint.Serialized>> {
        val session = currentDBSession()
        val jpqlQuery = """select new ${DBPausedFields::class.java.name}(checkpoint.id, blob.checkpoint, checkpoint.status,
                checkpoint.progressStep, checkpoint.ioRequestType, checkpoint.compatible) from ${DBFlowCheckpoint::class.java.name}
                checkpoint join ${DBFlowCheckpointBlob::class.java.name} blob on checkpoint.blob = blob.id where
                checkpoint.status = ${FlowStatus.PAUSED.ordinal}""".trimIndent()
        val query = session.createQuery(jpqlQuery, DBPausedFields::class.java)
        return query.resultList.stream().map {
            StateMachineRunId(UUID.fromString(it.id)) to it.toSerializedCheckpoint()
        }
    }

    override fun updateStatus(runId: StateMachineRunId, flowStatus: FlowStatus) {
        val update = "Update ${NODE_DATABASE_PREFIX}checkpoints set status = ${flowStatus.ordinal} where flow_id = '${runId.uuid}'"
        currentDBSession().createNativeQuery(update).executeUpdate()
    }

    override fun updateCompatible(runId: StateMachineRunId, compatible: Boolean) {
        val update = "Update ${NODE_DATABASE_PREFIX}checkpoints set compatible = $compatible where flow_id = '${runId.uuid}'"
        currentDBSession().createNativeQuery(update).executeUpdate()
    }

    private fun createDBFlowMetadata(flowId: String, checkpoint: Checkpoint): DBFlowMetadata {
        val context = checkpoint.checkpointState.invocationContext
        val flowInfo = checkpoint.checkpointState.subFlowStack.first()
        return DBFlowMetadata(
            flowId = flowId,
            invocationId = context.trace.invocationId.value,
            // Truncate the flow name to fit into the database column
            // Flow names are unlikely to be this long
            flowName = flowInfo.flowClass.name.take(MAX_FLOW_NAME_LENGTH),
            // will come from the context
            userSuppliedIdentifier = null,
            startType = context.getStartedType(),
            initialParameters = context.getFlowParameters().storageSerialize().bytes,
            launchingCordapp = (flowInfo.subFlowVersion as? SubFlowVersion.CorDappFlow)?.corDappName ?: "Core flow",
            platformVersion = PLATFORM_VERSION,
            startedBy = context.principal().name,
            invocationInstant = context.trace.invocationId.timestamp,
            startInstant = clock.instant(),
            finishInstant = null
        )
    }

    private fun createDBCheckpointBlob(
        flowId: String,
        serializedCheckpointState: SerializedBytes<CheckpointState>,
        serializedFlowState: SerializedBytes<FlowState>?,
        now: Instant
    ): DBFlowCheckpointBlob {
        return DBFlowCheckpointBlob(
            flowId = flowId,
            checkpoint = serializedCheckpointState.bytes,
            flowStack = serializedFlowState?.bytes,
            hmac = ByteArray(HMAC_SIZE_BYTES),
            persistedInstant = now
        )
    }

    /**
     * Creates, updates or deletes the result related to the current flow/checkpoint.
     *
     * This is needed because updates are not cascading via Hibernate, therefore operations must be handled manually.
     *
     * A [DBFlowResult] is created if [DBFlowCheckpoint.result] does not exist and the [Checkpoint] has a result..
     * The existing [DBFlowResult] is updated if [DBFlowCheckpoint.result] exists and the [Checkpoint] has a result.
     * The existing [DBFlowResult] is deleted if [DBFlowCheckpoint.result] exists and the [Checkpoint] has no result.
     * Nothing happens if both [DBFlowCheckpoint] and [Checkpoint] do not have a result.
     */
    private fun updateDBFlowResult(flowId: String, entity: DBFlowCheckpoint, checkpoint: Checkpoint, now: Instant): DBFlowResult? {
        val result = checkpoint.result?.let { createDBFlowResult(flowId, it, now) }
        if (entity.result != null) {
            if (result != null) {
                result.flow_id = entity.result!!.flow_id
                currentDBSession().update(result)
            } else {
                currentDBSession().delete(entity.result)
            }
        } else if (result != null) {
            currentDBSession().save(result)
        }
        return result
    }

    private fun createDBFlowResult(flowId: String, result: Any, now: Instant): DBFlowResult {
        return DBFlowResult(
            flow_id = flowId,
            value = result.storageSerialize().bytes,
            persistedInstant = now
        )
    }

    /**
     * Creates, updates or deletes the error related to the current flow/checkpoint.
     *
     * This is needed because updates are not cascading via Hibernate, therefore operations must be handled manually.
     *
     * A [DBFlowException] is created if [DBFlowCheckpoint.exceptionDetails] does not exist and the [Checkpoint] has an error attached to it.
     * The existing [DBFlowException] is updated if [DBFlowCheckpoint.exceptionDetails] exists and the [Checkpoint] has an error.
     * The existing [DBFlowException] is deleted if [DBFlowCheckpoint.exceptionDetails] exists and the [Checkpoint] has no error.
     * Nothing happens if both [DBFlowCheckpoint] and [Checkpoint] are related to no errors.
     */
    // DBFlowException to be integrated with rest of schema
    // Add a flag notifying if an exception is already saved in the database for below logic (are we going to do this after all?)
    private fun updateDBFlowException(flowId: String, checkpoint: Checkpoint, now: Instant): DBFlowException? {
        val exceptionDetails = (checkpoint.errorState as? ErrorState.Errored)?.let { createDBFlowException(flowId, it, now) }
//        if (checkpoint.dbExoSkeleton.dbFlowExceptionId != null) {
//            if (exceptionDetails != null) {
//                exceptionDetails.flow_id = checkpoint.dbExoSkeleton.dbFlowExceptionId!!
//                currentDBSession().update(exceptionDetails)
//            } else {
//                val session = currentDBSession()
//                val entity = session.get(DBFlowException::class.java, checkpoint.dbExoSkeleton.dbFlowExceptionId)
//                session.delete(entity)
//                return null
//            }
//        } else if (exceptionDetails != null) {
//            currentDBSession().save(exceptionDetails)
//            checkpoint.dbExoSkeleton.dbFlowExceptionId = exceptionDetails.flow_id
//        }
        return exceptionDetails
    }

    private fun createDBFlowException(flowId: String, errorState: ErrorState.Errored, now: Instant): DBFlowException {
        return errorState.errors.last().exception.let {
            DBFlowException(
                flow_id = flowId,
                type = it::class.java.name.truncate(MAX_EXC_TYPE_LENGTH, true),
                message = it.message?.truncate(MAX_EXC_MSG_LENGTH, false),
                stackTrace = it.stackTraceToString(),
                value = null, // TODO to be populated upon implementing https://r3-cev.atlassian.net/browse/CORDA-3681
                persistedInstant = now
            )
        }
    }

    private fun InvocationContext.getStartedType(): StartReason {
        return when (origin) {
            is InvocationOrigin.RPC, is InvocationOrigin.Shell -> StartReason.RPC
            is InvocationOrigin.Peer -> StartReason.INITIATED
            is InvocationOrigin.Service -> StartReason.SERVICE
            is InvocationOrigin.Scheduled -> StartReason.SCHEDULED
        }
    }

    private fun InvocationContext.getFlowParameters(): List<Any?> {
        // Only RPC flows have parameters which are found in index 1
        return if (arguments.isNotEmpty()) {
            uncheckedCast<Any?, Array<Any?>>(arguments[1]).toList()
        } else {
            emptyList()
        }
    }

    private fun DBFlowCheckpoint.toSerializedCheckpoint(): Checkpoint.Serialized {
        val serialisedFlowState = blob!!.flowStack?.let { SerializedBytes<FlowState>(it) }
        return Checkpoint.Serialized(
            serializedCheckpointState = SerializedBytes(blob!!.checkpoint),
            serializedFlowState = serialisedFlowState,
            // Always load as a [Clean] checkpoint to represent that the checkpoint is the last _good_ checkpoint
            errorState = ErrorState.Clean,
            // A checkpoint with a result should not normally be loaded (it should be [null] most of the time)
            result = result?.let { SerializedBytes<Any>(it.value) },
            status = status,
            progressStep = progressStep,
            flowIoRequest = ioRequestType,
            compatible = compatible
        )
    }

    private class DBPausedFields(
        val id: String,
        val checkpoint: ByteArray = EMPTY_BYTE_ARRAY,
        val status: FlowStatus,
        val progressStep: String?,
        val ioRequestType: String?,
        val compatible: Boolean
    ) {
        fun toSerializedCheckpoint(): Checkpoint.Serialized {
            return Checkpoint.Serialized(
                serializedCheckpointState = SerializedBytes(checkpoint),
                serializedFlowState = null,
                // Always load as a [Clean] checkpoint to represent that the checkpoint is the last _good_ checkpoint
                errorState = ErrorState.Clean,
                result = null,
                status = status,
                progressStep = progressStep,
                flowIoRequest = ioRequestType,
                compatible = compatible
            )
        }
    }

    private fun <T : Any> T.storageSerialize(): SerializedBytes<T> {
        return serialize(context = SerializationDefaults.STORAGE_CONTEXT)
    }

    private fun Checkpoint.isFinished() = when (status) {
        FlowStatus.COMPLETED, FlowStatus.KILLED, FlowStatus.FAILED -> true
        else -> false
    }

    private fun String.truncate(maxLength: Int, withWarnings: Boolean): String {
        var str = this
        if (length > maxLength) {
            if (withWarnings) {
                log.warn("Truncating long string before storing it into the database. String: $str.")
            }
            str = str.substring(0, maxLength)
        }
        return str
    }

    private fun Throwable.stackTraceToString(): String {
        var stackTraceStr = ExceptionUtils.getStackTrace(this)
        if (stackTraceStr.length > MAX_STACKTRACE_LENGTH) {
            // cut off the last line, which will be a half line
            val lineBreak = System.getProperty("line.separator")
            val truncateIndex = stackTraceStr.lastIndexOf(lineBreak, MAX_STACKTRACE_LENGTH - 1)
            stackTraceStr = stackTraceStr.substring(0, truncateIndex + lineBreak.length) // include last line break in
        }
        return stackTraceStr
    }
}
