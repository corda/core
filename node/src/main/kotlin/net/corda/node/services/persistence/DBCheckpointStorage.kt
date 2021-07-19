package net.corda.node.services.persistence

import net.corda.core.context.InvocationContext
import net.corda.core.context.InvocationOrigin
import net.corda.core.flows.StateMachineRunId
import net.corda.core.internal.PLATFORM_VERSION
import net.corda.core.internal.VisibleForTesting
import net.corda.core.internal.uncheckedCast
import net.corda.core.flows.ResultSerializationException
import net.corda.core.serialization.SerializationDefaults
import net.corda.core.serialization.SerializedBytes
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.internal.MissingSerializerException
import net.corda.core.serialization.serialize
import net.corda.core.utilities.contextLogger
import net.corda.node.services.api.CheckpointStorage
import net.corda.node.services.statemachine.Checkpoint
import net.corda.node.services.statemachine.Checkpoint.FlowStatus
import net.corda.node.services.statemachine.CheckpointState
import net.corda.node.services.statemachine.ErrorState
import net.corda.node.services.statemachine.FlowResultMetadata
import net.corda.node.services.statemachine.FlowState
import net.corda.node.services.statemachine.SubFlowVersion
import net.corda.nodeapi.internal.persistence.NODE_DATABASE_PREFIX
import net.corda.nodeapi.internal.persistence.currentDBSession
import org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY
import org.apache.commons.lang3.exception.ExceptionUtils
import org.hibernate.annotations.Type
import java.security.Principal
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
        @VisibleForTesting
        const val MAX_FLOW_IO_REQUEST_LENGTH = 128
        const val MAX_CLIENT_ID_LENGTH = 512

        private val RUNNABLE_CHECKPOINTS = setOf(FlowStatus.RUNNABLE, FlowStatus.HOSPITALIZED)

        // This is a dummy [DBFlowMetadata] object which help us whenever we want to persist a [DBFlowCheckpoint], but not persist its [DBFlowMetadata].
        // [DBFlowCheckpoint] needs to always reference a [DBFlowMetadata] ([DBFlowCheckpoint.flowMetadata] is not nullable).
        // However, since we do not -hibernate- cascade, it does not get persisted into the database.
        private val dummyDBFlowMetadata: DBFlowMetadata = DBFlowMetadata(
                flowId = "dummyFlowId",
                invocationId = "dummyInvocationId",
                flowName = "dummyFlowName",
                userSuppliedIdentifier = "dummyUserSuppliedIdentifier",
                startType = StartReason.INITIATED,
                initialParameters = ByteArray(0),
                launchingCordapp = "dummyLaunchingCordapp",
                platformVersion = -1,
                startedBy = "dummyStartedBy",
                invocationInstant = Instant.now(),
                startInstant = Instant.now(),
                finishInstant = null
        )

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
    data class DBFlowCheckpoint(
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
    data class DBFlowCheckpointBlob(
        @Id
        @Column(name = "flow_id", length = 64, nullable = false)
        var flowId: String,

        @Type(type = "corda-blob")
        @Column(name = "checkpoint_value", nullable = false)
        var checkpoint: ByteArray = EMPTY_BYTE_ARRAY,

        @Type(type = "corda-blob")
        @Column(name = "flow_state", nullable = true)
        var flowStack: ByteArray?,

        @Type(type = "corda-wrapper-binary")
        @Column(name = "hmac")
        var hmac: ByteArray,

        @Column(name = "timestamp")
        var persistedInstant: Instant
    ) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as DBFlowCheckpointBlob

            if (flowId != other.flowId) return false
            if (!checkpoint.contentEquals(other.checkpoint)) return false

            if (!(flowStack ?: EMPTY_BYTE_ARRAY)!!.contentEquals(other.flowStack ?: EMPTY_BYTE_ARRAY)) {
                return false
            }

            if (!hmac.contentEquals(other.hmac)) return false
            if (persistedInstant != other.persistedInstant) return false

            return true
        }

        override fun hashCode(): Int {
            var result = flowId.hashCode()
            result = 31 * result + checkpoint.contentHashCode()
            result = 31 * result + (flowStack?.contentHashCode() ?: 0)
            result = 31 * result + hmac.contentHashCode()
            result = 31 * result + persistedInstant.hashCode()
            return result
        }
    }

    @Entity
    @javax.persistence.Table(name = "${NODE_DATABASE_PREFIX}flow_results")
    data class DBFlowResult(
        @Id
        @Column(name = "flow_id", length = 64, nullable = false)
        var flow_id: String,

        @Type(type = "corda-blob")
        @Column(name = "result_value", nullable = true)
        var value: ByteArray? = null,

        @Column(name = "timestamp")
        val persistedInstant: Instant
    ) {
        @Suppress("ComplexMethod")
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false
            other as DBFlowResult
            if (flow_id != other.flow_id) return false
            val value = value
            val otherValue = other.value
            if (value != null) {
                if (otherValue == null) return false
                if (!value.contentEquals(otherValue)) return false
            } else if (otherValue != null) return false
            if (persistedInstant != other.persistedInstant) return false
            return true
        }

        override fun hashCode(): Int {
            var result = flow_id.hashCode()
            result = 31 * result + (value?.contentHashCode() ?: 0)
            result = 31 * result + persistedInstant.hashCode()
            return result
        }
    }

    @Entity
    @javax.persistence.Table(name = "${NODE_DATABASE_PREFIX}flow_exceptions")
    data class DBFlowException(
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
    ) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as DBFlowException

            if (flow_id != other.flow_id) return false
            if (type != other.type) return false
            if (message != other.message) return false
            if (stackTrace != other.stackTrace) return false
            if (!(value ?: EMPTY_BYTE_ARRAY)!!.contentEquals(other.value ?: EMPTY_BYTE_ARRAY)) {
                return false
            }
            if (persistedInstant != other.persistedInstant) return false

            return true
        }

        override fun hashCode(): Int {
            var result = flow_id.hashCode()
            result = 31 * result + type.hashCode()
            result = 31 * result + (message?.hashCode() ?: 0)
            result = 31 * result + stackTrace.hashCode()
            result = 31 * result + (value?.contentHashCode() ?: 0)
            result = 31 * result + persistedInstant.hashCode()
            return result
        }
    }

    @Entity
    @javax.persistence.Table(name = "${NODE_DATABASE_PREFIX}flow_metadata")
    data class DBFlowMetadata(
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

        @Column(name = "start_time", nullable = false)
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
        serializedFlowState: SerializedBytes<FlowState>?,
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

        val metadata = createDBFlowMetadata(flowId, checkpoint, now)

        val dbFlowException = if (checkpoint.status == FlowStatus.FAILED || checkpoint.status == FlowStatus.HOSPITALIZED) {
            val errored = checkpoint.errorState as? ErrorState.Errored
            errored?.run { createDBFlowException(flowId, errors.last().exception, now) }
                ?: throw IllegalStateException("Found '${checkpoint.status}' checkpoint whose error state is not ${ErrorState.Errored::class.java.simpleName}")
        } else {
            null
        }

        // Most fields are null as they cannot have been set when creating the initial checkpoint
        val dbFlowCheckpoint = DBFlowCheckpoint(
            flowId = flowId,
            blob = blob,
            result = null,
            exceptionDetails = dbFlowException,
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
        dbFlowException?.let { currentDBSession().save(it) }
    }

    @Suppress("ComplexMethod")
    override fun updateCheckpoint(
        id: StateMachineRunId,
        checkpoint: Checkpoint,
        serializedFlowState: SerializedBytes<FlowState>?,
        serializedCheckpointState: SerializedBytes<CheckpointState>
    ) {
        val now = clock.instant()
        val flowId = id.uuid.toString()

        val blob = if (checkpoint.status == FlowStatus.HOSPITALIZED) {
            // Do not update 'checkpointState' or 'flowState' if flow hospitalized
            null
        } else if (checkpoint.status == FlowStatus.FAILED) {
            // We need to update only the 'flowState' to null, and we don't want to update the checkpoint state
            // because we want to retain the last clean checkpoint state, therefore just use a query for that update.
            currentDBSession()
                .createNativeQuery("Update ${NODE_DATABASE_PREFIX}checkpoint_blobs set flow_state = null where flow_id = :flow_id")
                .setParameter("flow_id", flowId)
                .executeUpdate()
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

        val dbFlowResult = if (checkpoint.status == FlowStatus.COMPLETED) {
            try {
                createDBFlowResult(flowId, checkpoint.result, now)
            } catch (e: MissingSerializerException) {
                throw ResultSerializationException(e)
            }
        } else {
            null
        }

        val dbFlowException = if (checkpoint.status == FlowStatus.FAILED || checkpoint.status == FlowStatus.HOSPITALIZED) {
            val errored = checkpoint.errorState as? ErrorState.Errored
            errored?.run { createDBFlowException(flowId, errors.last().exception, now) }
                    ?: throw IllegalStateException("Found '${checkpoint.status}' checkpoint whose error state is not ${ErrorState.Errored::class.java.simpleName}")
        } else {
            null
        }

        // Updates to children entities ([DBFlowCheckpointBlob], [DBFlowResult], [DBFlowException], [DBFlowMetadata]) are not cascaded to children tables.
        val dbFlowCheckpoint = DBFlowCheckpoint(
            flowId = flowId,
            blob = blob,
            result = dbFlowResult,
            exceptionDetails = dbFlowException,
            flowMetadata = dummyDBFlowMetadata, // [DBFlowMetadata] will only update its 'finish_time' when a checkpoint finishes
            status = checkpoint.status,
            compatible = checkpoint.compatible,
            progressStep = checkpoint.progressStep?.take(MAX_PROGRESS_STEP_LENGTH),
            ioRequestType = checkpoint.flowIoRequest?.truncate(MAX_FLOW_IO_REQUEST_LENGTH, false),
            checkpointInstant = now
        )

        currentDBSession().update(dbFlowCheckpoint)
        blob?.let { currentDBSession().update(it) }
        dbFlowResult?.let { currentDBSession().save(it) }
        dbFlowException?.let { currentDBSession().save(it) }
        if (checkpoint.isFinished()) {
            setDBFlowMetadataFinishTime(flowId, now)
        }
    }

    override fun markAllPaused() {
        currentDBSession()
            .createNativeQuery("Update ${NODE_DATABASE_PREFIX}checkpoints set status = :paused_status where status in :runnable_statuses")
            .setParameter("paused_status", FlowStatus.PAUSED.ordinal)
            .setParameter("runnable_statuses", RUNNABLE_CHECKPOINTS.map { it.ordinal })
            .executeUpdate()
    }

    @Suppress("MagicNumber")
    override fun removeCheckpoint(id: StateMachineRunId, mayHavePersistentResults: Boolean): Boolean {
        var deletedRows = 0
        val flowId = id.uuid.toString()
        deletedRows += deleteRow(DBFlowCheckpoint::class.java, DBFlowCheckpoint::flowId.name, flowId)
        deletedRows += deleteRow(DBFlowCheckpointBlob::class.java, DBFlowCheckpointBlob::flowId.name, flowId)
        if (mayHavePersistentResults) {
            deletedRows += deleteRow(DBFlowResult::class.java, DBFlowResult::flow_id.name, flowId)
            deletedRows += deleteRow(DBFlowException::class.java, DBFlowException::flow_id.name, flowId)
        }
        deletedRows += deleteRow(DBFlowMetadata::class.java, DBFlowMetadata::flowId.name, flowId)
        return deletedRows >= 2
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

    private fun getDBFlowResult(id: StateMachineRunId): DBFlowResult? {
        return currentDBSession().find(DBFlowResult::class.java, id.uuid.toString())
    }

    private fun getDBFlowException(id: StateMachineRunId): DBFlowException? {
        return currentDBSession().find(DBFlowException::class.java, id.uuid.toString())
    }

    override fun getPausedCheckpoints(): Stream<Triple<StateMachineRunId, Checkpoint.Serialized, Boolean>> {
        val session = currentDBSession()
        val jpqlQuery = """select new ${DBPausedFields::class.java.name}(checkpoint.id, blob.checkpoint, checkpoint.status,
                checkpoint.progressStep, checkpoint.ioRequestType, checkpoint.compatible, exception.id) 
                from ${DBFlowCheckpoint::class.java.name} checkpoint 
                join ${DBFlowCheckpointBlob::class.java.name} blob on checkpoint.blob = blob.id
                left outer join ${DBFlowException::class.java.name} exception on checkpoint.exceptionDetails = exception.id
                where checkpoint.status = ${FlowStatus.PAUSED.ordinal}""".trimIndent()
        val query = session.createQuery(jpqlQuery, DBPausedFields::class.java)
        return query.resultList.stream().map {
            Triple(StateMachineRunId(UUID.fromString(it.id)), it.toSerializedCheckpoint(), it.wasHospitalized)
        }
    }

    override fun getFinishedFlowsResultsMetadata(): Stream<Pair<StateMachineRunId, FlowResultMetadata>> {
        val session = currentDBSession()
        val jpqlQuery =
            """select new ${DBFlowResultMetadataFields::class.java.name}(
                    checkpoint.id, 
                    checkpoint.status, 
                    metadata.userSuppliedIdentifier, 
                    metadata.startedBy
                ) 
                from ${DBFlowCheckpoint::class.java.name} checkpoint 
                join ${DBFlowMetadata::class.java.name} metadata on metadata.id = checkpoint.flowMetadata  
                where checkpoint.status = ${FlowStatus.COMPLETED.ordinal}
                or checkpoint.status = ${FlowStatus.FAILED.ordinal}
                or checkpoint.status = ${FlowStatus.KILLED.ordinal}""".trimIndent()
        val query = session.createQuery(jpqlQuery, DBFlowResultMetadataFields::class.java)
        return query.resultList.stream().map {
            val startedBy = it.startedBy
            StateMachineRunId(UUID.fromString(it.id)) to FlowResultMetadata(it.status, it.clientId, Principal { startedBy })
        }
    }

    override fun getFlowResult(id: StateMachineRunId, throwIfMissing: Boolean): Any? {
        val dbFlowResult = getDBFlowResult(id)
        if (throwIfMissing && dbFlowResult == null) {
            throw IllegalStateException("Flow's $id result was not found in the database. Something is very wrong.")
        }
        val serializedFlowResult = dbFlowResult?.value?.let { SerializedBytes<Any>(it) }
        return serializedFlowResult?.deserialize(context = SerializationDefaults.STORAGE_CONTEXT)
    }

    override fun getFlowException(id: StateMachineRunId, throwIfMissing: Boolean): Any? {
        val dbFlowException = getDBFlowException(id)
        if (throwIfMissing && dbFlowException == null) {
            throw IllegalStateException("Flow's $id exception was not found in the database. Something is very wrong.")
        }
        val serializedFlowException = dbFlowException?.value?.let { SerializedBytes<Any>(it) }
        return serializedFlowException?.deserialize(context = SerializationDefaults.STORAGE_CONTEXT)
    }

    override fun addFlowException(id: StateMachineRunId, exception: Throwable) {
        currentDBSession().save(createDBFlowException(id.uuid.toString(), exception, clock.instant()))
    }

    override fun removeFlowException(id: StateMachineRunId): Boolean {
        return deleteRow(DBFlowException::class.java, DBFlowException::flow_id.name, id.uuid.toString()) == 1
    }

    override fun updateStatus(runId: StateMachineRunId, flowStatus: FlowStatus) {
        currentDBSession()
            .createNativeQuery("Update ${NODE_DATABASE_PREFIX}checkpoints set status = :status, timestamp = :timestamp where flow_id = :id")
            .setParameter("status", flowStatus.ordinal)
            .setParameter("timestamp", clock.instant())
            .setParameter("id", runId.uuid.toString())
            .executeUpdate()
    }

    override fun updateCompatible(runId: StateMachineRunId, compatible: Boolean) {
        currentDBSession()
            .createNativeQuery("Update ${NODE_DATABASE_PREFIX}checkpoints set compatible = :compatible where flow_id = :flow_id")
            .setParameter("compatible", compatible)
            .setParameter("flow_id", runId.uuid.toString())
            .executeUpdate()
    }

    private fun createDBFlowMetadata(flowId: String, checkpoint: Checkpoint, now: Instant): DBFlowMetadata {
        val context = checkpoint.checkpointState.invocationContext
        val flowInfo = checkpoint.checkpointState.subFlowStack.first()
        return DBFlowMetadata(
            flowId = flowId,
            invocationId = context.trace.invocationId.value,
            // Truncate the flow name to fit into the database column
            // Flow names are unlikely to be this long
            flowName = flowInfo.flowClass.name.take(MAX_FLOW_NAME_LENGTH),
            userSuppliedIdentifier = context.clientId,
            startType = context.getStartedType(),
            initialParameters = context.getFlowParameters().storageSerialize().bytes,
            launchingCordapp = (flowInfo.subFlowVersion as? SubFlowVersion.CorDappFlow)?.corDappName ?: "Core flow",
            platformVersion = PLATFORM_VERSION,
            startedBy = context.principal().name,
            invocationInstant = context.trace.invocationId.timestamp,
            startInstant = now,
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

    private fun createDBFlowResult(flowId: String, result: Any?, now: Instant): DBFlowResult {
        return DBFlowResult(
            flow_id = flowId,
            value = result?.storageSerialize()?.bytes,
            persistedInstant = now
        )
    }

    private fun createDBFlowException(flowId: String, exception: Throwable, now: Instant): DBFlowException {
        return DBFlowException(
            flow_id = flowId,
            type = exception::class.java.name.truncate(MAX_EXC_TYPE_LENGTH, true),
            message = exception.message?.truncate(MAX_EXC_MSG_LENGTH, false),
            stackTrace = exception.stackTraceToString(),
            value = exception.storageSerialize().bytes,
            persistedInstant = now
        )
    }

    private fun setDBFlowMetadataFinishTime(flowId: String, now: Instant) {
        currentDBSession()
            .createNativeQuery("Update ${NODE_DATABASE_PREFIX}flow_metadata set finish_time = :finish_time where flow_id = :flow_id")
            .setParameter("finish_time", now)
            .setParameter("flow_id", flowId)
            .executeUpdate()
    }

    private fun InvocationContext.getStartedType(): StartReason {
        return when (origin) {
            is InvocationOrigin.RPC, is InvocationOrigin.Shell -> StartReason.RPC
            is InvocationOrigin.Peer -> StartReason.INITIATED
            is InvocationOrigin.Service -> StartReason.SERVICE
            is InvocationOrigin.Scheduled -> StartReason.SCHEDULED
        }
    }

    @Suppress("MagicNumber")
    private fun InvocationContext.getFlowParameters(): List<Any?> {
        // Only RPC flows have parameters which are found in index 1 or index 2 (if called with client id)
        return if (arguments!!.isNotEmpty()) {
            arguments!!.run {
                check(size == 2 || size == 3) { "Unexpected argument number provided in rpc call" }
                uncheckedCast<Any?, Array<Any?>>(last()).toList()
            }
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
            result = result?.let { dbFlowResult -> dbFlowResult.value?.let { SerializedBytes<Any>(it) } },
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
        val compatible: Boolean,
        exception: String?
    ) {
        val wasHospitalized = exception != null
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

    private class DBFlowResultMetadataFields(
        val id: String,
        val status: FlowStatus,
        val clientId: String?,
        val startedBy: String
    )

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
