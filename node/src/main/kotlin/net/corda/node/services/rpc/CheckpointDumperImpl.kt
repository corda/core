package net.corda.node.services.rpc

import co.paralleluniverse.fibers.Stack
import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.JsonUnwrapped
import com.fasterxml.jackson.annotation.JsonValue
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.util.DefaultIndenter
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.BeanDescription
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.databind.SerializationConfig
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier
import net.corda.client.jackson.JacksonSupport
import net.corda.client.jackson.internal.jsonObject
import net.corda.core.context.InvocationOrigin
import net.corda.core.contracts.Attachment
import net.corda.core.contracts.ScheduledStateRef
import net.corda.core.contracts.StateRef
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.FlowInfo
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.FlowSession
import net.corda.core.flows.StateMachineRunId
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.FlowAsyncOperation
import net.corda.core.internal.FlowIORequest
import net.corda.core.internal.WaitForLedgerCommit
import net.corda.core.internal.WaitForStateConsumption
import net.corda.core.internal.declaredField
import net.corda.core.internal.div
import net.corda.core.internal.exists
import net.corda.core.internal.objectOrNewInstance
import net.corda.core.internal.outputStream
import net.corda.core.internal.uncheckedCast
import net.corda.core.node.AppServiceHub.Companion.SERVICE_PRIORITY_NORMAL
import net.corda.core.node.ServiceHub
import net.corda.core.serialization.SerializeAsToken
import net.corda.core.serialization.SerializedBytes
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.internal.CheckpointSerializationContext
import net.corda.core.serialization.internal.CheckpointSerializationDefaults
import net.corda.core.serialization.internal.checkpointDeserialize
import net.corda.core.utilities.NonEmptySet
import net.corda.core.utilities.ProgressTracker
import net.corda.core.utilities.Try
import net.corda.core.utilities.contextLogger
import net.corda.node.internal.NodeStartup
import net.corda.node.services.api.CheckpointStorage
import net.corda.node.services.statemachine.Checkpoint
import net.corda.node.services.statemachine.ErrorState
import net.corda.node.services.statemachine.ExistingSessionMessagePayload
import net.corda.node.services.statemachine.FlowSessionImpl
import net.corda.node.services.statemachine.FlowState
import net.corda.node.services.statemachine.FlowStateMachineImpl
import net.corda.node.services.statemachine.SessionId
import net.corda.node.services.statemachine.SessionState
import net.corda.node.services.statemachine.SubFlow
import net.corda.nodeapi.internal.lifecycle.NodeLifecycleEvent
import net.corda.nodeapi.internal.lifecycle.NodeLifecycleObserver
import net.corda.nodeapi.internal.lifecycle.NodeLifecycleObserver.Companion.reportSuccess
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.serialization.internal.CheckpointSerializeAsTokenContextImpl
import net.corda.serialization.internal.withTokenContext
import java.io.InputStream
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.CRC32
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import kotlin.reflect.KProperty1
import kotlin.reflect.full.companionObject
import kotlin.reflect.full.memberProperties
import kotlin.streams.asSequence

class CheckpointDumperImpl(private val checkpointStorage: CheckpointStorage, private val database: CordaPersistence,
                           private val serviceHub: ServiceHub, val baseDirectory: Path,
                           private val cordappDirectories: Iterable<Path>) : NodeLifecycleObserver {
    companion object {
        internal val TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(UTC)
        private val log = contextLogger()
        private val DUMPABLE_CHECKPOINTS = setOf(
                Checkpoint.FlowStatus.RUNNABLE,
                Checkpoint.FlowStatus.HOSPITALIZED,
                Checkpoint.FlowStatus.PAUSED
        )

        private fun writeFiber2Zip(zipOutputStream : ZipOutputStream,
                                   context: CheckpointSerializationContext,
                                   runId: StateMachineRunId,
                                   flowState: FlowState.Started) {
            @Suppress("TooGenericExceptionCaught")
            try {
                flowState.frozenFiber.checkpointDeserialize(context)
            } catch (e: Exception) {
                log.error("Failed to deserialise checkpoint with flowId: ${runId.uuid}", e)
                null
            }?.let { fiber ->
                val zipEntry = ZipEntry("fibers/${fiber.logic.javaClass.name}-${runId.uuid}.fiber").apply {
                    //Fibers can easily be compressed, so they are stored as DEFLATED
                    method = ZipEntry.DEFLATED
                }
                zipOutputStream.putNextEntry(zipEntry)
                zipOutputStream.write(flowState.frozenFiber.bytes)
                zipOutputStream.closeEntry()
            }
        }

        private fun computeSizeAndCrc32(inputStream: InputStream,
                                        buffer : ByteArray) : Pair<Long, Long> {
            val crc32 = CRC32()
            var sz = 0L
            while (true) {
                val read = inputStream.read(buffer)
                if (read < 0) break
                sz += read
                crc32.update(buffer, 0, read)
            }
            return sz to crc32.value
        }

        private fun write2Zip(zip: ZipOutputStream,
                              inputStream: InputStream,
                              buffer : ByteArray) {
            while (true) {
                val read = inputStream.read(buffer)
                if (read < 0) break
                zip.write(buffer, 0, read)
            }
        }

        private fun writeStoredEntry(zip : ZipOutputStream, source : Path, destinationFileName : String, buffer : ByteArray) {
            val zipEntry = ZipEntry(destinationFileName).apply {
                // A stored ZipEntry requires computing the size and CRC32 in advance
                val (sz, crc32) = Files.newInputStream(source).use {
                    computeSizeAndCrc32(it, buffer)
                }
                method = ZipEntry.STORED
                size = sz
                compressedSize = sz
                crc = crc32
            }
            zip.putNextEntry(zipEntry)
            Files.newInputStream(source).use {
                write2Zip(zip, it, buffer)
            }
            zip.closeEntry()
        }
    }

    override val priority: Int = SERVICE_PRIORITY_NORMAL

    private val lock = AtomicInteger(0)

    private lateinit var checkpointSerializationContext: CheckpointSerializationContext
    private lateinit var writer: ObjectWriter

    private val isCheckpointAgentRunning by lazy(::checkpointAgentRunning)

    override fun update(nodeLifecycleEvent: NodeLifecycleEvent): Try<String> {
        return when (nodeLifecycleEvent) {
            is NodeLifecycleEvent.AfterNodeStart<*> -> Try.on {
                checkpointSerializationContext = CheckpointSerializationDefaults.CHECKPOINT_CONTEXT.withTokenContext(
                        CheckpointSerializeAsTokenContextImpl(
                                nodeLifecycleEvent.nodeServicesContext.tokenizableServices,
                                CheckpointSerializationDefaults.CHECKPOINT_SERIALIZER,
                                CheckpointSerializationDefaults.CHECKPOINT_CONTEXT,
                                serviceHub
                        )
                )

                val mapper = JacksonSupport.createNonRpcMapper()
                mapper.registerModule(SimpleModule().apply {
                    setSerializerModifier(CheckpointDumperBeanModifier)
                    addSerializer(FlowSessionImplSerializer)
                    addSerializer(MapSerializer)
                    addSerializer(AttachmentSerializer)
                    setMixInAnnotation(FlowAsyncOperation::class.java, FlowAsyncOperationMixin::class.java)
                    setMixInAnnotation(FlowLogic::class.java, FlowLogicMixin::class.java)
                    setMixInAnnotation(SessionId::class.java, SessionIdMixin::class.java)
                })
                val prettyPrinter = DefaultPrettyPrinter().apply {
                    indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE)
                }
                writer = mapper.writer(prettyPrinter)
                reportSuccess(nodeLifecycleEvent)
            }
            else -> super.update(nodeLifecycleEvent)
        }
    }

    fun dumpCheckpoints() {
        val now = serviceHub.clock.instant()
        val file = baseDirectory / NodeStartup.LOGS_DIRECTORY_NAME / "checkpoints_dump-${TIME_FORMATTER.format(now)}.zip"
        try {
            if (lock.getAndIncrement() == 0 && !file.exists()) {
                database.transaction {
                    checkpointStorage.getCheckpoints(DUMPABLE_CHECKPOINTS).use { stream ->
                        ZipOutputStream(file.outputStream()).use { zip ->
                            stream.forEach { (runId, serialisedCheckpoint) ->

                                if (isCheckpointAgentRunning)
                                    instrumentCheckpointAgent(runId)

                                val (bytes, fileName) = try {
                                    val checkpoint = serialisedCheckpoint.deserialize(checkpointSerializationContext)
                                    val json = checkpoint.toJson(runId.uuid, now)
                                    val jsonBytes = writer.writeValueAsBytes(json)
                                    jsonBytes to "${json.topLevelFlowClass.simpleName}-${runId.uuid}.json"
                                } catch (e: Exception) {
                                    log.info("Failed to deserialise checkpoint with flowId: ${runId.uuid}", e)
                                    val errorBytes = checkpointDeserializationErrorMessage(runId, e).toByteArray()
                                    errorBytes to "Undeserialisable-checkpoint-${runId.uuid}.json"
                                }
                                zip.putNextEntry(ZipEntry(fileName))
                                zip.write(bytes)
                                zip.closeEntry()
                            }
                        }
                    }
                }
            } else {
                log.info("Flow dump already in progress, skipping current call")
            }
        } finally {
            lock.decrementAndGet()
        }
    }

    @Suppress("ComplexMethod")
    fun debugCheckpoints() {
        val now = serviceHub.clock.instant()
        val file = baseDirectory / NodeStartup.LOGS_DIRECTORY_NAME / "checkpoints_debug-${TIME_FORMATTER.format(now)}.zip"
        try {
            if (lock.getAndIncrement() == 0 && !file.exists()) {
                database.transaction {
                    checkpointStorage.getCheckpoints(DUMPABLE_CHECKPOINTS).use { stream ->
                        ZipOutputStream(file.outputStream()).use { zip ->
                            @Suppress("MagicNumber")
                            val buffer = ByteArray(0x10000)

                            //Dump checkpoints in "fibers" folder
                            for((runId, serializedCheckpoint) in stream) {
                                val flowState = serializedCheckpoint.deserialize(checkpointSerializationContext).flowState
                                if(flowState is FlowState.Started) writeFiber2Zip(zip, checkpointSerializationContext, runId, flowState)
                            }

                            val jarFilter = { directoryEntry : Path -> directoryEntry.fileName.toString().endsWith(".jar") }
                            //Dump cordApps jar in the "cordapp" folder
                            for(cordappDirectory in cordappDirectories) {
                                val corDappJars = Files.list(cordappDirectory).filter(jarFilter).asSequence()
                                corDappJars.forEach { corDappJar ->
                                    //Jar files are already compressed, so they are stored in the zip as they are
                                    writeStoredEntry(zip, corDappJar, "cordapps/${corDappJar.fileName}", buffer)
                                }
                            }

                            //Dump all jars contained in the corda.jar in the lib directory and dump all
                            // the driver jars in the driver folder of the node to the driver folder of the dump file
                            val pairs = listOf(
                                "lib" to FileSystems.newFileSystem(
                                        Paths.get(System.getProperty("capsule.jar")), null).getPath("/"),
                                "drivers" to baseDirectory.resolve("drivers")
                            )
                            for((dest, source) in pairs) {
                                Files.list(source).filter(jarFilter).forEach { jarEntry ->
                                    writeStoredEntry(zip, jarEntry, "$dest/${jarEntry.fileName}", buffer)
                                }
                            }
                        }
                    }
                }
            } else {
                log.info("Flow dump already in progress, skipping current call")
            }
        } finally {
            lock.decrementAndGet()
        }
    }

    private fun instrumentCheckpointAgent(checkpointId: StateMachineRunId) {
        log.info("Checkpoint agent diagnostics for checkpointId: $checkpointId")
        try {
            val checkpointHook = Class.forName("net.corda.tools.CheckpointHook").kotlin
            if (checkpointHook.objectInstance == null)
                log.info("Instantiating checkpoint agent object instance")
            val instance = checkpointHook.objectOrNewInstance()
            val checkpointIdField = instance.declaredField<UUID>(instance.javaClass, "checkpointId")
            checkpointIdField.value = checkpointId.uuid
        } catch (e: Exception) {
            log.error("Checkpoint agent instrumentation failed for checkpointId: $checkpointId\n. ${e.message}")
        }
    }

    /**
     * Note that this method dynamically uses [net.corda.tools.CheckpointAgent.running], make sure to keep it up to date with
     * the checkpoint agent source code
     */
    private fun checkpointAgentRunning() = try {
        javaClass.classLoader.loadClass("net.corda.tools.CheckpointAgent").kotlin.companionObject
    } catch (e: ClassNotFoundException) {
        null
    }?.let { cls ->
        @Suppress("UNCHECKED_CAST")
        cls.memberProperties.find { it.name == "running"}
                ?.let {it as KProperty1<Any, Boolean>}
                ?.get(cls.objectInstance!!)
    } ?: false

    private fun Checkpoint.toJson(id: UUID, now: Instant): CheckpointJson {
        val (fiber, flowLogic) = when (flowState) {
            is FlowState.Unstarted -> {
                null to flowState.frozenFlowLogic.checkpointDeserialize(context = checkpointSerializationContext)
            }
            is FlowState.Started -> {
                val fiber = flowState.frozenFiber.checkpointDeserialize(context = checkpointSerializationContext)
                fiber to fiber.logic
            }
            else -> {
                throw IllegalStateException("Only runnable checkpoints with their flow stack are output by the checkpoint dumper")
            }
        }

        val flowCallStack = if (fiber != null) {
            // Poke into Quasar's stack and find the object references to the sub-flows so that we can correctly get the current progress
            // step for each sub-call.
            val stackObjects = fiber.getQuasarStack()
            checkpointState.subFlowStack.map { it.toJson(stackObjects) }
        } else {
            emptyList()
        }

        return CheckpointJson(
                flowId = id,
                topLevelFlowClass = flowLogic.javaClass,
                topLevelFlowLogic = flowLogic,
                flowCallStackSummary = flowCallStack.toSummary(),
                flowCallStack = flowCallStack,
                suspendedOn = (flowState as? FlowState.Started)?.flowIORequest?.toSuspendedOn(
                        timestamp,
                        now
                ),
                origin = checkpointState.invocationContext.origin.toOrigin(),
                ourIdentity = checkpointState.ourIdentity,
                activeSessions = checkpointState.sessions.mapNotNull { it.value.toActiveSession(it.key) },
                // This can only ever return as [ErrorState.Clean] which causes it to become [null]
                errored = errorState as? ErrorState.Errored
        )
    }

    private fun checkpointDeserializationErrorMessage(
            checkpointId: StateMachineRunId,
            exception: Exception
    ): String {
        return """
                *** Unable to deserialise checkpoint: ${exception.message} ***
                *** Check logs for further information, checkpoint flowId: ${checkpointId.uuid} ***
                """
                .trimIndent()
    }

    private fun FlowStateMachineImpl<*>.getQuasarStack() =
            declaredField<Stack>("stack").value.declaredField<Array<*>>("dataObject").value

    private fun SubFlow.toJson(stackObjects: Array<*>): FlowCall {
        val subFlowLogic = stackObjects.find(flowClass::isInstance) as? FlowLogic<*>
        val currentStep = subFlowLogic?.progressTracker?.currentStep
        return FlowCall(
                flowClass = flowClass,
                progressStep = if (currentStep == ProgressTracker.UNSTARTED) null else currentStep?.label,
                flowLogic = subFlowLogic
        )
    }

    private fun List<FlowCall>.toSummary() = map {
        FlowCallSummary(
                it.flowClass,
                it.progressStep
        )
    }

    @Suppress("unused")
    private class FlowCallSummary(
            val flowClass: Class<*>,
            val progressStep: String?
    )

    @Suppress("unused")
    private class FlowCall(
            val flowClass: Class<*>,
            val progressStep: String?,
            val flowLogic: FlowLogic<*>?
    )

    @Suppress("unused")
    @JsonInclude(Include.NON_NULL)
    private class Origin(
            val rpc: String? = null,
            val peer: CordaX500Name? = null,
            val service: String? = null,
            val scheduled: ScheduledStateRef? = null,
            val shell: InvocationOrigin.Shell? = null
    )

    private fun InvocationOrigin.toOrigin(): Origin {
        return when (this) {
            is InvocationOrigin.RPC -> Origin(rpc = actor.id.value)
            is InvocationOrigin.Peer -> Origin(peer = party)
            is InvocationOrigin.Service -> Origin(service = serviceClassName)
            is InvocationOrigin.Scheduled -> Origin(scheduled = scheduledState)
            is InvocationOrigin.Shell -> Origin(shell = this)
        }
    }

    @Suppress("unused")
    private class CheckpointJson(
            val flowId: UUID,
            val topLevelFlowClass: Class<FlowLogic<*>>,
            val topLevelFlowLogic: FlowLogic<*>,
            val flowCallStackSummary: List<FlowCallSummary>,
            val suspendedOn: SuspendedOn?,
            val flowCallStack: List<FlowCall>,
            val origin: Origin,
            val ourIdentity: Party,
            val activeSessions: List<ActiveSession>,
            val errored: ErrorState.Errored?
    )

    @Suppress("unused")
    @JsonInclude(Include.NON_NULL)
    private class SuspendedOn(
            val send: List<SendJson>? = null,
            val receive: NonEmptySet<FlowSession>? = null,
            val sendAndReceive: List<SendJson>? = null,
            val closeSessions: NonEmptySet<FlowSession>? = null,
            val waitForLedgerCommit: SecureHash? = null,
            val waitForStateConsumption: Set<StateRef>? = null,
            val getFlowInfo: NonEmptySet<FlowSession>? = null,
            val sleepTill: Instant? = null,
            val waitForSessionConfirmations: FlowIORequest.WaitForSessionConfirmations? = null,
            val customOperation: FlowIORequest.ExecuteAsyncOperation<*>? = null,
            val forceCheckpoint: FlowIORequest.ForceCheckpoint? = null
    ) {
        @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'", timezone = "UTC")
        lateinit var suspendedTimestamp: Instant
        var secondsSpentWaiting: Long = 0
    }

    @Suppress("unused")
    private class SendJson(val session: FlowSession, val sentPayloadType: Class<*>, val sentPayload: Any)

    private fun FlowIORequest<*>.toSuspendedOn(suspendedTimestamp: Instant, now: Instant): SuspendedOn {
        fun Map<FlowSession, SerializedBytes<Any>>.toJson(): List<SendJson> {
            return map {
                val payload = it.value.deserializeOrOutputPlaceholder()
                SendJson(it.key, payload.javaClass, payload)
            }
        }
        return when (this) {
            is FlowIORequest.Send -> SuspendedOn(send = sessionToMessage.toJson())
            is FlowIORequest.Receive -> SuspendedOn(receive = sessions)
            is FlowIORequest.SendAndReceive -> SuspendedOn(sendAndReceive = sessionToMessage.toJson())
            is FlowIORequest.CloseSessions -> SuspendedOn(closeSessions = sessions)
            is FlowIORequest.GetFlowInfo -> SuspendedOn(getFlowInfo = sessions)
            is FlowIORequest.Sleep -> SuspendedOn(sleepTill = wakeUpAfter)
            is FlowIORequest.WaitForSessionConfirmations -> SuspendedOn(waitForSessionConfirmations = this)
            is FlowIORequest.ForceCheckpoint -> SuspendedOn(forceCheckpoint = this)
            is FlowIORequest.ExecuteAsyncOperation -> {
                when (operation) {
                    is WaitForStateConsumption -> SuspendedOn(waitForStateConsumption = (operation as WaitForStateConsumption).stateRefs)
                    is WaitForLedgerCommit -> SuspendedOn(waitForLedgerCommit = (operation as WaitForLedgerCommit).hash)
                    else -> SuspendedOn(customOperation = this)
                }
            }
        }.also {
            it.suspendedTimestamp = suspendedTimestamp
            it.secondsSpentWaiting = TimeUnit.MILLISECONDS.toSeconds(Duration.between(suspendedTimestamp, now).toMillis())
        }
    }

    private fun SerializedBytes<Any>.deserializeOrOutputPlaceholder() = try {
        deserialize()
    } catch (e: Exception) {
        "*** Unable to deserialise message payload: ${e.message} ***"
    }

    @Suppress("unused")
    private class ActiveSession(
            val peer: Party,
            val ourSessionId: SessionId,
            val receivedMessages: List<ExistingSessionMessagePayload>,
            val peerFlowInfo: FlowInfo,
            val peerSessionId: SessionId?
    )

    private fun SessionState.toActiveSession(sessionId: SessionId): ActiveSession? {
        return if (this is SessionState.Initiated) {
            ActiveSession(peerParty, sessionId, receivedMessages, peerFlowInfo, peerSinkSessionId)
        } else {
            null
        }
    }

    @Suppress("unused")
    private interface SessionIdMixin {
        @get:JsonValue
        val toLong: Long
    }

    @Suppress("unused")
    private interface FlowAsyncOperationMixin {
        @get:JsonIgnore
        val serviceHub: ServiceHub

        // [Any] used so this single mixin can serialize [FlowExternalOperation] and [FlowExternalAsyncOperation]
        @get:JsonUnwrapped
        val operation: Any
    }

    @JsonAutoDetect(getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
    private interface FlowLogicMixin

    private object CheckpointDumperBeanModifier : BeanSerializerModifier() {
        override fun changeProperties(config: SerializationConfig,
                                      beanDesc: BeanDescription,
                                      beanProperties: MutableList<BeanPropertyWriter>): MutableList<BeanPropertyWriter> {
            if (SerializeAsToken::class.java.isAssignableFrom(beanDesc.beanClass)) {
                // Do not serialise node singletons
                // TODO This will cause the singleton to appear as an empty object. Ideally we don't want it to appear at all but this will
                // have to do for now.
                beanProperties.clear()
            } else if (FlowLogic::class.java.isAssignableFrom(beanDesc.beanClass)) {
                beanProperties.removeIf {
                    it.type.isTypeOrSubTypeOf(ProgressTracker::class.java) || it.name == "_stateMachine" || it.name == "deprecatedPartySessionMap"
                }
            }
            return beanProperties
        }
    }

    private object FlowSessionImplSerializer : JsonSerializer<FlowSessionImpl>() {
        override fun serialize(value: FlowSessionImpl, gen: JsonGenerator, serializers: SerializerProvider) {
            gen.jsonObject {
                writeObjectField("peer", value.counterparty)
                writeObjectField("ourSessionId", value.sourceSessionId)
            }
        }

        override fun handledType(): Class<FlowSessionImpl> = FlowSessionImpl::class.java
    }

    private object AttachmentSerializer : JsonSerializer<Attachment>() {
        override fun serialize(value: Attachment, gen: JsonGenerator, serializers: SerializerProvider) = gen.writeObject(value.id)
        override fun handledType(): Class<Attachment> = Attachment::class.java
    }

    private object MapSerializer : JsonSerializer<Map<Any, Any>>() {
        override fun serialize(map: Map<Any, Any>, gen: JsonGenerator, serializers: SerializerProvider) {
            gen.writeStartArray(map.size)
            map.forEach { (key, value) ->
                gen.jsonObject {
                    writeObjectField("key", key)
                    writeObjectField("value", value)
                }
            }
            gen.writeEndArray()
        }

        override fun handledType(): Class<Map<Any, Any>> = uncheckedCast(Map::class.java)
    }
}