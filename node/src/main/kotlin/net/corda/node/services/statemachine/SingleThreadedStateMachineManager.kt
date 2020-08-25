package net.corda.node.services.statemachine

import co.paralleluniverse.fibers.Fiber
import co.paralleluniverse.fibers.FiberExecutorScheduler
import co.paralleluniverse.fibers.instrument.JavaAgent
import co.paralleluniverse.strands.channels.Channel
import com.codahale.metrics.Gauge
import com.google.common.util.concurrent.ThreadFactoryBuilder
import net.corda.core.concurrent.CordaFuture
import net.corda.core.context.InvocationContext
import net.corda.core.flows.FlowException
import net.corda.core.flows.FlowInfo
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.StateMachineRunId
import net.corda.core.identity.Party
import net.corda.core.internal.FlowStateMachine
import net.corda.core.internal.FlowStateMachineHandle
import net.corda.core.internal.PLATFORM_VERSION
import net.corda.core.internal.VisibleForTesting
import net.corda.core.internal.bufferUntilSubscribed
import net.corda.core.internal.castIfPossible
import net.corda.core.internal.concurrent.OpenFuture
import net.corda.core.internal.concurrent.doneFuture
import net.corda.core.internal.concurrent.map
import net.corda.core.internal.concurrent.openFuture
import net.corda.core.internal.mapNotNull
import net.corda.core.internal.uncheckedCast
import net.corda.core.messaging.DataFeed
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.internal.CheckpointSerializationContext
import net.corda.core.serialization.internal.CheckpointSerializationDefaults
import net.corda.core.utilities.ProgressTracker
import net.corda.core.utilities.Try
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.debug
import net.corda.node.internal.InitiatedFlowFactory
import net.corda.node.services.api.CheckpointStorage
import net.corda.node.services.api.ServiceHubInternal
import net.corda.node.services.messaging.DeduplicationHandler
import net.corda.node.services.statemachine.FlowStateMachineImpl.Companion.currentStateMachine
import net.corda.node.services.statemachine.interceptors.DumpHistoryOnErrorInterceptor
import net.corda.node.services.statemachine.interceptors.HospitalisingInterceptor
import net.corda.node.services.statemachine.interceptors.PrintingInterceptor
import net.corda.node.utilities.AffinityExecutor
import net.corda.node.utilities.injectOldProgressTracker
import net.corda.node.utilities.isEnabledTimedFlow
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.nodeapi.internal.persistence.wrapWithDatabaseTransaction
import net.corda.serialization.internal.CheckpointSerializeAsTokenContextImpl
import net.corda.serialization.internal.withTokenContext
import org.apache.activemq.artemis.utils.ReusableLatch
import rx.Observable
import java.security.SecureRandom
import java.util.ArrayList
import java.util.HashSet
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import javax.annotation.concurrent.ThreadSafe
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.set
import kotlin.streams.toList

/**
 * The StateMachineManagerImpl will always invoke the flow fibers on the given [AffinityExecutor], regardless of which
 * thread actually starts them via [deliverExternalEvent].
 */
@ThreadSafe
internal class SingleThreadedStateMachineManager(
        val serviceHub: ServiceHubInternal,
        private val checkpointStorage: CheckpointStorage,
        val executor: ExecutorService,
        val database: CordaPersistence,
        private val secureRandom: SecureRandom,
        private val unfinishedFibers: ReusableLatch = ReusableLatch(),
        private val classloader: ClassLoader = SingleThreadedStateMachineManager::class.java.classLoader
) : StateMachineManager, StateMachineManagerInternal {
    companion object {
        private val logger = contextLogger()

        private val VALID_KILL_FLOW_STATUSES = setOf(
            Checkpoint.FlowStatus.RUNNABLE,
            Checkpoint.FlowStatus.FAILED,
            Checkpoint.FlowStatus.COMPLETED,
            Checkpoint.FlowStatus.HOSPITALIZED,
            Checkpoint.FlowStatus.PAUSED
        )

        @VisibleForTesting
        var beforeClientIDCheck: (() -> Unit)? = null
        @VisibleForTesting
        var onClientIDNotFound: (() -> Unit)? = null
        @VisibleForTesting
        var onCallingStartFlowInternal: (() -> Unit)? = null
        @VisibleForTesting
        var onStartFlowInternalThrewAndAboutToRemove: (() -> Unit)? = null
    }

    private val innerState = StateMachineInnerStateImpl()
    private val scheduler = FiberExecutorScheduler("Same thread scheduler", executor)
    private val scheduledFutureExecutor = Executors.newSingleThreadScheduledExecutor(
            ThreadFactoryBuilder().setNameFormat("flow-scheduled-future-thread").setDaemon(true).build()
    )
    // How many Fibers are running (this includes suspended flows). If zero and stopping is true, then we are halted.
    private val liveFibers = ReusableLatch()
    // Monitoring support.
    private val metrics = serviceHub.monitoringService.metrics
    private val sessionToFlow = ConcurrentHashMap<SessionId, StateMachineRunId>()
    private val flowMessaging: FlowMessaging = FlowMessagingImpl(serviceHub)
    private val actionFutureExecutor = ActionFutureExecutor(innerState, serviceHub, scheduledFutureExecutor)
    private val flowTimeoutScheduler = FlowTimeoutScheduler(innerState, scheduledFutureExecutor, serviceHub)
    private val ourSenderUUID = serviceHub.networkService.ourSenderUUID

    private lateinit var checkpointSerializationContext: CheckpointSerializationContext
    private lateinit var flowCreator: FlowCreator

    override val flowHospital: StaffedFlowHospital = makeFlowHospital()
    private val awaitLocks = HashMap<StateMachineRunId, Pair<Checkpoint.FlowStatus, OpenFuture<Checkpoint.FlowStatus>>>()
    private val transitionExecutor = makeTransitionExecutor()
    private val reloadCheckpointAfterSuspend = serviceHub.configuration.reloadCheckpointAfterSuspend

    override val allStateMachines: List<FlowLogic<*>>
        get() = innerState.withLock { flows.values.map { it.fiber.logic } }

    private val totalStartedFlows = metrics.counter("Flows.Started")
    private val totalFinishedFlows = metrics.counter("Flows.Finished")

    private inline fun <R> Flow<R>.withFlowLock(
        validStatuses: Set<Checkpoint.FlowStatus>,
        block: FlowStateMachineImpl<R>.() -> Boolean
    ): Boolean {
        if (!fiber.hasValidStatus(validStatuses)) return false
        return fiber.withFlowLock {
            // Get the flow again, in case another thread removed it from the map
            innerState.withLock {
                flows[id]?.run {
                    if (!fiber.hasValidStatus(validStatuses)) return false
                    block(uncheckedCast(this.fiber))
                }
            } ?: false
        }
    }

    private fun FlowStateMachineImpl<*>.hasValidStatus(validStatuses: Set<Checkpoint.FlowStatus>): Boolean {
        return transientState.checkpoint.status in validStatuses
    }

    override val protocolVersion: Int = PLATFORM_VERSION

    // would be good to return the event it processed, but this is more difficult because we need a way to extract this info
    override fun await(id: StateMachineRunId, status: Checkpoint.FlowStatus): CordaFuture<Checkpoint.FlowStatus> {
        innerState.withLock { flows[id] } ?: return openFuture<Checkpoint.FlowStatus>().apply {
            setException(IllegalArgumentException("Flow with $id does not exist"))
        }
        return openFuture<Checkpoint.FlowStatus>().also { awaitLocks[id] = status to it }
    }

    override fun release(id: StateMachineRunId): Boolean {
        val flow = innerState.withLock { flows[id] } ?: return false
        return awaitLocks[id]?.let { (_, future) ->
            if (future.isDone) {
                flow.fiber.transientState.lock.release()
                true
            } else {
                false
            }
        } ?: false
    }

//    // would be good to return the event it processed, but this is more difficult because we need a way to extract this info
//    fun await(id: StateMachineRunId, status: Checkpoint.FlowStatus, timeout: Duration): CordaFuture<Checkpoint.FlowStatus> {
//        val flow = innerState.withLock { flows[id] } ?: return openFuture<Checkpoint.FlowStatus>().apply {
//            setException(IllegalArgumentException("Flow with $id does not exist"))
//        }
//        // need to be able to return it, but this means i need to complete it on another thread (therefore do i need an executor)
//        // otherwise i should not be using a future
//        val future = openFuture<>()
//        do {
//            flow.fiber.transientState.run {
//                lock.drainPermits().acquire()
//                checkpoint.status.let {
//                    if (it == status) {
//                        return doneFuture(status)
//                    }
//                }
//                lock.release()
//            }
//        } while (flow.fiber.transientState.checkpoint.status != status)
//    }

    /**
     * An observable that emits triples of the changing flow, the type of change, and a process-specific ID number
     * which may change across restarts.
     *
     * We use assignment here so that multiple subscribers share the same wrapped Observable.
     */
    override val changes: Observable<StateMachineManager.Change> = innerState.changesPublisher

    @Suppress("ComplexMethod")
    override fun start(tokenizableServices: List<Any>, startMode: StateMachineManager.StartMode): CordaFuture<Unit> {
        checkQuasarJavaAgentPresence()
        val checkpointSerializationContext = CheckpointSerializationDefaults.CHECKPOINT_CONTEXT.withTokenContext(
                CheckpointSerializeAsTokenContextImpl(
                        tokenizableServices,
                        CheckpointSerializationDefaults.CHECKPOINT_SERIALIZER,
                        CheckpointSerializationDefaults.CHECKPOINT_CONTEXT,
                        serviceHub
                )
        )
        this.checkpointSerializationContext = checkpointSerializationContext
        val actionExecutor = makeActionExecutor(checkpointSerializationContext)
        when (startMode) {
            StateMachineManager.StartMode.ExcludingPaused -> {}
            StateMachineManager.StartMode.Safe -> markAllFlowsAsPaused()
        }
        this.flowCreator = FlowCreator(
            checkpointSerializationContext,
            checkpointStorage,
            scheduler,
            database,
            transitionExecutor,
            actionExecutor,
            secureRandom,
            serviceHub,
            unfinishedFibers,
            flowTimeoutScheduler::resetCustomTimeout
        )

        val (fibers, pausedFlows) = restoreFlowsFromCheckpoints()
        metrics.register("Flows.InFlight", Gauge<Int> { innerState.flows.size })

        setFlowDefaultUncaughtExceptionHandler()

        innerState.withLock {
            this.pausedFlows.putAll(pausedFlows)
            for ((id, flow) in pausedFlows) {
                val checkpoint = flow.checkpoint
                for (sessionId in getFlowSessionIds(checkpoint)) {
                    sessionToFlow[sessionId] = id
                }
            }
        }

        // - Incompatible checkpoints need to be handled upon implementing CORDA-3897
        for (flow in fibers.values) {
            flow.fiber.clientId?.let {
                innerState.clientIdsToFlowIds[it] = FlowWithClientIdStatus.Active(doneFuture(flow.fiber))
            }
        }

        for (pausedFlow in pausedFlows) {
            pausedFlow.value.checkpoint.checkpointState.invocationContext.clientId?.let {
                innerState.clientIdsToFlowIds[it] = FlowWithClientIdStatus.Active(
                    doneClientIdFuture(pausedFlow.key, pausedFlow.value.resultFuture, it)
                )
            }
        }

        val finishedFlowsResults = checkpointStorage.getFinishedFlowsResultsMetadata().toList()
        for ((id, finishedFlowResult) in finishedFlowsResults) {
            finishedFlowResult.clientId?.let {
                if (finishedFlowResult.status == Checkpoint.FlowStatus.COMPLETED) {
                    innerState.clientIdsToFlowIds[it] = FlowWithClientIdStatus.Removed(id, true)
                } else {
                    innerState.clientIdsToFlowIds[it] = FlowWithClientIdStatus.Removed(id, false)
                }
            } ?: logger.error("Found finished flow $id without a client id. Something is very wrong and this flow will be ignored.")
        }

        return serviceHub.networkMapCache.nodeReady.map {
            logger.info("Node ready, info: ${serviceHub.myInfo}")
            resumeRestoredFlows(fibers)
            flowMessaging.start { _, deduplicationHandler ->
                executor.execute {
                    deliverExternalEvent(deduplicationHandler.externalCause)
                }
            }
        }
    }

    private fun setFlowDefaultUncaughtExceptionHandler() {
        Fiber.setDefaultUncaughtExceptionHandler(
            FlowDefaultUncaughtExceptionHandler(
                flowHospital,
                checkpointStorage,
                database,
                scheduledFutureExecutor
            )
        )
    }

    override fun snapshot(): Set<FlowStateMachineImpl<*>> = innerState.flows.values.map { it.fiber }.toSet()

    override fun <A : FlowLogic<*>> findStateMachines(flowClass: Class<A>): List<Pair<A, CordaFuture<*>>> {
        return innerState.withLock {
            flows.values.mapNotNull {
                flowClass.castIfPossible(it.fiber.logic)?.let { it to it.stateMachine.resultFuture }
            }
        }
    }

    /**
     * Start the shutdown process, bringing the [SingleThreadedStateMachineManager] to a controlled stop.  When this method returns,
     * all Fibers have been suspended and checkpointed, or have completed.
     *
     * @param allowedUnsuspendedFiberCount Optional parameter is used in some tests.
     */
    override fun stop(allowedUnsuspendedFiberCount: Int) {
        require(allowedUnsuspendedFiberCount >= 0){"allowedUnsuspendedFiberCount must be greater than or equal to zero"}
        innerState.withLock {
            if (stopping) throw IllegalStateException("Already stopping!")
            stopping = true
            for ((_, flow) in flows) {
                flow.fiber.scheduleEvent(Event.SoftShutdown)
            }
        }
        // Account for any expected Fibers in a test scenario.
        liveFibers.countDown(allowedUnsuspendedFiberCount)
        liveFibers.await()
        flowHospital.close()
        scheduledFutureExecutor.shutdown()
        scheduler.shutdown()
    }

    /**
     * Atomic get snapshot + subscribe. This is needed so we don't miss updates between subscriptions to [changes] and
     * calls to [allStateMachines]
     */
    override fun track(): DataFeed<List<FlowLogic<*>>, StateMachineManager.Change> {
        return innerState.withMutex {
            database.transaction {
                DataFeed(flows.values.map { it.fiber.logic }, changesPublisher.bufferUntilSubscribed().wrapWithDatabaseTransaction(database))
            }
        }
    }

    @Suppress("ComplexMethod")
    private fun <A> startFlow(
            flowId: StateMachineRunId,
            flowLogic: FlowLogic<A>,
            context: InvocationContext,
            ourIdentity: Party?,
            deduplicationHandler: DeduplicationHandler?
    ): CordaFuture<out FlowStateMachineHandle<A>> {
        beforeClientIDCheck?.invoke()

        var newFuture: OpenFuture<FlowStateMachineHandle<A>>? = null

        val clientId = context.clientId
        if (clientId != null) {
            var existingStatus: FlowWithClientIdStatus? = null
            innerState.withLock {
                clientIdsToFlowIds.compute(clientId) { _, status ->
                    if (status != null) {
                        existingStatus = status
                        status
                    } else {
                        newFuture = openFuture()
                        FlowWithClientIdStatus.Active(newFuture!!)
                    }
                }
            }

            // Flow -started with client id- already exists, return the existing's flow future and don't start a new flow.
            existingStatus?.let {
                val existingFuture = activeOrRemovedClientIdFuture(it, clientId)
                return@startFlow uncheckedCast(existingFuture)
            }
            onClientIDNotFound?.invoke()
        }

        return try {
            startFlowInternal(
                flowId,
                invocationContext = context,
                flowLogic = flowLogic,
                flowStart = FlowStart.Explicit,
                ourIdentity = ourIdentity ?: ourFirstIdentity,
                deduplicationHandler = deduplicationHandler
            ).also {
                newFuture?.captureLater(uncheckedCast(it))
            }
        } catch (t: Throwable) {
            onStartFlowInternalThrewAndAboutToRemove?.invoke()
            innerState.withLock {
                clientIdsToFlowIds.remove(clientId)
                newFuture?.setException(t)
            }
            // Throwing the exception plain here is the same as to return an exceptionally completed future since the caller calls
            // getOrThrow() on the returned future at [CordaRPCOpsImpl.startFlow].
            throw t
        }
    }

    override fun killFlow(id: StateMachineRunId): Boolean {
        val flow = innerState.withLock { flows[id] }
        val killFlowResult = if (flow != null) {
            flow.withFlowLock(VALID_KILL_FLOW_STATUSES) {
                logger.info("Killing flow $id known to this node.")
                // The checkpoint and soft locks are removed here instead of relying on the processing of the next event after setting
                // the killed flag. This is to ensure a flow can be removed from the database, even if it is stuck in a infinite loop.
                database.transaction {
                    checkpointStorage.removeCheckpoint(id, mayHavePersistentResults = true)
                    serviceHub.vaultService.softLockRelease(id.uuid)
                }

                unfinishedFibers.countDown()

                flow.fiber.transientState = flow.fiber.transientState.copy(isKilled = true)
                scheduleEvent(Event.DoRemainingWork)
                true
            }
        } else {
            // It may be that the id refers to a checkpoint that couldn't be deserialised into a flow, so we delete it if it exists.
            database.transaction { checkpointStorage.removeCheckpoint(id, mayHavePersistentResults = true) }
        }

        return killFlowResult || flowHospital.dropSessionInit(id)
    }

    private fun markAllFlowsAsPaused() {
        return checkpointStorage.markAllPaused()
    }

    override fun addSessionBinding(flowId: StateMachineRunId, sessionId: SessionId) {
        val previousFlowId = sessionToFlow.put(sessionId, flowId)
        if (previousFlowId != null) {
            if (previousFlowId == flowId) {
                logger.warn("Session binding from $sessionId to $flowId re-added")
            } else {
                throw IllegalStateException(
                        "Attempted to add session binding from session $sessionId to flow $flowId, " +
                                "however there was already a binding to $previousFlowId"
                )
            }
        }
    }

    override fun removeSessionBindings(sessionIds: Set<SessionId>) {
        val reRemovedSessionIds = HashSet<SessionId>()
        for (sessionId in sessionIds) {
            val flowId = sessionToFlow.remove(sessionId)
            if (flowId == null) {
                reRemovedSessionIds.add(sessionId)
            }
        }
        if (reRemovedSessionIds.isNotEmpty()) {
            logger.warn("Session binding from $reRemovedSessionIds re-removed")
        }
    }

    override fun removeFlow(flowId: StateMachineRunId, removalReason: FlowRemovalReason, lastState: StateMachineState) {
        innerState.withLock {
            flowTimeoutScheduler.cancel(flowId)
            lastState.cancelFutureIfRunning()
            val flow = flows.remove(flowId)
            if (flow != null) {
                decrementLiveFibers()
                totalFinishedFlows.inc()
                return when (removalReason) {
                    is FlowRemovalReason.OrderlyFinish -> removeFlowOrderly(flow, removalReason, lastState)
                    is FlowRemovalReason.ErrorFinish -> removeFlowError(flow, removalReason, lastState)
                    FlowRemovalReason.SoftShutdown -> flow.fiber.scheduleEvent(Event.SoftShutdown)
                }
            } else {
                logger.warn("Flow $flowId re-finished")
            }
        }
    }

    override fun signalFlowHasStarted(flowId: StateMachineRunId) {
        innerState.withLock {
            startedFutures.remove(flowId)?.set(Unit)
            flows[flowId]?.let { flow ->
                changesPublisher.onNext(StateMachineManager.Change.Add(flow.fiber.logic))
            }
        }
    }

    private fun checkQuasarJavaAgentPresence() {
        check(JavaAgent.isActive()) {
            "Missing the '-javaagent' JVM argument. Make sure you run the tests with the Quasar java agent attached to your JVM."
        }
    }

    private fun decrementLiveFibers() {
        liveFibers.countDown()
    }

    private fun incrementLiveFibers() {
        liveFibers.countUp()
    }

    @Suppress("ComplexMethod")
    private fun restoreFlowsFromCheckpoints(): Pair<MutableMap<StateMachineRunId, Flow<*>>, MutableMap<StateMachineRunId, NonResidentFlow>> {
        val flows = mutableMapOf<StateMachineRunId, Flow<*>>()
        val pausedFlows = mutableMapOf<StateMachineRunId, NonResidentFlow>()
        checkpointStorage.getCheckpointsToRun().forEach Checkpoints@{(id, serializedCheckpoint) ->
            // If a flow is added before start() then don't attempt to restore it
            innerState.withLock { if (id in flows) return@Checkpoints }
            val checkpoint = tryDeserializeCheckpoint(serializedCheckpoint, id)?.also {
                if (it.status == Checkpoint.FlowStatus.HOSPITALIZED) {
                    if (checkpointStorage.removeFlowException(id)) {
                        checkpointStorage.updateStatus(id, Checkpoint.FlowStatus.RUNNABLE)
                    } else {
                        logger.error("Unable to remove database exception for flow $id. Something is very wrong. The flow will not be loaded and run.")
                        return@Checkpoints
                    }
                }
            } ?: return@Checkpoints
            val flow = flowCreator.createFlowFromCheckpoint(id, checkpoint)
            if (flow == null) {
                // Set the flowState to paused so we don't waste memory storing it anymore.
                pausedFlows[id] = NonResidentFlow(id, checkpoint.copy(flowState = FlowState.Paused), resumable = false)
            } else {
                flows[id] = flow
            }
        }
        checkpointStorage.getPausedCheckpoints().forEach Checkpoints@{ (id, serializedCheckpoint) ->
            val checkpoint = tryDeserializeCheckpoint(serializedCheckpoint, id) ?: return@Checkpoints
            pausedFlows[id] = NonResidentFlow(id, checkpoint)
        }
        return Pair(flows, pausedFlows)
    }

    private fun resumeRestoredFlows(flows: Map<StateMachineRunId, Flow<*>>) {
        for ((id, flow) in flows.entries) {
            addAndStartFlow(id, flow)
        }
    }

    @Suppress("TooGenericExceptionCaught", "ComplexMethod", "MaxLineLength") // this is fully intentional here, see comment in the catch clause
    override fun retryFlowFromSafePoint(currentState: StateMachineState) {
        currentState.cancelFutureIfRunning()
        // Get set of external events
        val flowId = currentState.flowLogic.runId
        val oldFlowLeftOver = innerState.withLock { flows[flowId] }?.fiber?.transientValues?.eventQueue
        if (oldFlowLeftOver == null) {
            logger.error("Unable to find flow for flow $flowId. Something is very wrong. The flow will not retry.")
            return
        }
        val flow = if (currentState.isAnyCheckpointPersisted) {
            // We intentionally grab the checkpoint from storage rather than relying on the one referenced by currentState. This is so that
            // we mirror exactly what happens when restarting the node.
            val checkpoint = database.transaction {
                val serializedCheckpoint = checkpointStorage.getCheckpoint(flowId)
                if (serializedCheckpoint == null) {
                    logger.error("Unable to find database checkpoint for flow $flowId. Something is very wrong. The flow will not retry.")
                    return@transaction null
                }

                tryDeserializeCheckpoint(serializedCheckpoint, flowId)?.also {
                    if (it.status == Checkpoint.FlowStatus.HOSPITALIZED) {
                        if (checkpointStorage.removeFlowException(flowId)) {
                            checkpointStorage.updateStatus(flowId, Checkpoint.FlowStatus.RUNNABLE)
                        } else {
                            logger.error("Unable to remove database exception for flow $flowId. Something is very wrong. The flow will not be loaded and run.")
                            return@transaction null
                        }
                    }
                } ?: return@transaction null
            } ?: return

            // Resurrect flow
            flowCreator.createFlowFromCheckpoint(
                flowId,
                checkpoint,
                currentState.reloadCheckpointAfterSuspendCount,
                currentState.lock,
                firstRestore = false
            ) ?: return
        } else {
            // Just flow initiation message
            null
        }
        innerState.withLock {
            if (stopping) {
                return
            }
            // Remove any sessions the old flow has.
            for (sessionId in getFlowSessionIds(currentState.checkpoint)) {
                sessionToFlow.remove(sessionId)
            }
            if (flow != null) {
                injectOldProgressTracker(currentState.flowLogic.progressTracker, flow.fiber.logic)
                addAndStartFlow(flowId, flow)
            }
            extractAndScheduleEventsForRetry(oldFlowLeftOver, currentState)
        }
    }

    /**
     * Extract all the [ExternalEvent] from this flows event queue and queue them (in the correct order) in the PausedFlow.
     * This differs from [extractAndScheduleEventsForRetry] which also extracts (and schedules) [Event.Pause]. This means that if there are
     * more events in the flows eventQueue then the flow won't pause again (after it is retried). These events are then scheduled (along
     * with any [ExistingSessionMessage] which arrive in the interim) when the flow is retried.
     */
    private fun extractAndQueueExternalEventsForPausedFlow(
        currentEventQueue: Channel<Event>,
        currentPendingDeduplicationHandlers: List<DeduplicationHandler>,
        pausedFlow: NonResidentFlow
    ) {
        pausedFlow.events += currentPendingDeduplicationHandlers.map{it.externalCause}
        do {
            val event = currentEventQueue.tryReceive()
            if (event is Event.GeneratedByExternalEvent) {
                pausedFlow.events.add(event.deduplicationHandler.externalCause)
            }
        } while (event != null)
    }


    /**
     * Extract all the incomplete deduplication handlers as well as the [ExternalEvent] and [Event.Pause] events from this flows event queue
     * [oldEventQueue]. Then schedule them (in the same order) for the new flow. This means that if a retried flow has a pause event
     * scheduled then the retried flow will eventually pause. The new flow will not retry again if future retry events have been scheduled.
     * When this method is called this flow must have been replaced by the new flow in [StateMachineInnerState.flows]. This method differs
     * from [extractAndQueueExternalEventsForPausedFlow] where (only) [externalEvents] are extracted and scheduled straight away.
     */
    private fun extractAndScheduleEventsForRetry(oldEventQueue: Channel<Event>, currentState: StateMachineState) {
        val flow = innerState.withLock {
            flows[currentState.flowLogic.runId]
        }
        val events = mutableListOf<Event>()
        do {
            val event = oldEventQueue.tryReceive()
            if (event is Event.Pause || event is Event.GeneratedByExternalEvent) events.add(event)
        } while (event != null)

        for (externalEvent in currentState.pendingDeduplicationHandlers) {
            deliverExternalEvent(externalEvent.externalCause)
        }
        for (event in events) {
            if (event is Event.GeneratedByExternalEvent) {
                deliverExternalEvent(event.deduplicationHandler.externalCause)
            } else {
                flow?.fiber?.scheduleEvent(event)
            }
        }
    }

    override fun deliverExternalEvent(event: ExternalEvent) {
        innerState.withLock {
            if (!stopping) {
                when (event) {
                    is ExternalEvent.ExternalMessageEvent -> onSessionMessage(event)
                    is ExternalEvent.ExternalStartFlowEvent<*> -> onExternalStartFlow(event)
                }
            }
        }
    }

    private fun <T> onExternalStartFlow(event: ExternalEvent.ExternalStartFlowEvent<T>) {
        val future = startFlow(
                event.flowId,
                event.flowLogic,
                event.context,
                ourIdentity = null,
                deduplicationHandler = event.deduplicationHandler
        )
        event.wireUpFuture(future)
    }

    private fun onSessionMessage(event: ExternalEvent.ExternalMessageEvent) {
        val peer = event.receivedMessage.peer
        val sessionMessage = try {
            event.receivedMessage.data.deserialize<SessionMessage>()
        } catch (ex: Exception) {
            logger.error("Unable to deserialize SessionMessage data from $peer", ex)
            event.deduplicationHandler.afterDatabaseTransaction()
            return
        }
        val sender = serviceHub.networkMapCache.getPeerByLegalName(peer)
        if (sender != null) {
            when (sessionMessage) {
                is ExistingSessionMessage -> onExistingSessionMessage(sessionMessage, sender, event)
                is InitialSessionMessage -> onSessionInit(sessionMessage, sender, event)
            }
        } else {
            // TODO Send the event to the flow hospital to be retried on network map update
            // TODO Test that restarting the node attempts to retry
            logger.error("Unknown peer $peer in $sessionMessage")
        }
    }

    private fun onExistingSessionMessage(
        sessionMessage: ExistingSessionMessage,
        sender: Party,
        externalEvent: ExternalEvent.ExternalMessageEvent
    ) {
        try {
            val deduplicationHandler = externalEvent.deduplicationHandler
            val recipientId = sessionMessage.recipientSessionId
            val flowId = sessionToFlow[recipientId]
            if (flowId == null) {
                deduplicationHandler.afterDatabaseTransaction()
                if (sessionMessage.payload === EndSessionMessage) {
                    logger.debug {
                        "Got ${EndSessionMessage::class.java.simpleName} for " +
                                "unknown session $recipientId, discarding..."
                    }
                } else {
                    // It happens when flows restart and the old sessions messages still arrive from a peer.
                    logger.info("Cannot find flow corresponding to session ID - $recipientId.")
                }
            } else {
                val event = Event.DeliverSessionMessage(sessionMessage, deduplicationHandler, sender)
                innerState.withLock {
                    flows[flowId]?.run { fiber.scheduleEvent(event) }
                        // If flow is not running add it to the list of external events to be processed if/when the flow resumes.
                        ?: pausedFlows[flowId]?.run { addExternalEvent(externalEvent) }
                        ?: logger.info("Cannot find fiber corresponding to flow ID $flowId")
                }
            }
        } catch (exception: Exception) {
            logger.error("Exception while routing $sessionMessage", exception)
            throw exception
        }
    }

    private fun onSessionInit(sessionMessage: InitialSessionMessage, sender: Party, event: ExternalEvent.ExternalMessageEvent) {
        try {
            val initiatedFlowFactory = getInitiatedFlowFactory(sessionMessage)
            val initiatedSessionId = SessionId.createRandom(secureRandom)
            val senderSession = FlowSessionImpl(sender, sender, initiatedSessionId)
            val flowLogic = initiatedFlowFactory.createFlow(senderSession)
            val initiatedFlowInfo = when (initiatedFlowFactory) {
                is InitiatedFlowFactory.Core -> FlowInfo(serviceHub.myInfo.platformVersion, "corda")
                is InitiatedFlowFactory.CorDapp -> FlowInfo(initiatedFlowFactory.flowVersion, initiatedFlowFactory.appName)
            }
            val senderCoreFlowVersion = when (initiatedFlowFactory) {
                is InitiatedFlowFactory.Core -> event.receivedMessage.platformVersion
                is InitiatedFlowFactory.CorDapp -> null
            }
            startInitiatedFlow(
                    event.flowId,
                    flowLogic,
                    event.deduplicationHandler,
                    senderSession,
                    initiatedSessionId,
                    sessionMessage,
                    senderCoreFlowVersion,
                    initiatedFlowInfo
            )
        } catch (t: Throwable) {
            logger.warn("Unable to initiate flow from $sender (appName=${sessionMessage.appName} " +
                    "flowVersion=${sessionMessage.flowVersion}), sending to the flow hospital", t)
            flowHospital.sessionInitErrored(sessionMessage, sender, event, t)
        }
    }

    // TODO this is a temporary hack until we figure out multiple identities
    private val ourFirstIdentity: Party get() = serviceHub.myInfo.legalIdentities[0]

    private fun getInitiatedFlowFactory(message: InitialSessionMessage): InitiatedFlowFactory<*> {
        val initiatorClass = try {
            Class.forName(message.initiatorFlowClassName, true, classloader)
        } catch (e: ClassNotFoundException) {
            throw SessionRejectException.UnknownClass(message.initiatorFlowClassName)
        }

        val initiatorFlowClass = try {
            initiatorClass.asSubclass(FlowLogic::class.java)
        } catch (e: ClassCastException) {
            throw SessionRejectException.NotAFlow(initiatorClass)
        }

        return serviceHub.getFlowFactory(initiatorFlowClass) ?: throw SessionRejectException.NotRegistered(initiatorFlowClass)
    }

    @Suppress("LongParameterList")
    private fun <A> startInitiatedFlow(
            flowId: StateMachineRunId,
            flowLogic: FlowLogic<A>,
            initiatingMessageDeduplicationHandler: DeduplicationHandler,
            peerSession: FlowSessionImpl,
            initiatedSessionId: SessionId,
            initiatingMessage: InitialSessionMessage,
            senderCoreFlowVersion: Int?,
            initiatedFlowInfo: FlowInfo
    ) {
        val flowStart = FlowStart.Initiated(peerSession, initiatedSessionId, initiatingMessage, senderCoreFlowVersion, initiatedFlowInfo)
        val ourIdentity = ourFirstIdentity
        startFlowInternal(
                flowId,
                InvocationContext.peer(peerSession.counterparty.name),
                flowLogic,
                flowStart,
                ourIdentity,
                initiatingMessageDeduplicationHandler
        )
    }

    @Suppress("LongParameterList")
    private fun <A> startFlowInternal(
            flowId: StateMachineRunId,
            invocationContext: InvocationContext,
            flowLogic: FlowLogic<A>,
            flowStart: FlowStart,
            ourIdentity: Party,
            deduplicationHandler: DeduplicationHandler?
    ): CordaFuture<FlowStateMachine<A>> {
        onCallingStartFlowInternal?.invoke()

        val existingFlow = innerState.withLock { flows[flowId] }
        val existingCheckpoint = if (existingFlow != null && existingFlow.fiber.transientState.isAnyCheckpointPersisted) {
            // Load the flow's checkpoint
            // The checkpoint will be missing if the flow failed before persisting the original checkpoint
            // CORDA-3359 - Do not start/retry a flow that failed after deleting its checkpoint (the whole of the flow might replay)
            val existingCheckpoint = database.transaction { checkpointStorage.getCheckpoint(flowId) }
            existingCheckpoint?.let { serializedCheckpoint ->
                tryDeserializeCheckpoint(serializedCheckpoint, flowId) ?: throw IllegalStateException(
                    "Unable to deserialize database checkpoint for flow $flowId. Something is very wrong. The flow will not retry."
                )
            }
        } else {
            // This is a brand new flow
            null
        }

        val flow = flowCreator.createFlowFromLogic(
            flowId,
            invocationContext,
            flowLogic,
            flowStart,
            ourIdentity,
            existingCheckpoint,
            deduplicationHandler,
            ourSenderUUID
        )
        val startedFuture = openFuture<Unit>()
        innerState.withLock {
            startedFutures[flowId] = startedFuture
        }
        totalStartedFlows.inc()
        addAndStartFlow(flowId, flow)
        return startedFuture.map { flow.fiber as FlowStateMachine<A> }
    }

    override fun scheduleFlowTimeout(flowId: StateMachineRunId) {
        flowTimeoutScheduler.timeout(flowId)
    }

    override fun cancelFlowTimeout(flowId: StateMachineRunId) {
        flowTimeoutScheduler.cancel(flowId)
    }

    override fun moveFlowToPaused(currentState: StateMachineState) {
        currentState.cancelFutureIfRunning()
        flowTimeoutScheduler.cancel(currentState.flowLogic.runId)
        innerState.withLock {
            val id = currentState.flowLogic.runId
            val flow = flows.remove(id)
            if (flow != null) {
                decrementLiveFibers()
                //Setting flowState = FlowState.Paused means we don't hold the frozen fiber in memory.
                val checkpoint = currentState.checkpoint.copy(status = Checkpoint.FlowStatus.PAUSED, flowState = FlowState.Paused)
                val pausedFlow = NonResidentFlow(id, checkpoint, flow.resultFuture)
                val eventQueue = flow.fiber.transientValues.eventQueue
                extractAndQueueExternalEventsForPausedFlow(eventQueue, currentState.pendingDeduplicationHandlers, pausedFlow)
                pausedFlows.put(id, pausedFlow)
            } else {
                logger.warn("Flow $id already removed before pausing")
            }
        }
    }

    private fun tryDeserializeCheckpoint(serializedCheckpoint: Checkpoint.Serialized, flowId: StateMachineRunId): Checkpoint? {
        return try {
            serializedCheckpoint.deserialize(checkpointSerializationContext)
        } catch (e: Exception) {
            if (reloadCheckpointAfterSuspend && currentStateMachine() != null) {
                logger.error(
                    "Unable to deserialize checkpoint for flow $flowId. [reloadCheckpointAfterSuspend] is turned on, throwing exception",
                    e
                )
                throw ReloadFlowFromCheckpointException(e)
            } else {
                logger.error("Unable to deserialize checkpoint for flow $flowId. Something is very wrong and this flow will be ignored.", e)
                null
            }
        }
    }

    private fun addAndStartFlow(id: StateMachineRunId, flow: Flow<*>) {
        val checkpoint = flow.fiber.snapshot().checkpoint
        for (sessionId in getFlowSessionIds(checkpoint)) {
            sessionToFlow[sessionId] = id
        }
        innerState.withLock {
            if (stopping) {
                startedFutures[id]?.setException(IllegalStateException("Will not start flow as SMM is stopping"))
                logger.trace("Not resuming as SMM is stopping.")
            } else {
                val oldFlow = flows.put(id, flow)
                if (oldFlow == null) {
                    incrementLiveFibers()
                    unfinishedFibers.countUp()
                } else {
                    oldFlow.resultFuture.captureLater(flow.resultFuture)
                }
                val flowLogic = flow.fiber.logic
                if (flowLogic.isEnabledTimedFlow()) flowTimeoutScheduler.timeout(id)
                flow.fiber.scheduleEvent(Event.DoRemainingWork)
                startOrResume(checkpoint, flow)
            }
        }
    }

    private fun startOrResume(checkpoint: Checkpoint, flow: Flow<*>) {
        when (checkpoint.flowState) {
            is FlowState.Unstarted -> {
                flow.fiber.start()
            }
            is FlowState.Started -> {
                Fiber.unparkDeserialized(flow.fiber, scheduler)
            }
            is FlowState.Finished -> throw IllegalStateException("Cannot start (or resume) a finished flow.")
        }
    }

    private fun getFlowSessionIds(checkpoint: Checkpoint): Set<SessionId> {
        val initiatedFlowStart = (checkpoint.flowState as? FlowState.Unstarted)?.flowStart as? FlowStart.Initiated
        return if (initiatedFlowStart == null) {
            checkpoint.checkpointState.sessions.keys
        } else {
            checkpoint.checkpointState.sessions.keys + initiatedFlowStart.initiatedSessionId
        }
    }

    private fun makeActionExecutor(checkpointSerializationContext: CheckpointSerializationContext): ActionExecutor {
        return ActionExecutorImpl(
            serviceHub,
            checkpointStorage,
            flowMessaging,
            this,
            actionFutureExecutor,
            checkpointSerializationContext
        )
    }

    private fun makeTransitionExecutor(): TransitionExecutor {
        val interceptors = ArrayList<TransitionInterceptor>()
        interceptors.add { HospitalisingInterceptor(flowHospital, it) }
        if (serviceHub.configuration.devMode) {
            interceptors.add { DumpHistoryOnErrorInterceptor(it) }
        }
        if (logger.isDebugEnabled) {
            interceptors.add { PrintingInterceptor(it) }
        }
        val transitionExecutor: TransitionExecutor = TransitionExecutorImpl(secureRandom, database)
        return interceptors.fold(transitionExecutor) { executor, interceptor -> interceptor(executor) }
    }

    private fun makeFlowHospital() : StaffedFlowHospital {
        // If the node is running as a notary service, we don't retain errored session initiation requests in case of missing Cordapps
        // to avoid memory leaks if the notary is under heavy load.
        return StaffedFlowHospital(flowMessaging, serviceHub.clock, ourSenderUUID)
    }

    private fun StateMachineInnerState.removeFlowOrderly(
            flow: Flow<*>,
            removalReason: FlowRemovalReason.OrderlyFinish,
            lastState: StateMachineState
    ) {
        drainFlowEventQueue(flow)
        // final sanity checks
        require(lastState.pendingDeduplicationHandlers.isEmpty()) { "Flow cannot be removed until all pending deduplications have completed" }
        require(lastState.isRemoved) { "Flow must be in removable state before removal" }
        require(lastState.checkpoint.checkpointState.subFlowStack.size == 1) { "Checkpointed stack must be empty" }
        require(flow.fiber.id !in sessionToFlow.values) { "Flow fibre must not be needed by an existing session" }
        flow.fiber.clientId?.let { setClientIdAsSucceeded(it, flow.fiber.id) }
        flow.resultFuture.set(removalReason.flowReturnValue)
        lastState.flowLogic.progressTracker?.currentStep = ProgressTracker.DONE
        changesPublisher.onNext(StateMachineManager.Change.Removed(lastState.flowLogic, Try.Success(removalReason.flowReturnValue)))
    }

    private fun StateMachineInnerState.removeFlowError(
            flow: Flow<*>,
            removalReason: FlowRemovalReason.ErrorFinish,
            lastState: StateMachineState
    ) {
        drainFlowEventQueue(flow)
        // Complete the started future, needed when the flow fails during flow init (before completing an [UnstartedFlowTransition])
        startedFutures.remove(flow.fiber.id)?.set(Unit)
        flow.fiber.clientId?.let {
            if (flow.fiber.isKilled) {
                clientIdsToFlowIds.remove(it)
            } else {
                setClientIdAsFailed(it, flow.fiber.id) }
            }
        val flowError = removalReason.flowErrors[0] // TODO what to do with several?
        val exception = flowError.exception
        (exception as? FlowException)?.originalErrorId = flowError.errorId
        flow.resultFuture.setException(exception)
        lastState.flowLogic.progressTracker?.endWithError(exception)
        changesPublisher.onNext(StateMachineManager.Change.Removed(lastState.flowLogic, Try.Failure<Nothing>(exception)))
    }

    // The flow's event queue may be non-empty in case it shut down abruptly. We handle outstanding events here.
    private fun drainFlowEventQueue(flow: Flow<*>) {
        while (true) {
            val event = flow.fiber.transientValues.eventQueue.tryReceive() ?: return
            when (event) {
                is Event.DoRemainingWork -> {}
                is Event.DeliverSessionMessage -> {
                    // Acknowledge the message so it doesn't leak in the broker.
                    event.deduplicationHandler.afterDatabaseTransaction()
                    when (event.sessionMessage.payload) {
                        EndSessionMessage -> {
                            logger.debug { "Unhandled message ${event.sessionMessage} due to flow shutting down" }
                        }
                        else -> {
                            logger.warn("Unhandled message ${event.sessionMessage} due to flow shutting down")
                        }
                    }
                }
                else -> {
                    logger.warn("Unhandled event $event due to flow shutting down")
                }
            }
        }
    }

    private fun StateMachineState.cancelFutureIfRunning() {
        future?.run {
            logger.debug { "Cancelling future for flow ${flowLogic.runId}" }
            if (!isDone) cancel(true)
            future = null
        }
    }

    private fun StateMachineInnerState.setClientIdAsSucceeded(clientId: String, id: StateMachineRunId) {
        setClientIdAsRemoved(clientId, id, true)
    }

    private fun StateMachineInnerState.setClientIdAsFailed(clientId: String, id: StateMachineRunId) {
        setClientIdAsRemoved(clientId, id, false)
    }

    private fun StateMachineInnerState.setClientIdAsRemoved(
        clientId: String,
        id: StateMachineRunId,
        succeeded: Boolean
    ) {
        clientIdsToFlowIds.compute(clientId) { _, existingStatus ->
            require(existingStatus != null && existingStatus is FlowWithClientIdStatus.Active)
            FlowWithClientIdStatus.Removed(id, succeeded)
        }
    }

    private fun activeOrRemovedClientIdFuture(existingStatus: FlowWithClientIdStatus, clientId: String) = when (existingStatus) {
        is FlowWithClientIdStatus.Active -> existingStatus.flowStateMachineFuture
        is FlowWithClientIdStatus.Removed -> {
            val flowId = existingStatus.flowId
            val resultFuture = if (existingStatus.succeeded) {
                val flowResult = database.transaction { checkpointStorage.getFlowResult(existingStatus.flowId, throwIfMissing = true) }
                doneFuture(flowResult)
            } else {
                val flowException =
                    database.transaction { checkpointStorage.getFlowException(existingStatus.flowId, throwIfMissing = true) }
                openFuture<Any?>().apply { setException(flowException as Throwable) }
            }

            doneClientIdFuture(flowId, resultFuture, clientId)
        }
    }

    /**
     * The flow out of which a [doneFuture] will be produced should be a started flow,
     * i.e. it should not exist in [mutex.content.startedFutures].
     */
    private fun doneClientIdFuture(
        id: StateMachineRunId,
        resultFuture: CordaFuture<Any?>,
        clientId: String
    ): CordaFuture<FlowStateMachineHandle<out Any?>> =
        doneFuture(object : FlowStateMachineHandle<Any?> {
            override val logic: Nothing? = null
            override val id: StateMachineRunId = id
            override val resultFuture: CordaFuture<Any?> = resultFuture
            override val clientId: String? = clientId
        }
        )

    override fun <T> reattachFlowWithClientId(clientId: String): FlowStateMachineHandle<T>? {
        return innerState.withLock {
            clientIdsToFlowIds[clientId]?.let {
                val existingFuture = activeOrRemovedClientIdFutureForReattach(it, clientId)
                existingFuture?.let { uncheckedCast(existingFuture.get()) }
            }
        }
    }

    @Suppress("NestedBlockDepth")
    private fun activeOrRemovedClientIdFutureForReattach(
        existingStatus: FlowWithClientIdStatus,
        clientId: String
    ): CordaFuture<out FlowStateMachineHandle<out Any?>>? {
        return when (existingStatus) {
            is FlowWithClientIdStatus.Active -> existingStatus.flowStateMachineFuture
            is FlowWithClientIdStatus.Removed -> {
                val flowId = existingStatus.flowId
                val resultFuture = if (existingStatus.succeeded) {
                    try {
                        val flowResult =
                            database.transaction { checkpointStorage.getFlowResult(existingStatus.flowId, throwIfMissing = true) }
                        doneFuture(flowResult)
                    } catch (e: IllegalStateException) {
                        null
                    }
                } else {
                    try {
                        val flowException =
                            database.transaction { checkpointStorage.getFlowException(existingStatus.flowId, throwIfMissing = true) }
                        openFuture<Any?>().apply { setException(flowException as Throwable) }
                    } catch (e: IllegalStateException) {
                        null
                    }
                }

                resultFuture?.let { doneClientIdFuture(flowId, it, clientId) }
            }
        }
    }

    override fun removeClientId(clientId: String): Boolean {
        var removedFlowId: StateMachineRunId? = null
        innerState.withLock {
            clientIdsToFlowIds.computeIfPresent(clientId) { _, existingStatus ->
                if (existingStatus is FlowWithClientIdStatus.Removed) {
                    removedFlowId = existingStatus.flowId
                    null
                } else {
                    existingStatus
                }
            }
        }

        removedFlowId?.let {
            return database.transaction { checkpointStorage.removeCheckpoint(it, mayHavePersistentResults = true) }
        }
        return false
    }
}
