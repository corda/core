package net.corda.core.flows

import co.paralleluniverse.fibers.Suspendable
import co.paralleluniverse.strands.Strand
import net.corda.core.CordaInternal
import net.corda.core.DeleteForDJVM
import net.corda.core.contracts.StateRef
import net.corda.core.crypto.SecureHash
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.AnonymousParty
import net.corda.core.identity.Party
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.internal.*
import net.corda.core.messaging.DataFeed
import net.corda.core.node.NodeInfo
import net.corda.core.node.ServiceHub
import net.corda.core.serialization.CordaSerializable
import net.corda.core.serialization.SerializationDefaults
import net.corda.core.serialization.serialize
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.ProgressTracker
import net.corda.core.utilities.UntrustworthyData
import net.corda.core.utilities.debug
import net.corda.core.utilities.toNonEmptySet
import org.slf4j.Logger
import java.time.Duration
import java.util.*

/**
 * A sub-class of [FlowLogic<T>] implements a flow using direct, straight line blocking code. Thus you
 * can write complex flow logic in an ordinary fashion, without having to think about callbacks, restarting after
 * a node crash, how many instances of your flow there are running and so on.
 *
 * Invoking the network will cause the call stack to be suspended onto the heap and then serialized to a database using
 * the Quasar fibers framework. Because of this, if you need access to data that might change over time, you should
 * request it just-in-time via the [serviceHub] property which is provided. Don't try and keep data you got from a
 * service across calls to send/receive/sendAndReceive because the world might change in arbitrary ways out from
 * underneath you, for instance, if the node is restarted or reconfigured!
 *
 * Additionally, be aware of what data you pin either via the stack or in your [FlowLogic] implementation. Very large
 * objects or datasets will hurt performance by increasing the amount of data stored in each checkpoint.
 *
 * If you'd like to use another FlowLogic class as a component of your own, construct it on the fly and then pass
 * it to the [subFlow] method. It will return the result of that flow when it completes.
 *
 * If your flow (whether it's a top-level flow or a subflow) is supposed to initiate a session with the counterparty
 * and request they start their counterpart flow, then make sure it's annotated with [InitiatingFlow]. This annotation
 * also has a version property to allow you to version your flow and enables a node to restrict support for the flow to
 * that particular version.
 *
 * Functions that suspend the flow (including all functions on [FlowSession]) accept a maySkipCheckpoint parameter
 * defaulting to false, false meaning a checkpoint should always be created on suspend. This parameter may be set to
 * true which allows the implementation to potentially optimise away the checkpoint, saving a roundtrip to the database.
 *
 * This option however comes with a big warning sign: Setting the parameter to true requires the flow's code to be
 * replayable from the previous checkpoint (or start of flow) up until the next checkpoint (or end of flow) in order to
 * prepare for hard failures. As suspending functions always commit the flow's database transaction regardless of this
 * parameter the flow must be prepared for scenarios where a previous running of the flow *already committed its
 * relevant database transactions*. Only set this option to true if you know what you're doing.
 */
@Suppress("DEPRECATION", "DeprecatedCallableAddReplaceWith")
@DeleteForDJVM
abstract class FlowLogic<out T> {
    /** This is where you should log things to. */
    val logger: Logger get() = stateMachine.logger

    @DeleteForDJVM
    companion object {
        /**
         * Return the outermost [FlowLogic] instance, or null if not in a flow.
         */
        @Suppress("unused")
        @JvmStatic
        val currentTopLevel: FlowLogic<*>?
            get() = (Strand.currentStrand() as? FlowStateMachine<*>)?.logic

        /**
         * If on a flow, suspends the flow and only wakes it up after at least [duration] time has passed.  Otherwise,
         * just sleep for [duration].  This sleep function is not designed to aid scheduling, for which you should
         * consider using [net.corda.core.contracts.SchedulableState].  It is designed to aid with managing contention
         * for which you have not managed via another means.
         *
         * Warning: long sleeps and in general long running flows are highly discouraged, as there is currently no
         * support for flow migration! This method will throw an exception if you attempt to sleep for longer than
         * 5 minutes.
         */
        @Suspendable
        @JvmStatic
        @JvmOverloads
        @Throws(FlowException::class)
        fun sleep(duration: Duration, maySkipCheckpoint: Boolean = false) {
            if (duration > Duration.ofMinutes(5)) {
                throw FlowException("Attempt to sleep for longer than 5 minutes is not supported.  Consider using SchedulableState.")
            }
            val fiber = (Strand.currentStrand() as? FlowStateMachine<*>)
            if (fiber == null) {
                Strand.sleep(duration.toMillis())
            } else {
                val request = FlowIORequest.Sleep(wakeUpAfter = fiber.serviceHub.clock.instant() + duration)
                fiber.suspend(request, maySkipCheckpoint = maySkipCheckpoint)
            }
        }

        private val DEFAULT_TRACKER = { ProgressTracker() }
    }

    /**
     * Returns a wrapped [java.util.UUID] object that identifies this state machine run (i.e. subflows have the same
     * identifier as their parents).
     */
    val runId: StateMachineRunId get() = stateMachine.id

    /**
     * Provides access to big, heavy classes that may be reconstructed from time to time, e.g. across restarts. It is
     * only available once the flow has started, which means it cannot be accessed in the constructor. Either
     * access this lazily or from inside [call].
     */
    val serviceHub: ServiceHub get() = stateMachine.serviceHub

    /**
     * Creates a communication session with [destination]. Subsequently you may send/receive using this session object. How the messaging
     * is routed depends on the [Destination] type, including whether this call does any initial communication.
     */
    @Suspendable
    fun initiateFlow(destination: Destination): FlowSession {
        require(destination is Party || destination is AnonymousParty) { "Unsupported destination type ${destination.javaClass.name}" }
        return stateMachine.initiateFlow(destination, serviceHub.identityService.wellKnownPartyFromAnonymous(destination as AbstractParty)
                ?: throw IllegalArgumentException("Could not resolve destination: $destination"))
    }

    /**
     * Creates a communication session with [party]. Subsequently you may send/receive using this session object. Note
     * that this function does not communicate in itself, the counter-flow will be kicked off by the first send/receive.
     */
    @Suspendable
    fun initiateFlow(party: Party): FlowSession = stateMachine.initiateFlow(party, party)

    /**
     * Specifies the identity, with certificate, to use for this flow. This will be one of the multiple identities that
     * belong to this node.
     * @see NodeInfo.legalIdentitiesAndCerts
     *
     * Note: The current implementation returns the single identity of the node. This will change once multiple identities
     * is implemented.
     */
    val ourIdentityAndCert: PartyAndCertificate
        get() {
            return serviceHub.myInfo.legalIdentitiesAndCerts.find { it.party == stateMachine.ourIdentity }
                    ?: throw IllegalStateException("Identity specified by ${stateMachine.id} (${stateMachine.ourIdentity}) is not one of ours!")
        }

    /**
     * Specifies the identity to use for this flow. This will be one of the multiple identities that belong to this node.
     * This is the same as calling `ourIdentityAndCert.party`.
     * @see NodeInfo.legalIdentities
     *
     * Note: The current implementation returns the single identity of the node. This will change once multiple identities
     * is implemented.
     */
    val ourIdentity: Party get() = stateMachine.ourIdentity

    // Used to implement the deprecated send/receive functions using Party. When such a deprecated function is used we
    // create a fresh session for the Party, put it here and use it in subsequent deprecated calls.
    private val deprecatedPartySessionMap = HashMap<Party, FlowSession>()

    private fun getDeprecatedSessionForParty(party: Party): FlowSession {
        return deprecatedPartySessionMap.getOrPut(party) { initiateFlow(party) }
    }

    /**
     * Returns a [FlowInfo] object describing the flow [otherParty] is using. With [FlowInfo.flowVersion] it
     * provides the necessary information needed for the evolution of flows and enabling backwards compatibility.
     *
     * This method can be called before any send or receive has been done with [otherParty]. In such a case this will force
     * them to start their flow.
     */
    @Deprecated("Use FlowSession.getCounterpartyFlowInfo()", level = DeprecationLevel.WARNING)
    @Suspendable
    fun getFlowInfo(otherParty: Party): FlowInfo = getDeprecatedSessionForParty(otherParty).getCounterpartyFlowInfo()

    /**
     * Serializes and queues the given [payload] object for sending to the [otherParty]. Suspends until a response
     * is received, which must be of the given [R] type.
     *
     * Remember that when receiving data from other parties the data should not be trusted until it's been thoroughly
     * verified for consistency and that all expectations are satisfied, as a malicious peer may send you subtly
     * corrupted data in order to exploit your code.
     *
     * Note that this function is not just a simple send+receive pair: it is more efficient and more correct to
     * use this when you expect to do a message swap than do use [send] and then [receive] in turn.
     *
     * @return an [UntrustworthyData] wrapper around the received object.
     */
    @Deprecated("Use FlowSession.sendAndReceive()", level = DeprecationLevel.WARNING)
    inline fun <reified R : Any> sendAndReceive(otherParty: Party, payload: Any): UntrustworthyData<R> {
        return sendAndReceive(R::class.java, otherParty, payload)
    }

    /**
     * Serializes and queues the given [payload] object for sending to the [otherParty]. Suspends until a response
     * is received, which must be of the given [receiveType]. Remember that when receiving data from other parties the data
     * should not be trusted until it's been thoroughly verified for consistency and that all expectations are
     * satisfied, as a malicious peer may send you subtly corrupted data in order to exploit your code.
     *
     * Note that this function is not just a simple send+receive pair: it is more efficient and more correct to
     * use this when you expect to do a message swap than do use [send] and then [receive] in turn.
     *
     * @return an [UntrustworthyData] wrapper around the received object.
     */
    @Deprecated("Use FlowSession.sendAndReceive()", level = DeprecationLevel.WARNING)
    @Suspendable
    open fun <R : Any> sendAndReceive(receiveType: Class<R>, otherParty: Party, payload: Any): UntrustworthyData<R> {
        return getDeprecatedSessionForParty(otherParty).sendAndReceive(receiveType, payload)
    }

    /**
     * Suspends until the specified [otherParty] sends us a message of type [R].
     *
     * Remember that when receiving data from other parties the data should not be trusted until it's been thoroughly
     * verified for consistency and that all expectations are satisfied, as a malicious peer may send you subtly
     * corrupted data in order to exploit your code.
     */
    @Deprecated("Use FlowSession.receive()", level = DeprecationLevel.WARNING)
    inline fun <reified R : Any> receive(otherParty: Party): UntrustworthyData<R> = receive(R::class.java, otherParty)

    /**
     * Suspends until the specified [otherParty] sends us a message of type [receiveType].
     *
     * Remember that when receiving data from other parties the data should not be trusted until it's been thoroughly
     * verified for consistency and that all expectations are satisfied, as a malicious peer may send you subtly
     * corrupted data in order to exploit your code.
     *
     * @return an [UntrustworthyData] wrapper around the received object.
     */
    @Deprecated("Use FlowSession.receive()", level = DeprecationLevel.WARNING)
    @Suspendable
    open fun <R : Any> receive(receiveType: Class<R>, otherParty: Party): UntrustworthyData<R> {
        return getDeprecatedSessionForParty(otherParty).receive(receiveType)
    }

    /**
     * Queues the given [payload] for sending to the [otherParty] and continues without suspending.
     *
     * Note that the other party may receive the message at some arbitrary later point or not at all: if [otherParty]
     * is offline then message delivery will be retried until it comes back or until the message is older than the
     * network's event horizon time.
     */
    @Deprecated("Use FlowSession.send()", level = DeprecationLevel.WARNING)
    @Suspendable
    open fun send(otherParty: Party, payload: Any) {
        getDeprecatedSessionForParty(otherParty).send(payload)
    }

    @Suspendable
    internal fun <R : Any> FlowSession.sendAndReceiveWithRetry(receiveType: Class<R>, payload: Any): UntrustworthyData<R> {
        val request = FlowIORequest.SendAndReceive(
                sessionToMessage = mapOf(this to payload.serialize(context = SerializationDefaults.P2P_CONTEXT)),
                shouldRetrySend = true
        )
        return stateMachine.suspend(request, maySkipCheckpoint = false)[this]!!.checkPayloadIs(receiveType)
    }

    @Suspendable
    internal inline fun <reified R : Any> FlowSession.sendAndReceiveWithRetry(payload: Any): UntrustworthyData<R> {
        return sendAndReceiveWithRetry(R::class.java, payload)
    }

    /** Suspends until a message has been received for each session in the specified [sessions].
     *
     * Consider [receiveAll(receiveType: Class<R>, sessions: List<FlowSession>): List<UntrustworthyData<R>>] when the same type is expected from all sessions.
     *
     * Remember that when receiving data from other parties the data should not be trusted until it's been thoroughly
     * verified for consistency and that all expectations are satisfied, as a malicious peer may send you subtly
     * corrupted data in order to exploit your code.
     *
     * @returns a [Map] containing the objects received, wrapped in an [UntrustworthyData], by the [FlowSession]s who sent them.
     */
    @Suspendable
    @JvmOverloads
    open fun receiveAllMap(sessions: Map<FlowSession, Class<out Any>>, maySkipCheckpoint: Boolean = false): Map<FlowSession, UntrustworthyData<Any>> {
        enforceNoPrimitiveInReceive(sessions.values)
        val replies = stateMachine.suspend(
                ioRequest = FlowIORequest.Receive(sessions.keys.toNonEmptySet()),
                maySkipCheckpoint = maySkipCheckpoint
        )
        return replies.mapValues { (session, payload) -> payload.checkPayloadIs(sessions[session]!!) }
    }

    /**
     * Suspends until a message has been received for each session in the specified [sessions].
     *
     * Consider [sessions: Map<FlowSession, Class<out Any>>): Map<FlowSession, UntrustworthyData<Any>>] when sessions are expected to receive different types.
     *
     * Remember that when receiving data from other parties the data should not be trusted until it's been thoroughly
     * verified for consistency and that all expectations are satisfied, as a malicious peer may send you subtly
     * corrupted data in order to exploit your code.
     *
     * @returns a [List] containing the objects received, wrapped in an [UntrustworthyData], with the same order of [sessions].
     */
    @Suspendable
    @JvmOverloads
    open fun <R : Any> receiveAll(receiveType: Class<R>, sessions: List<FlowSession>, maySkipCheckpoint: Boolean = false): List<UntrustworthyData<R>> {
        enforceNoPrimitiveInReceive(listOf(receiveType))
        enforceNoDuplicates(sessions)
        return castMapValuesToKnownType(receiveAllMap(associateSessionsToReceiveType(receiveType, sessions)))
    }

    /**
     * Invokes the given subflow. This function returns once the subflow completes successfully with the result
     * returned by that subflow's [call] method. If the subflow has a progress tracker, it is attached to the
     * current step in this flow's progress tracker.
     *
     * If the subflow is not an initiating flow (i.e. not annotated with [InitiatingFlow]) then it will continue to use
     * the existing sessions this flow has created with its counterparties. This allows for subflows which can act as
     * building blocks for other flows, for example removing the boilerplate of common sequences of sends and receives.
     *
     * @throws FlowException This is either thrown by [subLogic] itself or propagated from any of the remote
     * [FlowLogic]s it communicated with. The subflow can be retried by catching this exception.
     */
    @Suspendable
    @Throws(FlowException::class)
    open fun <R> subFlow(subLogic: FlowLogic<R>): R {
        subLogic.stateMachine = stateMachine
        maybeWireUpProgressTracking(subLogic)
        logger.debug { "Calling subflow: $subLogic" }
        val result = stateMachine.subFlow(subLogic)
        logger.debug { "Subflow finished with result ${result.toString().abbreviate(300)}" }
        return result
    }

    /**
     * Flows can call this method to ensure that the active FlowInitiator is authorised for a particular action.
     * This provides fine grained control over application level permissions, when RPC control over starting the flow is insufficient,
     * or the permission is runtime dependent upon the choices made inside long lived flow code.
     * For example some users may have restricted limits on how much cash they can transfer, or whether they can change certain fields.
     * An audit event is always recorded whenever this method is used.
     * If the permission is not granted for the FlowInitiator a FlowException is thrown.
     * @param permissionName is a string representing the desired permission. Each flow is given a distinct namespace for these permissions.
     * @param extraAuditData in the audit log for this permission check these extra key value pairs will be recorded.
     */
    @Throws(FlowException::class)
    fun checkFlowPermission(permissionName: String, extraAuditData: Map<String, String>) {
        stateMachine.checkFlowPermission(permissionName, extraAuditData)
    }

    /**
     * Flows can call this method to record application level flow audit events
     * @param eventType is a string representing the type of event. Each flow is given a distinct namespace for these names.
     * @param comment a general human readable summary of the event.
     * @param extraAuditData in the audit log for this permission check these extra key value pairs will be recorded.
     */
    fun recordAuditEvent(eventType: String, comment: String, extraAuditData: Map<String, String>) {
        stateMachine.recordAuditEvent(eventType, comment, extraAuditData)
    }

    /**
     * Override this to provide a [ProgressTracker]. If one is provided and stepped, the framework will do something
     * helpful with the progress reports e.g record to the audit service. If this flow is invoked as a subflow of another,
     * then the tracker will be made a child of the current step in the parent. If it's null, this flow doesn't track
     * progress.
     *
     * Note that this has to return a tracker before the flow is invoked. You can't change your mind half way
     * through.
     */
    open val progressTracker: ProgressTracker? = DEFAULT_TRACKER()

    /**
     * This is where you fill out your business logic.
     */
    @Suspendable
    @Throws(FlowException::class)
    abstract fun call(): T

    /**
     * Returns a pair of the current progress step, as a string, and an observable of stringified changes to the
     * [progressTracker].
     *
     * @return Returns null if this flow has no progress tracker.
     */
    fun track(): DataFeed<String, String>? {
        // TODO this is not threadsafe, needs an atomic get-step-and-subscribe
        return progressTracker?.let {
            DataFeed(it.currentStep.label, it.changes.map { it.toString() })
        }
    }

    /**
     * Returns a pair of the current progress step index (as integer) in steps tree of current [progressTracker], and an observable
     * of its upcoming changes.
     *
     * @return Returns null if this flow has no progress tracker.
     */
    fun trackStepsTreeIndex(): DataFeed<Int, Int>? {
        // TODO this is not threadsafe, needs an atomic get-step-and-subscribe
        return progressTracker?.let {
            DataFeed(it.stepsTreeIndex, it.stepsTreeIndexChanges)
        }
    }

    /**
     * Returns a pair of the current steps tree of current [progressTracker] as pairs of zero-based depth and stringified step
     * label and observable of upcoming changes to the structure.
     *
     * @return Returns null if this flow has no progress tracker.
     */
    fun trackStepsTree(): DataFeed<List<Pair<Int, String>>, List<Pair<Int, String>>>? {
        // TODO this is not threadsafe, needs an atomic get-step-and-subscribe
        return progressTracker?.let {
            DataFeed(it.allStepsLabels, it.stepsTreeChanges)
        }
    }

    /**
     * Suspends the flow until the transaction with the specified ID is received, successfully verified and
     * sent to the vault for processing. Note that this call suspends until the transaction is considered
     * valid by the local node, but that doesn't imply the vault will consider it relevant.
     */
    @Suspendable
    @JvmOverloads
    fun waitForLedgerCommit(hash: SecureHash, maySkipCheckpoint: Boolean = false): SignedTransaction {
        val request = FlowIORequest.WaitForLedgerCommit(hash)
        return stateMachine.suspend(request, maySkipCheckpoint = maySkipCheckpoint)
    }

    /**
     * Suspends the current flow until all the provided [StateRef]s have been consumed.
     *
     * WARNING! Remember that the flow which uses this async operation will _NOT_ wake-up until all the supplied StateRefs
     * have been consumed. If the node isn't aware of the supplied StateRefs or if the StateRefs are never consumed, then
     * the calling flow will remain suspended FOREVER!!
     *
     * @param stateRefs the StateRefs which will be consumed in the future.
     */
    @Suspendable
    fun waitForStateConsumption(stateRefs: Set<StateRef>) = executeAsync(WaitForStateConsumption(stateRefs, serviceHub))

    /**
     * Returns a shallow copy of the Quasar stack frames at the time of call to [flowStackSnapshot]. Use this to inspect
     * what objects would be serialised at the time of call to a suspending action (e.g. send/receive).
     * Note: This logic is only available during tests and is not meant to be used during the production deployment.
     * Therefore the default implementation does nothing.
     */
    @Suspendable
    fun flowStackSnapshot(): FlowStackSnapshot? = stateMachine.flowStackSnapshot(this::class.java)

    /**
     * Persists a shallow copy of the Quasar stack frames at the time of call to [persistFlowStackSnapshot].
     * Use this to track the monitor evolution of the quasar stack values during the flow execution.
     * The flow stack snapshot is stored in a file located in {baseDir}/flowStackSnapshots/YYYY-MM-DD/{flowId}/
     * where baseDir is the node running directory and flowId is the flow unique identifier generated by the platform.
     *
     * Note: With respect to the [flowStackSnapshot], the snapshot being persisted by this method is partial,
     * meaning that only flow relevant traces and local variables are persisted.
     * Also, this logic is only available during tests and is not meant to be used during the production deployment.
     * Therefore the default implementation does nothing.
     */
    @Suspendable
    fun persistFlowStackSnapshot() = stateMachine.persistFlowStackSnapshot(this::class.java)

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private var _stateMachine: FlowStateMachine<*>? = null
    /**
     * @suppress
     * Internal only. Reference to the [co.paralleluniverse.fibers.Fiber] instance that is the top level controller for
     * the entire flow. When inside a flow this is equivalent to [co.paralleluniverse.strands.Strand.currentStrand]. This
     * is public only because it must be accessed across module boundaries.
     */
    var stateMachine: FlowStateMachine<*>
        @CordaInternal
        get() = _stateMachine ?: throw IllegalStateException(
                "You cannot access the flow's state machine until the flow has been started.")
        @CordaInternal
        set(value) {
            _stateMachine = value
        }

    private fun maybeWireUpProgressTracking(subLogic: FlowLogic<*>) {
        val ours = progressTracker
        val theirs = subLogic.progressTracker
        if (ours != null && theirs != null && ours != theirs) {
            if (ours.currentStep == ProgressTracker.UNSTARTED) {
                logger.debug { "Initializing the progress tracker for flow: ${this::class.java.name}." }
                ours.nextStep()
            }
            ours.setChildProgressTracker(ours.currentStep, theirs)
        }
    }

    private fun enforceNoDuplicates(sessions: List<FlowSession>) {
        require(sessions.size == sessions.toSet().size) { "A flow session can only appear once as argument." }
    }

    private fun enforceNoPrimitiveInReceive(types: Collection<Class<*>>) {
        val primitiveTypes = types.filter { it.isPrimitive }
        require(primitiveTypes.isEmpty()) { "Cannot receive primitive type(s) $primitiveTypes" }
    }

    private fun <R> associateSessionsToReceiveType(receiveType: Class<R>, sessions: List<FlowSession>): Map<FlowSession, Class<R>> {
        return sessions.associateByTo(LinkedHashMap(), { it }, { receiveType })
    }

    private fun <R> castMapValuesToKnownType(map: Map<FlowSession, UntrustworthyData<Any>>): List<UntrustworthyData<R>> {
        return map.values.map { uncheckedCast<Any, UntrustworthyData<R>>(it) }
    }

    fun getMinimumCommonVersion(parties: List<AbstractParty>): Int {
        val versions = parties.mapNotNull { serviceHub.networkMapCache.getNodeByLegalIdentity(it) }.map { nodeInfo ->
            if (nodeInfo.legalIdentities.first().name.organisationUnit?.contains("V5") == true) {
                5
            } else {
                4
            }
        }
        return versions.min() ?: 4
    }
}

/**
 * Version and name of the CorDapp hosting the other side of the flow.
 */
@CordaSerializable
data class FlowInfo(
        /**
         * The integer flow version the other side is using.
         * @see InitiatingFlow
         */
        val flowVersion: Int,
        /**
         * Name of the CorDapp jar hosting the flow, without the .jar extension. It will include a unique identifier
         * to deduplicate it from other releases of the same CorDapp, typically a version string. See the
         * [CorDapp JAR format](https://docs.corda.net/cordapp-build-systems.html#cordapp-jar-format) for more details.
         */
        val appName: String)
