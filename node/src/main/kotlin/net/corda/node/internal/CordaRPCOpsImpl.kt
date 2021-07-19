package net.corda.node.internal

import net.corda.client.rpc.notUsed
import net.corda.common.logging.CordaVersion
import net.corda.core.concurrent.CordaFuture
import net.corda.core.context.InvocationContext
import net.corda.core.context.InvocationOrigin
import net.corda.core.contracts.ContractState
import net.corda.core.cordapp.Cordapp
import net.corda.core.cordapp.CordappInfo
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.FlowInitiator
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.StartableByRPC
import net.corda.core.flows.StateMachineRunId
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.FlowStateMachineHandle
import net.corda.core.internal.RPC_UPLOADER
import net.corda.core.internal.STRUCTURAL_STEP_PREFIX
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.DataFeed
import net.corda.core.messaging.FlowHandle
import net.corda.core.messaging.FlowHandleImpl
import net.corda.core.messaging.FlowHandleWithClientId
import net.corda.core.messaging.FlowHandleWithClientIdImpl
import net.corda.core.messaging.FlowProgressHandle
import net.corda.core.messaging.FlowProgressHandleImpl
import net.corda.core.messaging.ParametersUpdateInfo
import net.corda.core.messaging.RPCReturnsObservables
import net.corda.core.messaging.StateMachineInfo
import net.corda.core.messaging.StateMachineTransactionMapping
import net.corda.core.messaging.StateMachineUpdate
import net.corda.core.messaging.pendingFlowsCount
import net.corda.core.node.NetworkParameters
import net.corda.core.node.NodeDiagnosticInfo
import net.corda.core.node.NodeInfo
import net.corda.core.node.services.AttachmentId
import net.corda.core.node.services.NetworkMapCache
import net.corda.core.node.services.Vault
import net.corda.core.node.services.vault.AttachmentQueryCriteria
import net.corda.core.node.services.vault.AttachmentSort
import net.corda.core.node.services.vault.PageSpecification
import net.corda.core.node.services.vault.QueryCriteria
import net.corda.core.node.services.vault.Sort
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.loggerFor
import net.corda.node.services.api.FlowStarter
import net.corda.node.services.api.ServiceHubInternal
import net.corda.node.services.network.toMapChange
import net.corda.node.services.network.toNodeInfo
import net.corda.node.services.rpc.context
import net.corda.node.services.statemachine.StateMachineManager
import net.corda.nodeapi.exceptions.MissingAttachmentException
import net.corda.nodeapi.exceptions.NonRpcFlowException
import net.corda.nodeapi.exceptions.RejectedCommandException
import rx.Observable
import rx.Subscription
import java.io.InputStream
import java.security.PublicKey
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

/**
 * Server side implementations of RPCs available to MQ based client tools. Execution takes place on the server
 * thread (i.e. serially). Arguments are serialised and deserialised automatically.
 */
internal class CordaRPCOpsImpl(
        private val services: ServiceHubInternal,
        private val smm: StateMachineManager,
        private val flowStarter: FlowStarter,
        private val shutdownNode: () -> Unit
) : CordaRPCOps, AutoCloseable {

    private companion object {
        private val logger = loggerFor<CordaRPCOpsImpl>()
    }

    private val drainingShutdownHook = AtomicReference<Subscription?>()

    init {
        services.nodeProperties.flowsDrainingMode.values.filter { it.isDisabled() }.subscribe({
            cancelDrainingShutdownHook()
        }, {
            // Nothing to do in case of errors here.
        })
    }

    private fun Pair<Boolean, Boolean>.isDisabled(): Boolean = first && !second

    /**
     * Returns the RPC protocol version, which is the same the node's platform Version. Exists since version 1 so guaranteed
     * to be present.
     */
    override val protocolVersion: Int get() = nodeInfo().platformVersion

    override fun networkMapSnapshot(): List<NodeInfo> {
        return services.networkMapCache.allMembers.map { it.toNodeInfo() }.toList()
    }

    override val networkParameters: NetworkParameters get() = services.networkParameters

    override fun networkParametersFeed(): DataFeed<ParametersUpdateInfo?, ParametersUpdateInfo> = throw NotImplementedError()

    override fun acceptNewNetworkParameters(parametersHash: SecureHash) = throw NotImplementedError()

    override fun networkMapFeed(): DataFeed<List<NodeInfo>, NetworkMapCache.MapChange> {
        val dataFeed = services.networkMapCache.track()
        return DataFeed(dataFeed.snapshot.map { it.toNodeInfo() }, dataFeed.updates.map { it.toMapChange() })
    }

    override fun <T : ContractState> vaultQueryBy(criteria: QueryCriteria,
                                                  paging: PageSpecification,
                                                  sorting: Sort,
                                                  contractStateType: Class<out T>): Vault.Page<T> {
        contractStateType.checkIsA<ContractState>()
        return services.vaultService._queryBy(criteria, paging, sorting, contractStateType)
    }

    @RPCReturnsObservables
    override fun <T : ContractState> vaultTrackBy(criteria: QueryCriteria,
                                                  paging: PageSpecification,
                                                  sorting: Sort,
                                                  contractStateType: Class<out T>): DataFeed<Vault.Page<T>, Vault.Update<T>> {
        contractStateType.checkIsA<ContractState>()
        return services.vaultService._trackBy(criteria, paging, sorting, contractStateType)
    }

    @Suppress("OverridingDeprecatedMember", "DEPRECATION")
    override fun internalVerifiedTransactionsSnapshot(): List<SignedTransaction> {
        val (snapshot, updates) = internalVerifiedTransactionsFeed()
        updates.notUsed()
        return snapshot
    }

    @Suppress("OverridingDeprecatedMember")
    override fun internalFindVerifiedTransaction(txnId: SecureHash): SignedTransaction? =
            services.validatedTransactions.getTransaction(txnId)

    @Suppress("OverridingDeprecatedMember")
    override fun internalVerifiedTransactionsFeed(): DataFeed<List<SignedTransaction>, SignedTransaction> {
        return services.validatedTransactions.track()
    }

    override fun stateMachinesSnapshot(): List<StateMachineInfo> {
        val (snapshot, updates) = stateMachinesFeed()
        updates.notUsed()
        return snapshot
    }

    override fun killFlow(id: StateMachineRunId): Boolean = smm.killFlow(id)

    override fun <T> reattachFlowWithClientId(clientId: String): FlowHandleWithClientId<T>? {
        return smm.reattachFlowWithClientId<T>(clientId, context().principal())?.run {
            FlowHandleWithClientIdImpl(id = id, returnValue = resultFuture, clientId = clientId)
        }
    }

    override fun removeClientId(clientId: String): Boolean = smm.removeClientId(clientId, context().principal(), false)

    override fun removeClientIdAsAdmin(clientId: String): Boolean = smm.removeClientId(clientId, context().principal(), true)

    override fun finishedFlowsWithClientIds(): Map<String, Boolean> = smm.finishedFlowsWithClientIds(context().principal(), false)

    override fun finishedFlowsWithClientIdsAsAdmin(): Map<String, Boolean> = smm.finishedFlowsWithClientIds(context().principal(), true)

    override fun stateMachinesFeed(): DataFeed<List<StateMachineInfo>, StateMachineUpdate> {

        val (allStateMachines, changes) = smm.track()
        return DataFeed(
                allStateMachines.map { stateMachineInfoFromFlowLogic(it) },
                changes.map { stateMachineUpdateFromStateMachineChange(it) }
        )
    }

    override fun stateMachineRecordedTransactionMappingSnapshot(): List<StateMachineTransactionMapping> {
        val (snapshot, updates) = stateMachineRecordedTransactionMappingFeed()
        updates.notUsed()
        return snapshot
    }

    override fun stateMachineRecordedTransactionMappingFeed():
            DataFeed<List<StateMachineTransactionMapping>, StateMachineTransactionMapping> {
        return services.stateMachineRecordedTransactionMapping.track()
    }

    override fun nodeInfo(): NodeInfo {
        return services.myInfo
    }

    override fun nodeDiagnosticInfo(): NodeDiagnosticInfo {
        val versionInfo = services.diagnosticsService.nodeVersionInfo()
        val cordapps = services.cordappProvider.cordapps
                .filter { !it.jarPath.toString().endsWith("corda-core-${CordaVersion.releaseVersion}.jar") }
                .map {
                    CordappInfo(
                            type = when (it.info) {
                                is Cordapp.Info.Contract -> "Contract CorDapp"
                                is Cordapp.Info.Workflow -> "Workflow CorDapp"
                                else -> "CorDapp"
                            },
                            name = it.name,
                            shortName = it.info.shortName,
                            minimumPlatformVersion = it.minimumPlatformVersion,
                            targetPlatformVersion = it.targetPlatformVersion,
                            version = it.info.version,
                            vendor = it.info.vendor,
                            licence = it.info.licence,
                            jarHash = it.jarHash)
                }
        return NodeDiagnosticInfo(
                version = versionInfo.releaseVersion,
                revision = versionInfo.revision,
                platformVersion = versionInfo.platformVersion,
                vendor = versionInfo.vendor,
                cordapps = cordapps
        )
    }

    override fun notaryIdentities(): List<Party> {
        return services.networkMapCache.notaryIdentities
    }

    override fun addVaultTransactionNote(txnId: SecureHash, txnNote: String) {
        services.vaultService.addNoteToTransaction(txnId, txnNote)
    }

    override fun getVaultTransactionNotes(txnId: SecureHash): Iterable<String> {
        return services.vaultService.getTransactionNotes(txnId)
    }

    override fun <T> startTrackedFlowDynamic(logicType: Class<out FlowLogic<T>>, vararg args: Any?): FlowProgressHandle<T> {
        val stateMachine = startFlow(logicType, context(), args)
        return FlowProgressHandleImpl(
                id = stateMachine.id,
                returnValue = stateMachine.resultFuture,
                progress = stateMachine.logic?.track()?.updates?.filter { !it.startsWith(STRUCTURAL_STEP_PREFIX) } ?: Observable.empty(),
                stepsTreeIndexFeed = stateMachine.logic?.trackStepsTreeIndex(),
                stepsTreeFeed = stateMachine.logic?.trackStepsTree()
        )
    }

    override fun <T> startFlowDynamic(logicType: Class<out FlowLogic<T>>, vararg args: Any?): FlowHandle<T> {
        val stateMachine = startFlow(logicType, context(), args)
        return FlowHandleImpl(id = stateMachine.id, returnValue = stateMachine.resultFuture)
    }

    override fun <T> startFlowDynamicWithClientId(
        clientId: String,
        logicType: Class<out FlowLogic<T>>,
        vararg args: Any?
    ): FlowHandleWithClientId<T> {
        return startFlow(logicType, context().withClientId(clientId), args).run {
            FlowHandleWithClientIdImpl(id = id, returnValue = resultFuture, clientId = clientId)
        }
    }

    @Suppress("SpreadOperator")
    private fun <T> startFlow(logicType: Class<out FlowLogic<T>>, context: InvocationContext, args: Array<out Any?>): FlowStateMachineHandle<T> {
        if (!logicType.isAnnotationPresent(StartableByRPC::class.java)) throw NonRpcFlowException(logicType)
        if (isFlowsDrainingModeEnabled()) {
            return context.clientId?.let { smm.reattachFlowWithClientId<T>(it, context.principal()) }
                ?: throw RejectedCommandException("Node is draining before shutdown. Cannot start new flows through RPC.")
        }
        return flowStarter.invokeFlowAsync(logicType, context, *args).getOrThrow()
    }

    override fun attachmentExists(id: SecureHash): Boolean {
        return services.attachments.openAttachment(id) != null
    }

    override fun openAttachment(id: SecureHash): InputStream {
        return services.attachments.openAttachment(id)?.open() ?:
            throw MissingAttachmentException("Unable to open attachment with id: $id")
    }

    override fun uploadAttachment(jar: InputStream): SecureHash {
        return services.attachments.privilegedImportAttachment(jar, RPC_UPLOADER, null)
    }

    override fun uploadAttachmentWithMetadata(jar: InputStream, uploader: String, filename: String): SecureHash {
        return services.attachments.privilegedImportAttachment(jar, uploader, filename)
    }

    override fun queryAttachments(query: AttachmentQueryCriteria, sorting: AttachmentSort?): List<AttachmentId> {
        return services.attachments.queryAttachments(query, sorting)
    }

    override fun currentNodeTime(): Instant = Instant.now(services.clock)

    override fun waitUntilNetworkReady(): CordaFuture<Void?> = services.networkMapCache.nodeReady

    override fun wellKnownPartyFromAnonymous(party: AbstractParty): Party? {
        return services.identityService.wellKnownPartyFromAnonymous(party)
    }

    override fun partyFromKey(key: PublicKey): Party? {
        return services.identityService.partyFromKey(key)
    }

    override fun wellKnownPartyFromX500Name(x500Name: CordaX500Name): Party? {
        return services.identityService.wellKnownPartyFromX500Name(x500Name)
    }

    // TODO[DR]: Remove from API
    override fun notaryPartyFromX500Name(x500Name: CordaX500Name): Party? = throw NotImplementedError()

    override fun partiesFromName(query: String, exactMatch: Boolean): Set<Party> {
        return services.identityService.partiesFromName(query, exactMatch)
    }

    // TODO[DR]: Use Party instead of AbstractParty and getPartyByKey()
    override fun nodeInfoFromParty(party: AbstractParty): NodeInfo? {
        return services.networkMapCache.getMemberByKey(party.owningKey)?.toNodeInfo()
    }

    override fun registeredFlows(): List<String> = services.rpcFlows.asSequence().map(Class<*>::getName).sorted().toList()

    override fun clearNetworkMapCache() {
        services.networkMapCache.clearCache()
    }

    override fun refreshNetworkMapCache() = throw NotImplementedError()

    override fun <T : ContractState> vaultQuery(contractStateType: Class<out T>): Vault.Page<T> {
        return vaultQueryBy(QueryCriteria.VaultQueryCriteria(), PageSpecification(), Sort(emptySet()), contractStateType)
    }

    override fun <T : ContractState> vaultQueryByCriteria(
            criteria: QueryCriteria,
            contractStateType: Class<out T>
    ): Vault.Page<T> {
        return vaultQueryBy(criteria, PageSpecification(), Sort(emptySet()), contractStateType)
    }

    override fun <T : ContractState> vaultQueryByWithPagingSpec(
            contractStateType: Class<out T>,
            criteria: QueryCriteria,
            paging: PageSpecification
    ): Vault.Page<T> {
        return vaultQueryBy(criteria, paging, Sort(emptySet()), contractStateType)
    }

    override fun <T : ContractState> vaultQueryByWithSorting(
            contractStateType: Class<out T>,
            criteria: QueryCriteria,
            sorting: Sort
    ): Vault.Page<T> {
        return vaultQueryBy(criteria, PageSpecification(), sorting, contractStateType)
    }

    override fun <T : ContractState> vaultTrack(contractStateType: Class<out T>): DataFeed<Vault.Page<T>, Vault.Update<T>> {
        return vaultTrackBy(QueryCriteria.VaultQueryCriteria(), PageSpecification(), Sort(emptySet()), contractStateType)
    }

    override fun <T : ContractState> vaultTrackByCriteria(
            contractStateType: Class<out T>,
            criteria: QueryCriteria
    ): DataFeed<Vault.Page<T>, Vault.Update<T>> {
        return vaultTrackBy(criteria, PageSpecification(), Sort(emptySet()), contractStateType)
    }

    override fun <T : ContractState> vaultTrackByWithPagingSpec(
            contractStateType: Class<out T>,
            criteria: QueryCriteria,
            paging: PageSpecification
    ): DataFeed<Vault.Page<T>, Vault.Update<T>> {
        return vaultTrackBy(criteria, paging, Sort(emptySet()), contractStateType)
    }

    override fun <T : ContractState> vaultTrackByWithSorting(
            contractStateType: Class<out T>,
            criteria: QueryCriteria,
            sorting: Sort
    ): DataFeed<Vault.Page<T>, Vault.Update<T>> {
        return vaultTrackBy(criteria, PageSpecification(), sorting, contractStateType)
    }

    override fun setFlowsDrainingModeEnabled(enabled: Boolean) = setPersistentDrainingModeProperty(enabled, propagateChange = true)

    override fun isFlowsDrainingModeEnabled() = services.nodeProperties.flowsDrainingMode.isEnabled()

    override fun shutdown() = terminate(false)

    override fun terminate(drainPendingFlows: Boolean) {
        if (drainPendingFlows) {
            logger.info("Waiting for pending flows to complete before shutting down.")
            setFlowsDrainingModeEnabled(true)
            val subscription = @Suppress("DEPRECATION") pendingFlowsCount()
                    .updates
                    .doOnNext { (completed, total) -> logger.info("Pending flows progress before shutdown: $completed / $total.") }
                    .doOnCompleted { setPersistentDrainingModeProperty(enabled = false, propagateChange = false) }
                    .doOnCompleted(::cancelDrainingShutdownHook)
                    .doOnCompleted { logger.info("No more pending flows to drain. Shutting down.") }
                    .doOnCompleted(shutdownNode::invoke)
                    .subscribe(
                            { }, // Nothing to do on each update here, only completion matters.
                            { error ->
                                logger.error("Error while waiting for pending flows to drain in preparation for shutdown. " +
                                        "Cause was: ${error.message}", error)
                            }
                    )
            drainingShutdownHook.set(subscription)
        } else {
            shutdownNode.invoke()
        }
    }

    override fun isWaitingForShutdown() = drainingShutdownHook.get() != null

    override fun close() {
        cancelDrainingShutdownHook()
    }

    private fun cancelDrainingShutdownHook() {
        drainingShutdownHook.getAndSet(null)?.let {
            it.unsubscribe()
            logger.info("Cancelled draining shutdown hook.")
        }
    }

    private fun setPersistentDrainingModeProperty(enabled: Boolean, propagateChange: Boolean) {
        services.nodeProperties.flowsDrainingMode.setEnabled(enabled, propagateChange)
    }

    private fun stateMachineInfoFromFlowLogic(flowLogic: FlowLogic<*>): StateMachineInfo {
        return StateMachineInfo(
                flowLogic.runId,
                flowLogic.javaClass.name,
                flowLogic.stateMachine.context.toFlowInitiator(),
                flowLogic.track(),
                flowLogic.stateMachine.context
        )
    }

    private fun stateMachineUpdateFromStateMachineChange(change: StateMachineManager.Change): StateMachineUpdate {
        return when (change) {
            is StateMachineManager.Change.Add -> StateMachineUpdate.Added(stateMachineInfoFromFlowLogic(change.logic))
            is StateMachineManager.Change.Removed -> StateMachineUpdate.Removed(change.logic.runId, change.result)
        }
    }

    private fun InvocationContext.toFlowInitiator(): FlowInitiator {
        val principal = origin.principal().name
        return when (origin) {
            is InvocationOrigin.RPC -> FlowInitiator.RPC(principal)
            is InvocationOrigin.Peer -> {
                val wellKnownParty = services.identityService.wellKnownPartyFromX500Name((origin as InvocationOrigin.Peer).party)
                wellKnownParty?.let { FlowInitiator.Peer(it) }
                        ?: throw IllegalStateException("Unknown peer with name ${(origin as InvocationOrigin.Peer).party}.")
            }
            is InvocationOrigin.Service -> FlowInitiator.Service(principal)
            InvocationOrigin.Shell -> FlowInitiator.Shell
            is InvocationOrigin.Scheduled -> FlowInitiator.Scheduled((origin as InvocationOrigin.Scheduled).scheduledState)
        }
    }

    /**
     * RPC can be invoked from the shell where the type parameter of any [Class] parameter is lost, so we must
     * explicitly check that the provided [Class] is the one we want.
     */
    private inline fun <reified TARGET> Class<*>.checkIsA() {
        require(TARGET::class.java.isAssignableFrom(this)) { "$name is not a ${TARGET::class.java.name}" }
    }

    private fun InvocationContext.withClientId(clientId: String) = copy(clientId = clientId)
}
