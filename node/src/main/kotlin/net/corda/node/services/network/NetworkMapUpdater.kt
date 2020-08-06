package net.corda.node.services.network

import com.google.common.util.concurrent.MoreExecutors
import net.corda.cliutils.ExitCodes
import net.corda.core.CordaRuntimeException
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.SignedData
import net.corda.core.crypto.sha256
import net.corda.core.internal.NetworkParametersStorage
import net.corda.core.internal.VisibleForTesting
import net.corda.core.internal.copyTo
import net.corda.core.internal.div
import net.corda.core.internal.exists
import net.corda.core.internal.readObject
import net.corda.core.internal.sign
import net.corda.core.messaging.DataFeed
import net.corda.core.messaging.ParametersUpdateInfo
import net.corda.core.node.AutoAcceptable
import net.corda.core.node.NetworkParameters
import net.corda.core.node.NodeInfo
import net.corda.core.node.services.KeyManagementService
import net.corda.core.serialization.serialize
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.minutes
import net.corda.node.services.api.NetworkMapCacheInternal
import net.corda.node.services.config.NetworkParameterAcceptanceSettings
import net.corda.node.utilities.NamedThreadFactory
import net.corda.nodeapi.exceptions.OutdatedNetworkParameterHashException
import net.corda.nodeapi.internal.SignedNodeInfo
import net.corda.nodeapi.internal.network.NETWORK_PARAMS_FILE_NAME
import net.corda.nodeapi.internal.network.NETWORK_PARAMS_UPDATE_FILE_NAME
import net.corda.nodeapi.internal.network.NetworkMap
import net.corda.nodeapi.internal.network.ParametersUpdate
import net.corda.nodeapi.internal.network.SignedNetworkParameters
import net.corda.nodeapi.internal.network.verifiedNetworkParametersCert
import rx.Observable
import rx.Subscription
import rx.subjects.PublishSubject
import java.lang.Integer.max
import java.lang.Integer.min
import java.lang.reflect.Method
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.security.cert.X509Certificate
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Consumer
import java.util.function.Supplier
import kotlin.reflect.KProperty1
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.jvm.javaGetter
import kotlin.system.exitProcess

class NetworkMapUpdater(private val networkMapCache: NetworkMapCacheInternal,
                        private val nodeInfoWatcher: NodeInfoWatcher,
                        private val networkMapClient: NetworkMapClient?,
                        private val baseDirectory: Path,
                        private val extraNetworkMapKeys: List<UUID>,
                        private val networkParametersStorage: NetworkParametersStorage
) : AutoCloseable, NetworkParameterUpdateListener {
    companion object {
        private val logger = contextLogger()
        private val defaultRetryInterval = 1.minutes
        private const val bulkNodeInfoFetchThreshold = 50
    }

    private val parametersUpdatesTrack = PublishSubject.create<ParametersUpdateInfo>()
    private val networkMapPoller = ScheduledThreadPoolExecutor(1, NamedThreadFactory("Network Map Updater Thread")).apply {
        executeExistingDelayedTasksAfterShutdownPolicy = false
    }
    private var newNetworkParameters: Pair<ParametersUpdate, SignedNetworkParameters>? = null
    private val fileWatcherSubscription = AtomicReference<Subscription?>()
    private var autoAcceptNetworkParameters: Boolean = true
    private lateinit var trustRoot: X509Certificate
    @Volatile
    private lateinit var currentParametersHash: SecureHash
    private lateinit var ourNodeInfo: SignedNodeInfo
    private lateinit var ourNodeInfoHash: SecureHash

    private lateinit var networkParameters: NetworkParameters
    private lateinit var keyManagementService: KeyManagementService
    private lateinit var excludedAutoAcceptNetworkParameters: Set<String>
    private var networkParametersHotloader: NetworkParametersHotloader? = null

    private val _updateComplete: PublishSubject<Unit> = PublishSubject.create()
    val updateComplete: Observable<Unit>
        get() = _updateComplete

    override fun close() {
        fileWatcherSubscription.updateAndGet { subscription ->
            subscription?.apply {
                if (!isUnsubscribed) {
                    unsubscribe()
                }
            }
            null // sets the atomic ref to null
        }
        MoreExecutors.shutdownAndAwaitTermination(networkMapPoller, 50, TimeUnit.SECONDS)
    }
    @Suppress("LongParameterList")
    fun start(trustRoot: X509Certificate,
              currentParametersHash: SecureHash,
              ourNodeInfo: SignedNodeInfo,
              networkParameters: NetworkParameters,
              keyManagementService: KeyManagementService,
              networkParameterAcceptanceSettings: NetworkParameterAcceptanceSettings,
              networkParametersHotloader: NetworkParametersHotloader?
             ) {
        fileWatcherSubscription.updateAndGet { subscription ->
            require(subscription == null) { "Should not call this method twice" }
            this.trustRoot = trustRoot
            this.currentParametersHash = currentParametersHash
            this.ourNodeInfo = ourNodeInfo
            this.ourNodeInfoHash = ourNodeInfo.raw.hash
            this.networkParameters = networkParameters
            this.keyManagementService = keyManagementService
            this.autoAcceptNetworkParameters = networkParameterAcceptanceSettings.autoAcceptEnabled
            this.excludedAutoAcceptNetworkParameters = networkParameterAcceptanceSettings.excludedAutoAcceptableParameters
            this.networkParametersHotloader = networkParametersHotloader


            val autoAcceptNetworkParametersNames = autoAcceptablePropertyNames - excludedAutoAcceptNetworkParameters
            if (autoAcceptNetworkParameters && autoAcceptNetworkParametersNames.isNotEmpty()) {
                logger.info("Auto-accept enabled for network parameter changes which modify only: $autoAcceptNetworkParametersNames")
            }
            watchForNodeInfoFiles().also {
                if (networkMapClient != null) {
                    watchHttpNetworkMap()
                }
            }
        }
    }

    private fun watchForNodeInfoFiles(): Subscription {
        return nodeInfoWatcher
                .nodeInfoUpdates()
                .subscribe {
                    for (update in it) {
                        when (update) {
                            is NodeInfoUpdate.Add -> networkMapCache.addOrUpdateNode(update.nodeInfo)
                            is NodeInfoUpdate.Remove -> {
                                if (update.hash != ourNodeInfoHash) {
                                    val nodeInfo = networkMapCache.getNodeByHash(update.hash)
                                    nodeInfo?.let(networkMapCache::removeNode)
                                }
                            }
                        }
                    }
                    if (networkMapClient == null) {
                        // Mark the network map cache as ready on a successful poll of the node infos dir if not using
                        // the HTTP network map even if there aren't any node infos
                        networkMapCache.nodeReady.set(null)
                        _updateComplete.onNext(Unit)
                    }
                }
    }

    private fun watchHttpNetworkMap() {
        // The check may be expensive, so always run it in the background even the first time.
        networkMapPoller.submit(object : Runnable {
            override fun run() {
                val nextScheduleDelay = try {
                    updateNetworkMapCache()
                } catch (e: Exception) {
                    logger.warn("Error encountered while updating network map, will retry in $defaultRetryInterval", e)
                    defaultRetryInterval
                }
                // Schedule the next update.
                networkMapPoller.schedule(this, nextScheduleDelay.toMillis(), TimeUnit.MILLISECONDS)
            }
        })
    }

    fun trackParametersUpdate(): DataFeed<ParametersUpdateInfo?, ParametersUpdateInfo> {
        val currentUpdateInfo = newNetworkParameters?.let {
            ParametersUpdateInfo(it.first.newParametersHash, it.second.verified(), it.first.description, it.first.updateDeadline)
        }
        return DataFeed(currentUpdateInfo, parametersUpdatesTrack)
    }

    fun updateNetworkMapCache(): Duration {
        if (networkMapClient == null) {
            throw CordaRuntimeException("Network map cache can be updated only if network map/compatibility zone URL is specified")
        }
        val (globalNetworkMap, cacheTimeout, version) = networkMapClient.getNetworkMap()
        globalNetworkMap.parametersUpdate?.let { handleUpdateNetworkParameters(networkMapClient, it) }
        val additionalHashes = getPrivateNetworkNodeHashes(version)
        val allHashesFromNetworkMap = (globalNetworkMap.nodeInfoHashes + additionalHashes).toSet()
        if (currentParametersHash != globalNetworkMap.networkParameterHash) {
            hotloadOrExitOnParametersMismatch(globalNetworkMap)
        }
        // Calculate any nodes that are now gone and remove _only_ them from the cache
        // NOTE: We won't remove them until after the add/update cycle as only then will we definitely know which nodes are no longer
        // in the network
        val allNodeHashes = networkMapCache.allNodeHashes
        val nodeHashesToBeDeleted = (allNodeHashes - allHashesFromNetworkMap - nodeInfoWatcher.processedNodeInfoHashes)
                .filter { it != ourNodeInfoHash }
        // enforce bulk fetch when no other nodes are known or unknown nodes count is less than threshold
        if (version == "1" || (allNodeHashes.size > 1 && (allHashesFromNetworkMap - allNodeHashes).size < bulkNodeInfoFetchThreshold))
            updateNodeInfosV1(allHashesFromNetworkMap, allNodeHashes, networkMapClient)
        else
            updateNodeInfos(allHashesFromNetworkMap)
        // NOTE: We remove nodes after any new/updates because updated nodes will have a new hash and, therefore, any
        // nodes that we can actually pull out of the cache (with the old hashes) should be a truly removed node.
        nodeHashesToBeDeleted.mapNotNull { networkMapCache.getNodeByHash(it) }.forEach(networkMapCache::removeNode)

        // Mark the network map cache as ready on a successful poll of the HTTP network map, even on the odd chance that
        // it's empty
        networkMapCache.nodeReady.set(null)
        _updateComplete.onNext(Unit)
        return cacheTimeout
    }

    private fun updateNodeInfos(allHashesFromNetworkMap: Set<SecureHash>) {
        val networkMapDownloadStartTime = System.currentTimeMillis()
        val nodeInfos = try {
            networkMapClient!!.getNodeInfos()
        } catch (e: Exception) {
            logger.warn("Error encountered when downloading node infos", e)
            emptyList<NodeInfo>()
        }
        (allHashesFromNetworkMap - nodeInfos.map { it.serialize().sha256() }).forEach {
            logger.warn("Error encountered when downloading node info '$it', skipping...")
        }
        networkMapCache.addOrUpdateNodes(nodeInfos)
        logger.info("Fetched: ${nodeInfos.size} using 1 bulk request in ${System.currentTimeMillis() - networkMapDownloadStartTime}ms")
    }

    private fun updateNodeInfosV1(allHashesFromNetworkMap: Set<SecureHash>, allNodeHashes: List<SecureHash>, networkMapClient: NetworkMapClient) {
        //at the moment we use a blocking HTTP library - but under the covers, the OS will interleave threads waiting for IO
        //as HTTP GET is mostly IO bound, use more threads than CPU's
        //maximum threads to use = 24, as if we did not limit this on large machines it could result in 100's of concurrent requests
        val threadsToUseForNetworkMapDownload = min(Runtime.getRuntime().availableProcessors() * 4, 24)
        val executorToUseForDownloadingNodeInfos = Executors.newFixedThreadPool(threadsToUseForNetworkMapDownload, NamedThreadFactory("NetworkMapUpdaterNodeInfoDownloadThread"))
        //DB insert is single threaded - use a single threaded executor for it.
        val executorToUseForInsertionIntoDB = Executors.newSingleThreadExecutor(NamedThreadFactory("NetworkMapUpdateDBInsertThread"))
        val hashesToFetch = (allHashesFromNetworkMap - allNodeHashes)
        val networkMapDownloadStartTime = System.currentTimeMillis()
        if (hashesToFetch.isNotEmpty()) {
            val networkMapDownloadFutures = hashesToFetch.chunked(max(hashesToFetch.size / threadsToUseForNetworkMapDownload, 1))
                    .map { nodeInfosToGet ->
                        //for a set of chunked hashes, get the nodeInfo for each hash
                        CompletableFuture.supplyAsync(Supplier<List<NodeInfo>> {
                            nodeInfosToGet.mapNotNull { nodeInfo ->
                                try {
                                    networkMapClient.getNodeInfo(nodeInfo)
                                } catch (e: Exception) {
                                    // Failure to retrieve one node info shouldn't stop the whole update, log and return null instead.
                                    logger.warn("Error encountered when downloading node info '$nodeInfo', skipping...", e)
                                    null
                                }
                            }
                        }, executorToUseForDownloadingNodeInfos).thenAcceptAsync(Consumer { retrievedNodeInfos ->
                            // Add new node info to the network map cache, these could be new node info or modification of node info for existing nodes.
                            networkMapCache.addOrUpdateNodes(retrievedNodeInfos)
                        }, executorToUseForInsertionIntoDB)
                    }.toTypedArray()
            //wait for all the futures to complete
            val waitForAllHashes = CompletableFuture.allOf(*networkMapDownloadFutures)
            waitForAllHashes.thenRunAsync {
                logger.info("Fetched: ${hashesToFetch.size} using $threadsToUseForNetworkMapDownload Threads in ${System.currentTimeMillis() - networkMapDownloadStartTime}ms")
                executorToUseForDownloadingNodeInfos.shutdown()
                executorToUseForInsertionIntoDB.shutdown()
            }.getOrThrow()
        }
    }

    private fun getPrivateNetworkNodeHashes(version: String): List<SecureHash> {
        // private networks are not supported by latest versions of Network Map
        // for compatibility reasons, this call is still present for new nodes that communicate with old Network Map service versions
        // but can be omitted if we know that the version of the Network Map is recent enough
        return if (version == "1") {
            extraNetworkMapKeys.flatMap {
                try {
                    networkMapClient!!.getNetworkMap(it).payload.nodeInfoHashes
                } catch (e: Exception) {
                    // Failure to retrieve one network map using UUID shouldn't stop the whole update.
                    logger.warn("Error encountered when downloading network map with uuid '$it', skipping...", e)
                    emptyList<SecureHash>()
                }
            }
        } else {
            emptyList()
        }
    }

    private fun hotloadOrExitOnParametersMismatch(networkMap: NetworkMap) {
        val updatesFile = baseDirectory / NETWORK_PARAMS_UPDATE_FILE_NAME
        val newParameterHash = networkMap.networkParameterHash
        val nodeAcceptedNewParameters = updatesFile.exists() && newParameterHash == updatesFile.readObject<SignedNetworkParameters>().raw.hash

        if (!nodeAcceptedNewParameters) {
            logger.error(
                    """Node is using network parameters with hash $currentParametersHash but the network map is advertising ${networkMap.networkParameterHash}.
To resolve this mismatch, and move to the current parameters, delete the $NETWORK_PARAMS_FILE_NAME file from the node's directory and restart.
The node will shutdown now.""")
            exitProcess(ExitCodes.FAILURE)
        }

        val hotloadSucceeded = networkParametersHotloader!!.attemptHotload(newParameterHash)
        if (!hotloadSucceeded) {
            logger.info("Flag day occurred. Network map switched to the new network parameters: " +
                    "${networkMap.networkParameterHash}. Node will shutdown now and needs to be started again.")
            exitProcess(ExitCodes.SUCCESS)
        }
        currentParametersHash = newParameterHash
    }

    private fun handleUpdateNetworkParameters(networkMapClient: NetworkMapClient, update: ParametersUpdate) {
        if (update.newParametersHash == newNetworkParameters?.first?.newParametersHash) {
            // This update was handled already.
            return
        }
        val newSignedNetParams = networkMapClient.getNetworkParameters(update.newParametersHash)
        val newNetParams = newSignedNetParams.verifiedNetworkParametersCert(trustRoot)
        networkParametersStorage.saveParameters(newSignedNetParams)
        logger.info("Downloaded new network parameters: $newNetParams from the update: $update")
        newNetworkParameters = Pair(update, newSignedNetParams)
        val updateInfo = ParametersUpdateInfo(
                update.newParametersHash,
                newNetParams,
                update.description,
                update.updateDeadline)

        if (autoAcceptNetworkParameters && networkParameters.canAutoAccept(newNetParams, excludedAutoAcceptNetworkParameters)) {
            logger.info("Auto-accepting network parameter update ${update.newParametersHash}")
            acceptNewNetworkParameters(update.newParametersHash) { hash ->
                hash.serialize().sign { keyManagementService.sign(it.bytes, ourNodeInfo.verified().legalIdentities[0].owningKey) }
            }
        } else {
            parametersUpdatesTrack.onNext(updateInfo)
        }
    }

    fun acceptNewNetworkParameters(parametersHash: SecureHash, sign: (SecureHash) -> SignedData<SecureHash>) {
        networkMapClient
                ?: throw IllegalStateException("Network parameters updates are not supported without compatibility zone configured")
        // TODO This scenario will happen if node was restarted and didn't download parameters yet, but we accepted them.
        // Add persisting of newest parameters from update.
        val (update, signedNewNetParams) = requireNotNull(newNetworkParameters) { "Couldn't find parameters update for the hash: $parametersHash" }
        // We should check that we sign the right data structure hash.
        val newNetParams = signedNewNetParams.verifiedNetworkParametersCert(trustRoot)
        val newParametersHash = signedNewNetParams.raw.hash
        if (parametersHash == newParametersHash) {
            // The latest parameters have priority.
            signedNewNetParams.serialize()
                    .open()
                    .copyTo(baseDirectory / NETWORK_PARAMS_UPDATE_FILE_NAME, StandardCopyOption.REPLACE_EXISTING)
            networkMapClient.ackNetworkParametersUpdate(sign(parametersHash))
            logger.info("Accepted network parameter update $update: $newNetParams")
        } else {
            throw OutdatedNetworkParameterHashException(parametersHash, newParametersHash)
        }
    }

    override fun onNewNetworkParameters(networkParameters: NetworkParameters) {
        this.networkParameters = networkParameters
    }
}

private val memberPropertyPartition = NetworkParameters::class.declaredMemberProperties.partition { it.isAutoAcceptable() }
private val autoAcceptableNamesAndGetters = memberPropertyPartition.first.associateBy({ it.name }, { it.javaGetter })
@VisibleForTesting
internal val autoAcceptablePropertyNames = autoAcceptableNamesAndGetters.keys
private val nonAutoAcceptableGetters = memberPropertyPartition.second.map { it.javaGetter }

/**
 * Returns true if the only properties changed in [newNetworkParameters] are [AutoAcceptable] and not
 * included in the [excludedParameterNames]
 */
@VisibleForTesting
internal fun NetworkParameters.canAutoAccept(newNetworkParameters: NetworkParameters, excludedParameterNames: Set<String>): Boolean {
    return nonAutoAcceptableGetters.none { valueChanged(newNetworkParameters, it) } &&
            autoAcceptableNamesAndGetters.none { it.key in excludedParameterNames && valueChanged(newNetworkParameters, it.value) }
}

private fun KProperty1<out NetworkParameters, Any?>.isAutoAcceptable(): Boolean = findAnnotation<AutoAcceptable>() != null

internal fun NetworkParameters.valueChanged(newNetworkParameters: NetworkParameters, getter: Method?): Boolean {
    val propertyValue = getter?.invoke(this)
    val newPropertyValue = getter?.invoke(newNetworkParameters)
    return propertyValue != newPropertyValue
}
