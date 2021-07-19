package net.corda.node.services.network

import net.corda.node.internal.LifecycleSupport
import net.corda.node.services.api.MembershipGroupCacheInternal
import rx.Scheduler
import java.nio.file.Path
import java.time.Duration

// TODO[DR]: Temporarily using NodeInfo
class MembershipGroupFileUpdater(private val networkMapCache: MembershipGroupCacheInternal,
                                 nodePath: Path,
                                 scheduler: Scheduler,
                                 pollInterval: Duration) : LifecycleSupport {
    override val started = networkMapCache.nodeReady.isDone

    private val nodeInfoWatcher = NodeInfoWatcher(nodePath, scheduler, pollInterval)

    override fun start() {
        nodeInfoWatcher.nodeInfoUpdates().subscribe { list ->
            // TODO[DR]: Handle removals
            list.filterIsInstance<NodeInfoUpdate.Add>().forEach { networkMapCache.addOrUpdateNode(it.nodeInfo) }
            networkMapCache.nodeReady.set(null)
        }
    }

    override fun stop() {
    }
}