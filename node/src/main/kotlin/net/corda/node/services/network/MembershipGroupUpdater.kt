package net.corda.node.services.network

import com.google.common.util.concurrent.MoreExecutors
import net.corda.core.internal.concurrent.map
import net.corda.core.internal.concurrent.thenMatch
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.seconds
import net.corda.node.internal.LifecycleSupport
import net.corda.node.services.api.ServiceHubInternal
import net.corda.node.utilities.NamedThreadFactory
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

class MembershipGroupUpdater(private val serviceHub: ServiceHubInternal) : LifecycleSupport {
    companion object {
        private val logger = contextLogger()
        private val updateInterval = 20.seconds
    }

    private val membershipGroupService by lazy { serviceHub.cordaService(FlowMembershipGroupService::class.java) }

    private val executor = ScheduledThreadPoolExecutor(1, NamedThreadFactory("MembershipGroupUpdater")).apply {
        executeExistingDelayedTasksAfterShutdownPolicy = false
    }

    override val started = serviceHub.networkMapCache.nodeReady.isDone

    override fun stop() {
        MoreExecutors.shutdownAndAwaitTermination(executor, updateInterval.toMillis(), TimeUnit.MILLISECONDS)
    }

    override fun start() {
        if (serviceHub.mgm) {
            logger.info("Membership Group Manager started")
            serviceHub.networkMapCache.nodeReady.set(null)
        } else {
            logger.info("Start publishing MemberInfo")
            membershipGroupService.publishMemberInfo(serviceHub.myMemberInfo).returnValue.map {
                logger.info("Done publishing MemberInfo")
                executor.execute { syncMembershipGroupCache(true) }
            }
        }
    }

    private fun syncMembershipGroupCache(firstRun: Boolean = false) {
        if (firstRun) {
            logger.info("Start updating membership group cache")
        }
        membershipGroupService.syncMembershipGroupCache().returnValue.thenMatch({
            if (firstRun) {
                logger.info("Done updating membership group cache")
                serviceHub.networkMapCache.nodeReady.set(null)
            }
            executor.schedule({ syncMembershipGroupCache() }, updateInterval.toMillis(), TimeUnit.MILLISECONDS)
        }, {
            logger.error("Failed to update membership group cache, will retry", it)
            executor.schedule({ syncMembershipGroupCache(firstRun) }, updateInterval.toMillis(), TimeUnit.MILLISECONDS)
        })
    }
}
