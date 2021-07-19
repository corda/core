package net.corda.node.services.network

import net.corda.core.node.AppServiceHub
import net.corda.core.node.MemberInfo
import net.corda.core.node.services.CordaService
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.node.internal.membership.flows.PublishMemberInfoFlow
import net.corda.node.internal.membership.flows.SyncMembershipGroupCacheFlow

@CordaService
class FlowMembershipGroupService(private val appServiceHub: AppServiceHub) : SingletonSerializeAsToken() {
    fun publishMemberInfo(memberInfo: MemberInfo) = appServiceHub.startFlow(PublishMemberInfoFlow(memberInfo))

    fun syncMembershipGroupCache() = appServiceHub.startFlow(SyncMembershipGroupCacheFlow())
}
