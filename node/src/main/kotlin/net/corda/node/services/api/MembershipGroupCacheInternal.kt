package net.corda.node.services.api

import net.corda.core.crypto.CompositeKey
import net.corda.core.internal.concurrent.OpenFuture
import net.corda.core.node.MemberInfo
import net.corda.core.node.NodeInfo
import net.corda.core.node.services.MembershipGroupCache
import net.corda.core.serialization.CordaSerializable
import net.corda.node.services.network.memberInfo
import net.corda.node.services.network.toMemberInfo

// TODO[DR]: Move MembershipRequest to MembershipGroupManagementFlow when not needed by SingleThreadedStateMachineManager.
@CordaSerializable
data class MembershipRequest(val memberInfo: MemberInfo)

interface MembershipGroupCacheInternal : MembershipGroupCache {
    val mgmInfo: MemberInfo

    val nodeReady: OpenFuture<Void?>

    fun addOrUpdateMember(memberInfo: MemberInfo)

    fun addOrUpdateMembers(memberInfoList: List<MemberInfo>)

    fun removeMember(memberInfo: MemberInfo)

    fun getMembersByKeyHash(keyHash: String): List<MemberInfo>

    fun clearCache()

    // TODO[DR]: Legacy, remove later.
    fun addOrUpdateNode(nodeInfo: NodeInfo) {
        addOrUpdateMember(nodeInfo.toMemberInfo())
        if (nodeInfo.legalIdentities.size > 1 && nodeInfo.legalIdentities.last().owningKey is CompositeKey) {
            addOrUpdateMember(memberInfo(nodeInfo.legalIdentities.last()))
        }
    }
}