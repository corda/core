package net.corda.node.internal.membership.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.crypto.SignedData
import net.corda.core.flows.FlowSession
import net.corda.core.flows.InitiatedBy
import net.corda.core.flows.InitiatingFlow
import net.corda.core.flows.StartableByService
import net.corda.core.node.MemberInfo
import net.corda.core.node.MemberStatus
import net.corda.core.utilities.unwrap
import net.corda.node.internal.membership.sign

@InitiatingFlow
@StartableByService
class SyncMembershipGroupCacheFlow : MembershipGroupManagementFlow() {

    @Suspendable
    override fun call() {
        val session = initiateFlow(membershipGroupCache.mgmInfo.party)
        session.send(Unit)

        val updatedMembers = session.receive<SignedData<List<MemberInfo>>>().unwrap { it.verified() }
        val removedMembers = membershipGroupCache.allMembers.filter { memberInfo ->
            updatedMembers.find { it.party.name == memberInfo.party.name } == null
        }

        membershipGroupCache.addOrUpdateMembers(updatedMembers)
        removedMembers.forEach { membershipGroupCache.removeMember(it) }
    }
}

@InitiatedBy(SyncMembershipGroupCacheFlow::class)
class SyncMembershipGroupCacheResponderFlow(private val session: FlowSession) : MembershipGroupManagementFlow() {

    @Suspendable
    override fun call() {
        // Add check that session.counterpart is registered
        authorise()
        session.receive<Unit>()

        val members = membershipGroupCache.allMembers.filterNot { it.status == MemberStatus.PENDING }

        val signedMembers = members.sign(serviceHub.keyManagementService, ourIdentity.owningKey)
        session.send(signedMembers)
    }
}