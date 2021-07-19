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
import net.corda.node.services.api.MembershipRequest

@InitiatingFlow
@StartableByService
class PublishMemberInfoFlow(private val memberInfo: MemberInfo) : MembershipGroupManagementFlow() {

    @Suspendable
    override fun call() {
        val session = initiateFlow(membershipGroupCache.mgmInfo.party)
        session.send(MembershipRequest(memberInfo).sign(serviceHub.keyManagementService, ourIdentity.owningKey))

        session.receive<SignedData<MemberInfo>>().unwrap { it.verified() }
    }
}

@InitiatedBy(PublishMemberInfoFlow::class)
class PublishMemberInfoResponderFlow(private val session: FlowSession) : MembershipGroupManagementFlow() {

    @Suspendable
    override fun call() {
        authorise()

        val request = session.receive<SignedData<MembershipRequest>>().unwrap { it }.verified()
        val memberInfo = request.memberInfo.copy(status = MemberStatus.ACTIVE)
        membershipGroupCache.addOrUpdateMember(memberInfo)

        val signedMemberInfo = memberInfo.sign(serviceHub.keyManagementService, ourIdentity.owningKey)
        session.send(signedMemberInfo)
    }
}