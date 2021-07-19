package net.corda.node.internal.membership.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.flows.FlowException
import net.corda.core.flows.FlowLogic
import net.corda.node.services.api.MembershipGroupCacheInternal
import net.corda.node.services.api.ServiceHubInternal
import net.corda.node.services.network.mgm

abstract class MembershipGroupManagementFlow : FlowLogic<Unit>() {

    protected val membershipGroupCache: MembershipGroupCacheInternal
        get() = (serviceHub as ServiceHubInternal).networkMapCache

    @Suspendable
    protected fun authorise() {
        if (!(serviceHub as ServiceHubInternal).mgm) {
            throw FlowException("$ourIdentity is not manager of the Membership Group")
        }
    }
}
