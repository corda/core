package net.corda.bn.flows

import net.corda.bn.schemas.MembershipStateSchemaV1
import net.corda.bn.states.MembershipState
import net.corda.core.contracts.StateAndRef
import net.corda.core.contracts.UniqueIdentifier
import net.corda.core.identity.Party
import net.corda.core.node.ServiceHub
import net.corda.core.node.services.CordaService
import net.corda.core.node.services.Vault
import net.corda.core.node.services.queryBy
import net.corda.core.node.services.vault.QueryCriteria
import net.corda.core.node.services.vault.builder
import net.corda.core.serialization.SingletonSerializeAsToken

@CordaService
class DatabaseService(private val serviceHub: ServiceHub) : SingletonSerializeAsToken() {

    fun getMembership(networkId: String, party: Party): StateAndRef<MembershipState<*, *>>? {
        val criteria = QueryCriteria.VaultQueryCriteria(Vault.StateStatus.UNCONSUMED)
                .and(networkIdCriteria(networkId))
                .and(identityCriteria(party))
        val states = serviceHub.vaultService.queryBy<MembershipState<*, *>>(criteria).states
        return states.maxBy { it.state.data.modified }
    }

    fun getMembership(linearId: UniqueIdentifier): StateAndRef<MembershipState<*, *>>? {
        val criteria = QueryCriteria.VaultQueryCriteria(Vault.StateStatus.UNCONSUMED)
                .and(linearIdCriteria(linearId))
        val states = serviceHub.vaultService.queryBy<MembershipState<*, *>>(criteria).states
        return states.maxBy { it.state.data.modified }
    }

    fun getMembersAuthorisedToActivateMembership(networkId: String): List<Party> = emptyList()

    fun getMembersAuthorisedToModifyMembershipStatus(networkId: String): List<Party> = emptyList()

    fun getMembersAuthorisedToModifyMembership(networkId: String): List<Party> = emptyList()

    private fun networkIdCriteria(networkID: String) = QueryCriteria.VaultCustomQueryCriteria(builder { MembershipStateSchemaV1.PersistentMembershipState::networkId.equal(networkID) })
    private fun identityCriteria(cordaIdentity: Party) = QueryCriteria.VaultCustomQueryCriteria(builder { MembershipStateSchemaV1.PersistentMembershipState::cordaIdentity.equal(cordaIdentity) })
    private fun linearIdCriteria(linearId: UniqueIdentifier) = QueryCriteria.LinearStateQueryCriteria(linearId = listOf(linearId))
}