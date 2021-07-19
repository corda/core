package net.corda.core.node.services

import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.messaging.DataFeed
import net.corda.core.node.MemberInfo
import net.corda.core.serialization.CordaSerializable
import rx.Observable
import java.security.PublicKey

interface MembershipGroupCache {
    @CordaSerializable
    sealed class GroupChange {
        abstract val memberInfo: MemberInfo

        data class Added(override val memberInfo: MemberInfo) : GroupChange()
        data class Removed(override val memberInfo: MemberInfo) : GroupChange()
        data class Modified(override val memberInfo: MemberInfo, val previousInfo: MemberInfo) : GroupChange()
    }

    fun track(): DataFeed<List<MemberInfo>, GroupChange>

    val changed: Observable<GroupChange>

    val allMembers: List<MemberInfo>

    fun getMemberByName(name: CordaX500Name): MemberInfo?

    fun getMemberByKey(key: PublicKey): MemberInfo?

    fun getMemberByParty(party: Party): MemberInfo?

    val allParties: List<Party>

    fun getPartyByName(name: CordaX500Name): Party?

    fun getPartyByKey(key: PublicKey): Party?

    // Notaries (legacy)
    val notaryIdentities: List<Party>
    fun isNotary(party: Party): Boolean
    fun isValidatingNotary(party: Party): Boolean
}
