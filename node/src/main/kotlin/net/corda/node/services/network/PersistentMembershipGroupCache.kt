package net.corda.node.services.network

import net.corda.core.crypto.CompositeKey
import net.corda.core.crypto.toStringShort
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.NamedCacheFactory
import net.corda.core.internal.bufferUntilSubscribed
import net.corda.core.internal.concurrent.OpenFuture
import net.corda.core.internal.concurrent.openFuture
import net.corda.core.messaging.DataFeed
import net.corda.core.node.MemberInfo
import net.corda.core.node.NotaryInfo
import net.corda.core.node.services.MembershipGroupCache
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.debug
import net.corda.node.internal.schemas.MemberInfoSchemaV1.PersistentMemberInfo
import net.corda.node.services.api.IdentityServiceInternal
import net.corda.node.services.api.MembershipGroupCacheInternal
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.nodeapi.internal.persistence.bufferUntilDatabaseCommit
import net.corda.nodeapi.internal.persistence.wrapWithDatabaseTransaction
import rx.Observable
import rx.subjects.PublishSubject
import java.security.PublicKey

@Suppress("TooManyFunctions")
class PersistentMembershipGroupCache(
        @Suppress("UNUSED_PARAMETER") cacheFactory: NamedCacheFactory,
        private val database: CordaPersistence,
        private val identityService: IdentityServiceInternal
) : MembershipGroupCacheInternal, SingletonSerializeAsToken() {

    companion object {
        private val logger = contextLogger()
    }

    override val nodeReady: OpenFuture<Void?> = openFuture()
    private val _changed = PublishSubject.create<MembershipGroupCache.GroupChange>()
    override val changed: Observable<MembershipGroupCache.GroupChange> = _changed.wrapWithDatabaseTransaction()
    private val changePublisher: rx.Observer<MembershipGroupCache.GroupChange> get() = _changed.bufferUntilDatabaseCommit()

    override val allMembers: List<MemberInfo>
        get() = queryAllPersistentInfo().map { it.toMemberInfo() }
    override val allParties: List<Party> get() = allMembers.map { it.party }

    private lateinit var notaries: List<NotaryInfo>
    private var _mgmInfo: MemberInfo? = null
    override val mgmInfo: MemberInfo get() = _mgmInfo!!

    fun start(notaries: List<NotaryInfo>, mgmInfo: MemberInfo? = null) {
        this.notaries = notaries
        this._mgmInfo = mgmInfo
    }

    override fun track(): DataFeed<List<MemberInfo>, MembershipGroupCache.GroupChange> {
        synchronized(_changed) {
            return DataFeed(allMembers, _changed.bufferUntilSubscribed().wrapWithDatabaseTransaction())
        }
    }

    override fun addOrUpdateMember(memberInfo: MemberInfo) {
        synchronized(_changed) {
            database.transaction {
                val oldMemberInfo = queryPersistentInfoByName(memberInfo.party.name)?.toMemberInfo()
                if (oldMemberInfo != memberInfo) {
                    val persistentMemberInfo = PersistentMemberInfo.fromMemberInfo(memberInfo)
                    // TODO[DR]: Remove use of identityService
                    identityService.registerIdentity(getTestPartyAndCertificate(memberInfo.party))
                    session.merge(persistentMemberInfo)

                    val change = oldMemberInfo?.let {
                        MembershipGroupCache.GroupChange.Modified(memberInfo, oldMemberInfo)
                    } ?: MembershipGroupCache.GroupChange.Added(memberInfo)
                    changePublisher.onNext(change)
                }
            }
            identityService.invalidateCaches(memberInfo.party.name)
        }
    }

    override fun addOrUpdateMembers(memberInfoList: List<MemberInfo>) {
        memberInfoList.forEach {
            addOrUpdateMember(it)
        }
    }

    override fun removeMember(memberInfo: MemberInfo) {
        synchronized(_changed) {
            database.transaction {
                val dbEntry = queryPersistentInfoByName(memberInfo.party.name)
                dbEntry?.let {
                    session.remove(it)
                    changePublisher.onNext(MembershipGroupCache.GroupChange.Removed(it.toMemberInfo()))
                }
            }
            identityService.invalidateCaches(memberInfo.party.name)
        }
    }

    override fun clearCache() {
        logger.info("Clearing Membership Group Cache entries")
        database.transaction {
            val result = queryAllPersistentInfo()
            logger.debug { "Number of member infos to be cleared: ${result.size}" }
            for (memberInfo in result) session.remove(memberInfo)
        }
    }

    override fun getPartyByName(name: CordaX500Name): Party? {
        return getMemberByName(name)?.party
    }

    override fun getPartyByKey(key: PublicKey): Party? {
        return getMemberByKey(key)?.party
    }

    override fun getMemberByKey(key: PublicKey): MemberInfo? {
        return getMemberByKeyHash(key.toStringShort())
    }

    override fun getMemberByName(name: CordaX500Name): MemberInfo? {
        return queryPersistentInfoByName(name)?.toMemberInfo()
                ?: _mgmInfo?.takeIf { name == it.party.name }
    }

    override fun getMemberByParty(party: Party): MemberInfo? {
        return getMemberByKey(party.owningKey)?.takeIf { party.name == it.party.name }
    }

    private fun getMemberByKeyHash(keyHash: String): MemberInfo? {
        return queryPersistentInfoByKeyHash(keyHash)?.toMemberInfo()
                ?: _mgmInfo?.takeIf { keyHash == it.party.owningKey.toStringShort() }
    }

    override fun getMembersByKeyHash(keyHash: String): List<MemberInfo> {
        val memberInfo = getMemberByKeyHash(keyHash) ?: return listOf()
        val owningKey = memberInfo.party.owningKey
        return if (owningKey is CompositeKey) {
            owningKey.leafKeys.mapNotNull { getMemberByKey(it) }
        } else {
            listOf(memberInfo)
        }
    }

    private fun queryPersistentInfoByKeyHash(keyHash: String): PersistentMemberInfo? = database.transaction {
        session.createQuery(
                "SELECT m FROM ${PersistentMemberInfo::class.java.name} m INNER JOIN m.keys k WHERE k.publicKeyHash = :keyHash",
                PersistentMemberInfo::class.java
        ).setParameter("keyHash", keyHash)
                .resultList
                .singleOrNull()
    }

    private fun queryPersistentInfoByName(name: CordaX500Name): PersistentMemberInfo? = database.transaction {
        session.createQuery(
                "SELECT m FROM ${PersistentMemberInfo::class.java.name} m WHERE name = :name",
                PersistentMemberInfo::class.java
        ).setParameter("name", name.toString())
                .resultList
                .singleOrNull()
    }

    private fun queryAllPersistentInfo(): List<PersistentMemberInfo> = database.transaction {
        session.createQuery(
                "SELECT m FROM ${PersistentMemberInfo::class.java.name} m",
                PersistentMemberInfo::class.java
        ).resultList
    }

    override val notaryIdentities: List<Party> get() = notaries.map { it.identity }

    override fun isNotary(party: Party): Boolean = notaries.any { it.identity == party }

    override fun isValidatingNotary(party: Party): Boolean = notaries.any { it.validating && it.identity == party }
}
