package net.corda.node.services.network

import com.nhaarman.mockito_kotlin.mock
import net.corda.core.crypto.CompositeKey
import net.corda.core.crypto.toStringShort
import net.corda.core.identity.Party
import net.corda.core.node.MemberStatus
import net.corda.nodeapi.internal.persistence.DatabaseConfig
import net.corda.testing.core.ALICE_NAME
import net.corda.testing.core.BOB_NAME
import net.corda.testing.core.CHARLIE_NAME
import net.corda.testing.core.DUMMY_NOTARY_NAME
import net.corda.testing.core.SerializationEnvironmentRule
import net.corda.testing.core.TestIdentity
import net.corda.testing.internal.TestingNamedCacheFactory
import net.corda.testing.internal.configureDatabase
import net.corda.testing.node.MockServices
import org.assertj.core.api.Assertions.assertThat
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class PersistentMembershipGroupCacheTest {

    @Rule
    @JvmField
    val testSerialization = SerializationEnvironmentRule()

    private val alice = TestIdentity(ALICE_NAME, 80).party
    private val bob = TestIdentity(BOB_NAME, 90).party
    private val charlie = TestIdentity(CHARLIE_NAME, 100).party

    private val aliceInfo = memberInfo(alice)
    private val bobInfo = memberInfo(bob)

    private val database = configureDatabase(MockServices.makeTestDataSourceProperties(), DatabaseConfig(), { null }, { null })
    private val membershipGroupCache = PersistentMembershipGroupCache(TestingNamedCacheFactory(), database, mock())

    @Before
    internal fun setUp() {
        membershipGroupCache.addOrUpdateMembers(listOf(aliceInfo, bobInfo))
    }

    @After
    internal fun tearDown() {
        database.close()
    }

    @Test(timeout = 300_000)
    fun `insert member into database`() {
        assertEquals(aliceInfo, membershipGroupCache.getMemberByParty(alice))
        assertEquals(bobInfo, membershipGroupCache.getMemberByParty(bob))
        assertNull(membershipGroupCache.getMemberByParty(charlie))
    }

    @Test(timeout = 300_000)
    fun `update member info`() {
        val properties = mapOf("username" to "dummy")
        membershipGroupCache.addOrUpdateMember(aliceInfo.copy(status = MemberStatus.SUSPENDED, properties = properties))
        val updatedMember = membershipGroupCache.getMemberByParty(alice)
        assertEquals(MemberStatus.SUSPENDED, updatedMember?.status)
        assertEquals(properties, updatedMember?.properties)
    }

    @Test(timeout = 300_000)
    fun `remove member info`() {
        assertEquals(aliceInfo, membershipGroupCache.getMemberByParty(alice))
        membershipGroupCache.removeMember(aliceInfo)
        assertNull(membershipGroupCache.getMemberByParty(alice))
        assertEquals(bobInfo, membershipGroupCache.getMemberByParty(bob))
    }

    @Test(timeout = 300_000)
    fun `get member info by key`() {
        assertEquals(aliceInfo, membershipGroupCache.getMemberByKey(alice.owningKey))
        assertEquals(bobInfo, membershipGroupCache.getMemberByKey(bob.owningKey))
        assertNull(membershipGroupCache.getMemberByKey(charlie.owningKey))
    }

    @Test(timeout = 300_000)
    fun `get member info by key hash`() {
        assertThat(membershipGroupCache.getMembersByKeyHash(alice.owningKey.toStringShort())).containsExactlyInAnyOrder(aliceInfo)
        assertThat(membershipGroupCache.getMembersByKeyHash(bob.owningKey.toStringShort())).containsExactlyInAnyOrder(bobInfo)
        assertThat(membershipGroupCache.getMembersByKeyHash(charlie.owningKey.toStringShort())).isEmpty()
    }

    @Test(timeout = 300_000)
    fun `get distributed member info by key hash`() {
        val compositeKey = CompositeKey.Builder().addKeys(alice.owningKey, bob.owningKey).build(threshold = 1)
        val notary = Party(DUMMY_NOTARY_NAME, compositeKey)
        membershipGroupCache.addOrUpdateMember(memberInfo(notary))
        assertThat(membershipGroupCache.getMembersByKeyHash(notary.owningKey.toStringShort())).containsExactlyInAnyOrder(aliceInfo, bobInfo)
    }

    @Test(timeout = 300_000)
    fun `get distributed member info by key hash with missing component`() {
        val compositeKey = CompositeKey.Builder().addKeys(alice.owningKey, charlie.owningKey).build(threshold = 1)
        val notary = Party(DUMMY_NOTARY_NAME, compositeKey)
        membershipGroupCache.addOrUpdateMember(memberInfo(notary))
        assertThat(membershipGroupCache.getMembersByKeyHash(notary.owningKey.toStringShort())).containsExactlyInAnyOrder(aliceInfo)
    }

    @Test(timeout = 300_000)
    fun `get all members`() {
        assertThat(membershipGroupCache.allMembers).containsExactlyInAnyOrder(aliceInfo, bobInfo)
    }
}
