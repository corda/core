package net.corda.node.internal.schemas

import net.corda.core.crypto.Crypto
import net.corda.core.crypto.toStringShort
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.node.MemberInfo
import net.corda.core.node.MemberStatus
import net.corda.core.schemas.MappedSchema
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import java.security.PublicKey
import javax.persistence.CascadeType
import javax.persistence.Column
import javax.persistence.Entity
import javax.persistence.FetchType
import javax.persistence.ForeignKey
import javax.persistence.Id
import javax.persistence.JoinColumn
import javax.persistence.Lob
import javax.persistence.OneToMany
import javax.persistence.Table

object MemberInfoSchema

object MemberInfoSchemaV1 : MappedSchema(
        schemaFamily = MemberInfoSchema.javaClass,
        version = 1,
        mappedTypes = listOf(
                PersistentMemberInfo::class.java,
                PersistentKey::class.java
        )
) {
    override val migrationResource = "member-info.changelog-master"

    @Entity
    @Table(name = "member_info")
    @Suppress("LongParameterList")
    class PersistentMemberInfo(
            @Id
            @Column(name = "name", nullable = false)
            val name: String,

            @Column(name = "group_id", nullable = false)
            val groupId: String,

            @Column(name = "public_key_hash", nullable = false)
            val publicKeyHash: String,

            @Column(name = "keys", nullable = false)
            @OneToMany(cascade = [CascadeType.ALL], fetch = FetchType.EAGER, orphanRemoval = true)
            @JoinColumn(name = "name", foreignKey = ForeignKey(name = "FK__member_info_key"))
            val keys: Set<PersistentKey>,

            @Column(name = "endpoints", nullable = false)
            @Lob
            val endpoints: ByteArray,

            @Column(name = "status", nullable = false)
            val status: MemberStatus,

            @Column(name = "software_version", nullable = false)
            val softwareVersion: String,

            @Column(name = "platform_version", nullable = false)
            val platformVersion: Int,

            @Column(name = "properties", nullable = false)
            @Lob
            val properties: ByteArray
    ) {
        companion object {
            fun fromMemberInfo(memberInfo: MemberInfo) = with(memberInfo) {
                PersistentMemberInfo(
                        name = party.name.toString(),
                        groupId = groupId,
                        publicKeyHash = party.owningKey.toStringShort(),
                        keys = keys.map { PersistentKey.fromPublicKey(it) }.toSet(),
                        endpoints = endpoints.serialize().bytes,
                        status = status,
                        softwareVersion = softwareVersion,
                        platformVersion = platformVersion,
                        properties = properties.serialize().bytes
                )
            }
        }

        fun toParty(): Party {
            val publicKey = keys.single { it.publicKeyHash == publicKeyHash }.toPublicKey()
            return Party(CordaX500Name.parse(name), publicKey)
        }

        fun toMemberInfo() = MemberInfo(
                party = toParty(),
                groupId = groupId,
                keys = keys.map { it.toPublicKey() },
                endpoints = endpoints.deserialize(),
                status = status,
                softwareVersion = softwareVersion,
                platformVersion = platformVersion,
                properties = properties.deserialize()
        )
    }

    @Entity
    @Table(name = "member_info_key")
    class PersistentKey(
            @Id
            @Column(name = "public_key_hash", nullable = false)
            val publicKeyHash: String,

            @Lob
            @Column(name = "public_key", nullable = false)
            val publicKey: ByteArray
    ) {
        companion object {
            fun fromPublicKey(publicKey: PublicKey) = PersistentKey(publicKey.toStringShort(), publicKey.encoded)
        }

        fun toPublicKey() = Crypto.decodePublicKey(publicKey)
    }
}