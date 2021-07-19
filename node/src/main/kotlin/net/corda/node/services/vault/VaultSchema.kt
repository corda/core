package net.corda.node.services.vault

import net.corda.core.contracts.ContractState
import net.corda.core.contracts.MAX_ISSUER_REF_SIZE
import net.corda.core.contracts.UniqueIdentifier
import net.corda.core.crypto.toStringShort
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.Party
import net.corda.core.node.services.MAX_CONSTRAINT_DATA_SIZE
import net.corda.core.node.services.Vault
import net.corda.core.schemas.*
import net.corda.core.serialization.CordaSerializable
import net.corda.core.utilities.OpaqueBytes
import org.hibernate.annotations.Immutable
import org.hibernate.annotations.Type
import java.io.Serializable
import java.time.Instant
import java.util.*
import javax.persistence.*

/**
 * JPA representation of the core Vault Schema
 */
object VaultSchema

/**
 * First version of the Vault ORM schema
 */
@Suppress("MagicNumber") // database column length
@CordaSerializable
object VaultSchemaV1 : MappedSchema(
        schemaFamily = VaultSchema.javaClass,
        version = 1,
        mappedTypes = listOf(
                VaultStates::class.java,
                VaultLinearStates::class.java,
                VaultFungibleStates::class.java,
                VaultTxnNote::class.java,
                PersistentParty::class.java,
                StateToExternalId::class.java
        )
) {

    override val migrationResource = "vault-schema.changelog-master"

    @Entity
    @Table(name = "vault_states", indexes = [Index(name = "state_status_idx", columnList = "state_status"), Index(name = "lock_id_idx", columnList = "lock_id, state_status")])
    class VaultStates(
            /** NOTE: serialized transaction state (including contract state) is now resolved from transaction store */
            // TODO: create a distinct table to hold serialized state data (once DBTransactionStore is encrypted)

            /** refers to the X500Name of the notary a state is attached to */
            @Column(name = "notary_name", nullable = false)
            var notary: Party,

            /** references a concrete ContractState that is [QueryableState] and has a [MappedSchema] */
            @Column(name = "contract_state_class_name", nullable = false)
            var contractStateClassName: String,

            /** state lifecycle: unconsumed, consumed */
            @Column(name = "state_status", nullable = false)
            var stateStatus: Vault.StateStatus,

            /** refers to timestamp recorded upon entering UNCONSUMED state */
            @Column(name = "recorded_timestamp", nullable = false)
            var recordedTime: Instant,

            /** refers to timestamp recorded upon entering CONSUMED state */
            @Column(name = "consumed_timestamp", nullable = true)
            var consumedTime: Instant? = null,

            /** used to denote a state has been soft locked (to prevent double spend)
             *  will contain a temporary unique [UUID] obtained from a flow session */
            @Column(name = "lock_id", nullable = true)
            var lockId: String? = null,

            /** Used to determine whether a state abides by the relevancy rules of the recording node */
            @Column(name = "relevancy_status", nullable = false)
            var relevancyStatus: Vault.RelevancyStatus,

            /** refers to the last time a lock was taken (reserved) or updated (released, re-reserved) */
            @Column(name = "lock_timestamp", nullable = true)
            var lockUpdateTime: Instant? = null,

            /** refers to constraint type (none, hash, whitelisted, signature) associated with a contract state */
            @Column(name = "constraint_type", nullable = false)
            var constraintType: Vault.ConstraintInfo.Type,

            /** associated constraint type data (if any) */
            @Column(name = "constraint_data", length = MAX_CONSTRAINT_DATA_SIZE, nullable = true)
            @Type(type = "corda-wrapper-binary")
            var constraintData: ByteArray? = null
    ) : PersistentState()

    @Entity
    @Table(name = "vault_linear_states", indexes = [Index(name = "external_id_index", columnList = "external_id"), Index(name = "uuid_index", columnList = "uuid")])
    class VaultLinearStates(
            /** [ContractState] attributes */

            /**
             *  Represents a [LinearState] [UniqueIdentifier]
             */
            @Column(name = "external_id", nullable = true)
            var externalId: String?,

            @Column(name = "uuid", nullable = false)
            @Type(type = "uuid-char")
            var uuid: UUID
    ) : PersistentState() {
        constructor(uid: UniqueIdentifier) : this(externalId = uid.externalId, uuid = uid.id)
    }

    @Entity
    @Table(name = "vault_fungible_states")
    class VaultFungibleStates(
            /** [OwnableState] attributes */

            /** X500Name of owner party **/
            @Column(name = "owner_name", nullable = true)
            var owner: AbstractParty?,

            /** [FungibleAsset] attributes
             *
             *  Note: the underlying Product being issued must be modelled into the
             *  custom contract itself (eg. see currency in Cash contract state)
             */

            /** Amount attributes */
            @Column(name = "quantity", nullable = false)
            var quantity: Long,

            /** Issuer attributes */

            /** X500Name of issuer party **/
            @Column(name = "issuer_name", nullable = true)
            var issuer: AbstractParty?,

            @Column(name = "issuer_ref", length = MAX_ISSUER_REF_SIZE, nullable = true)
            @Type(type = "corda-wrapper-binary")
            var issuerRef: ByteArray?
    ) : PersistentState() {
        constructor(_owner: AbstractParty, _quantity: Long, _issuerParty: AbstractParty, _issuerRef: OpaqueBytes) :
                this(owner = _owner, quantity = _quantity, issuer = _issuerParty, issuerRef = _issuerRef.bytes)
    }

    @Entity
    @Table(name = "vault_transaction_notes", indexes = [Index(name = "seq_no_index", columnList = "seq_no"), Index(name = "transaction_id_index", columnList = "transaction_id")])
    class VaultTxnNote(
            @Id
            @GeneratedValue
            @Column(name = "seq_no", nullable = false)
            var seqNo: Int,

            @Column(name = "transaction_id", length = 80, nullable = true)
            var txId: String?,

            @Column(name = "note", nullable = true)
            var note: String?
    ) {
        constructor(txId: String, note: String) : this(0, txId, note)
    }

    @Embeddable
    @Immutable
    data class PersistentStateRefAndKey(/* Foreign key. */ @Embedded override var stateRef: PersistentStateRef?, @Column(name = "public_key_hash", nullable = false) var publicKeyHash: String?) : DirectStatePersistable, Serializable {
        constructor() : this(null, null)
    }

    @Entity
    @Table(name = "state_party", indexes = [Index(name = "state_party_idx", columnList = "public_key_hash")])
    class PersistentParty(
            @EmbeddedId
            override val compositeKey: PersistentStateRefAndKey,

            @Column(name = "x500_name", nullable = true)
            var x500Name: AbstractParty? = null
    ) : IndirectStatePersistable<PersistentStateRefAndKey> {
        constructor(stateRef: PersistentStateRef, abstractParty: AbstractParty)
                : this(PersistentStateRefAndKey(stateRef, abstractParty.owningKey.toStringShort()), abstractParty)
    }

    @Entity
    @Immutable
    @Table(name = "v_pkey_hash_ex_id_map")
    class StateToExternalId(
            @EmbeddedId
            override val compositeKey: PersistentStateRefAndKey,

            @Column(name = "external_id")
            @Type(type = "uuid-char")
            val externalId: UUID
    ) : IndirectStatePersistable<PersistentStateRefAndKey>
}

