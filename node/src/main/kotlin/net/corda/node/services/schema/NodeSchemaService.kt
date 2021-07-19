package net.corda.node.services.schema

import net.corda.core.contracts.ContractState
import net.corda.core.contracts.FungibleAsset
import net.corda.core.contracts.FungibleState
import net.corda.core.contracts.LinearState
import net.corda.core.schemas.*
import net.corda.core.schemas.MappedSchemaValidator.crossReferencesToOtherMappedSchema
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.node.internal.DBNetworkParametersStorage
import net.corda.node.internal.schemas.MemberInfoSchemaV1
import net.corda.node.internal.schemas.NodeInfoSchemaV1
import net.corda.node.services.api.SchemaService
import net.corda.node.services.events.NodeSchedulerService
import net.corda.node.services.identity.PersistentIdentityService
import net.corda.node.services.keys.BasicHSMKeyManagementService
import net.corda.node.services.messaging.P2PMessageDeduplicator
import net.corda.node.services.persistence.DBCheckpointStorage
import net.corda.node.services.persistence.DBTransactionStorage
import net.corda.node.services.persistence.NodeAttachmentService
import net.corda.node.services.persistence.PublicKeyHashToExternalId
import net.corda.node.services.upgrade.ContractUpgradeServiceImpl
import net.corda.node.services.vault.VaultSchemaV1

/**
 * Most basic implementation of [SchemaService].
 * TODO: support loading schema options from node configuration.
 * TODO: support configuring what schemas are to be selected for persistence.
 * TODO: support plugins for schema version upgrading or custom mapping not supported by original [QueryableState].
 * TODO: create whitelisted tables when a CorDapp is first installed
 */
class NodeSchemaService(private val extraSchemas: Set<MappedSchema> = emptySet()) : SchemaService, SingletonSerializeAsToken() {
    // Core Entities used by a Node
    object NodeCore

    object NodeCoreV1 : MappedSchema(schemaFamily = NodeCore.javaClass, version = 1,
            mappedTypes = listOf(DBCheckpointStorage.DBFlowCheckpoint::class.java,
                    DBCheckpointStorage.DBFlowCheckpointBlob::class.java,
                    DBCheckpointStorage.DBFlowResult::class.java,
                    DBCheckpointStorage.DBFlowException::class.java,
                    DBCheckpointStorage.DBFlowMetadata::class.java,

                    DBTransactionStorage.DBTransaction::class.java,
                    BasicHSMKeyManagementService.PersistentKey::class.java,
                    NodeSchedulerService.PersistentScheduledState::class.java,
                    NodeAttachmentService.DBAttachment::class.java,
                    P2PMessageDeduplicator.ProcessedMessage::class.java,
                    PersistentIdentityService.PersistentPublicKeyHashToCertificate::class.java,
                    PersistentIdentityService.PersistentPublicKeyHashToParty::class.java,
                    PersistentIdentityService.PersistentHashToPublicKey::class.java,
                    ContractUpgradeServiceImpl.DBContractUpgrade::class.java,
                    DBNetworkParametersStorage.PersistentNetworkParameters::class.java,
                    PublicKeyHashToExternalId::class.java
            )) {
        override val migrationResource = "node-core.changelog-master"
    }

    // Required schemas are those used by internal Corda services
    private val requiredSchemas: Set<MappedSchema> =
            setOf(CommonSchemaV1,
                    VaultSchemaV1,
                    NodeInfoSchemaV1,
                    MemberInfoSchemaV1,
                    NodeCoreV1)

    val internalSchemas = requiredSchemas + extraSchemas.filter { schema ->
                schema::class.qualifiedName?.startsWith("net.corda.notary.") ?: false
    }

    val appSchemas = extraSchemas - internalSchemas

    override val schemas: Set<MappedSchema> = requiredSchemas + extraSchemas

    // Currently returns all schemas supported by the state, with no filtering or enrichment.
    override fun selectSchemas(state: ContractState): Iterable<MappedSchema> {
        val schemas = mutableSetOf<MappedSchema>()
        if (state is QueryableState)
            schemas += state.supportedSchemas()
        if (state is LinearState)
            schemas += VaultSchemaV1   // VaultLinearStates
        if (state is FungibleAsset<*>)
            schemas += VaultSchemaV1   // VaultFungibleAssets
        if (state is FungibleState<*>)
            schemas += VaultSchemaV1   // VaultFungibleStates

        return schemas
    }

    // Because schema is always one supported by the state, just delegate.
    override fun generateMappedObject(state: ContractState, schema: MappedSchema): PersistentState {
        if ((schema === VaultSchemaV1) && (state is LinearState))
            return VaultSchemaV1.VaultLinearStates(state.linearId)
        if ((schema === VaultSchemaV1) && (state is FungibleAsset<*>))
            return VaultSchemaV1.VaultFungibleStates(state.owner, state.amount.quantity, state.amount.token.issuer.party, state.amount.token.issuer.reference)
        if ((schema === VaultSchemaV1) && (state is FungibleState<*>))
            return VaultSchemaV1.VaultFungibleStates(owner = null, quantity = state.amount.quantity, issuer = null, issuerRef = null)
        return (state as QueryableState).generateMappedObject(schema)
    }

    /** Returns list of [MappedSchemaValidator.SchemaCrossReferenceReport] violations. */
    fun mappedSchemasWarnings(): List<MappedSchemaValidator.SchemaCrossReferenceReport> =
            schemas.map { schema -> crossReferencesToOtherMappedSchema(schema) }.flatMap { it.toList() }

}


