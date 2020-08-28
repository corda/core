package net.corda.node.migration

import net.corda.core.contracts.*
import net.corda.core.cordapp.CordappContext
import net.corda.core.cordapp.CordappProvider
import net.corda.core.crypto.SecureHash
import net.corda.core.internal.deserialiseComponentGroup
import net.corda.core.internal.div
import net.corda.core.internal.readObject
import net.corda.core.node.NetworkParameters
import net.corda.core.node.ServicesForResolution
import net.corda.core.node.services.AttachmentId
import net.corda.core.node.services.IdentityService
import net.corda.core.node.services.NetworkParametersService
import net.corda.core.node.services.TransactionStorage
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.internal.AttachmentsClassLoaderBuilder
import net.corda.core.transactions.ContractUpgradeLedgerTransaction
import net.corda.core.transactions.NotaryChangeLedgerTransaction
import net.corda.core.transactions.WireTransaction
import net.corda.core.utilities.contextLogger
import net.corda.node.internal.DBNetworkParametersStorage
import net.corda.node.services.attachments.NodeAttachmentTrustCalculator
import net.corda.node.services.persistence.AttachmentStorageInternal
import net.corda.nodeapi.internal.network.NETWORK_PARAMS_FILE_NAME
import net.corda.nodeapi.internal.network.SignedNetworkParameters
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.nodeapi.internal.persistence.SchemaMigration
import java.nio.file.Paths
import java.time.Clock
import java.time.Duration
import java.util.Comparator.comparingInt

class MigrationServicesForResolution(
        override val identityService: IdentityService,
        override val attachments: AttachmentStorageInternal,
        private val transactions: TransactionStorage,
        private val cordaDB: CordaPersistence,
        cacheFactory: MigrationNamedCacheFactory
): ServicesForResolution {

    companion object {
        val logger = contextLogger()
    }
    override val cordappProvider: CordappProvider
        get() = object : CordappProvider {

            val cordappLoader = SchemaMigration.loader.get()

            override fun getAppContext(): CordappContext {
                TODO("not implemented")
            }

            override fun getContractAttachmentID(contractClassName: ContractClassName): AttachmentId? {
                TODO("not implemented")
            }
        }
    private val cordappLoader = SchemaMigration.loader.get()

    private val attachmentTrustCalculator = NodeAttachmentTrustCalculator(
        attachments,
        cacheFactory
    )

    private fun defaultNetworkParameters(): NetworkParameters {
        logger.warn("Using a dummy set of network parameters for migration.")
        val clock = Clock.systemUTC()
        return NetworkParameters(
                1,
                listOf(),
                1,
                1,
                clock.instant(),
                1,
                mapOf(),
                Duration.ZERO,
                mapOf()
        )
    }

    private fun getNetworkParametersFromFile(): SignedNetworkParameters? {
        return try {
            val dir = System.getProperty(SchemaMigration.NODE_BASE_DIR_KEY)
            val path = Paths.get(dir) / NETWORK_PARAMS_FILE_NAME
            path.readObject()
        } catch (e: Exception) {
            logger.info("Couldn't find network parameters file: ${e.message}. This is expected if the node is starting for the first time.")
            null
        }
    }

    override val networkParametersService: NetworkParametersService = object : NetworkParametersService {

        private val storage = DBNetworkParametersStorage.createParametersMap(cacheFactory)

        private val filedParams = getNetworkParametersFromFile()

        override val defaultHash: SecureHash = filedParams?.raw?.hash ?: SecureHash.getZeroHash()
        override val currentHash: SecureHash = cordaDB.transaction {
            storage.allPersisted.use {
                it.max(comparingInt { it.second.verified().epoch }).map { it.first }.orElse(defaultHash)
            }
        }

        override fun lookup(hash: SecureHash): NetworkParameters? {
            // Note that the parameters in any file shouldn't be put into the database - this will be done by the node on startup.
            return if (hash == filedParams?.raw?.hash) {
                filedParams.raw.deserialize()
            } else {
                cordaDB.transaction { storage[hash]?.verified() }
            }
        }
    }

    override val networkParameters: NetworkParameters = networkParametersService.lookup(networkParametersService.currentHash)
            ?: getNetworkParametersFromFile()?.raw?.deserialize()
            ?: defaultNetworkParameters()

    private fun extractStateFromTx(tx: WireTransaction, stateIndices: Collection<Int>): List<TransactionState<ContractState>> {
        return try {
            val txAttachments = tx.attachments.mapNotNull { attachments.openAttachment(it)}
            val states = AttachmentsClassLoaderBuilder.withAttachmentsClassloaderContext(
                    txAttachments,
                    networkParameters,
                    tx.id,
                    attachmentTrustCalculator::calculate,
                    parent = cordappLoader.appClassLoader) {
                deserialiseComponentGroup(tx.componentGroups, TransactionState::class, ComponentGroupEnum.OUTPUTS_GROUP, forceDeserialize = true)
            }
            states.filterIndexed {index, _ -> stateIndices.contains(index)}.toList()
        } catch (e: Exception) {
            // If there is no attachment that allows the state class to be deserialised correctly, then carpent a state class anyway. It
            // might still be possible to access the participants depending on how the state class was serialised.
            logger.debug("Could not use attachments to deserialise transaction output states for transaction ${tx.id}")
            tx.outputs.filterIndexed { index, _ -> stateIndices.contains(index)}
        }
    }

    override fun loadState(stateRef: StateRef): TransactionState<*> {
        val stx = transactions.getTransaction(stateRef.txhash)
                ?: throw MigrationException("Could not get transaction with hash ${stateRef.txhash} out of vault")
        val baseTx = stx.resolveBaseTransaction(this)
        return when (baseTx) {
            is NotaryChangeLedgerTransaction -> baseTx.outputs[stateRef.index]
            is ContractUpgradeLedgerTransaction -> baseTx.outputs[stateRef.index]
            is WireTransaction -> extractStateFromTx(baseTx, listOf(stateRef.index)).first()
            else -> throw MigrationException("Unknown transaction type ${baseTx::class.qualifiedName} found when loading a state")
        }
    }

    override fun loadStates(stateRefs: Set<StateRef>): Set<StateAndRef<ContractState>> {
        return stateRefs.groupBy { it.txhash }.flatMap {
            val stx = transactions.getTransaction(it.key)
                    ?: throw MigrationException("Could not get transaction with hash ${it.key} out of vault")
            val baseTx = stx.resolveBaseTransaction(this)
            val stateList = when (baseTx) {
                is NotaryChangeLedgerTransaction -> it.value.map { stateRef -> StateAndRef(baseTx.outputs[stateRef.index], stateRef) }
                is ContractUpgradeLedgerTransaction -> it.value.map { stateRef -> StateAndRef(baseTx.outputs[stateRef.index], stateRef) }
                is WireTransaction -> extractStateFromTx(baseTx, it.value.map { stateRef -> stateRef.index })
                        .mapIndexed {index, state -> StateAndRef(state, StateRef(baseTx.id, index)) }
                else -> throw MigrationException("Unknown transaction type ${baseTx::class.qualifiedName} found when loading a state")
            }
            stateList
        }.toSet()
    }

    override fun loadContractAttachment(stateRef: StateRef): Attachment {
        throw NotImplementedError()
    }
}