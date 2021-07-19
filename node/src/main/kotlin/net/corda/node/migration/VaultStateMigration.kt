package net.corda.node.migration

import liquibase.database.Database
import net.corda.core.contracts.*
import net.corda.core.crypto.SecureHash
import net.corda.core.node.services.Vault
import net.corda.core.schemas.MappedSchema
import net.corda.core.schemas.PersistentStateRef
import net.corda.core.serialization.SerializationContext
import net.corda.core.serialization.internal.*
import net.corda.core.utilities.contextLogger
import net.corda.node.internal.DBNetworkParametersStorage
import net.corda.node.services.identity.PersistentIdentityService
import net.corda.node.services.keys.BasicHSMKeyManagementService
import net.corda.node.services.persistence.DBTransactionStorage
import net.corda.node.services.persistence.NodeAttachmentService
import net.corda.node.services.vault.NodeVaultService
import net.corda.node.services.vault.VaultSchemaV1
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.nodeapi.internal.persistence.DatabaseTransaction
import net.corda.nodeapi.internal.persistence.currentDBSession
import net.corda.serialization.internal.AMQP_P2P_CONTEXT
import net.corda.serialization.internal.AMQP_STORAGE_CONTEXT
import net.corda.serialization.internal.CordaSerializationMagic
import net.corda.serialization.internal.SerializationFactoryImpl
import net.corda.serialization.internal.amqp.AbstractAMQPSerializationScheme
import net.corda.serialization.internal.amqp.amqpMagic
import org.hibernate.Session
import org.hibernate.query.Query
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ForkJoinTask
import java.util.concurrent.RecursiveAction
import javax.persistence.criteria.Root
import javax.persistence.criteria.Selection

class VaultStateMigration : CordaMigration() {
    companion object {
        private val logger = contextLogger()
    }

    private fun addStateParties(session: Session, stateAndRef: StateAndRef<ContractState>) {
        val state = stateAndRef.state.data
        val persistentStateRef = PersistentStateRef(stateAndRef.ref)
        try {
            state.participants.groupBy { it.owningKey }.forEach { participants ->
                val persistentParty = VaultSchemaV1.PersistentParty(persistentStateRef, participants.value.first())
                session.persist(persistentParty)
            }
        } catch (e: AbstractMethodError) {
            // This should only happen if there was no attachment that could be used to deserialise the output states, and the state was
            // serialised such that the participants list cannot be accessed (participants is calculated and not marked as a
            // SerializableCalculatedProperty.
            throw VaultStateMigrationException("Cannot add state parties for state ${stateAndRef.ref} as state class is not on the " +
                    "classpath and participants cannot be synthesised")
        }
    }

    private fun getStateAndRef(persistentState: VaultSchemaV1.VaultStates): StateAndRef<ContractState> {
        val persistentStateRef = persistentState.stateRef ?:
                throw VaultStateMigrationException("Persistent state ref missing from state")
        val txHash = SecureHash.create(persistentStateRef.txId)
        val stateRef = StateRef(txHash, persistentStateRef.index)
        val state = try {
            servicesForResolution.loadState(stateRef)
        } catch (e: Exception) {
            throw VaultStateMigrationException("Could not load state for stateRef $stateRef : ${e.message}", e)
        }
        return StateAndRef(state, stateRef)
    }

    override fun execute(database: Database?) {
        logger.info("Migrating vault state data to V4 tables")
        if (database == null) {
            logger.error("Cannot migrate vault states: Liquibase failed to provide a suitable database connection")
            throw VaultStateMigrationException("Cannot migrate vault states as liquibase failed to provide a suitable database connection")
        }
        initialiseNodeServices(database, setOf(VaultMigrationSchemaV1, VaultSchemaV1))
        var statesSkipped = 0
        val persistentStates = VaultStateIterator(cordaDB)
        if (persistentStates.numStates > 0) {
            logger.warn("Found ${persistentStates.numStates} states to update from a previous version. This may take a while for large "
            + "volumes of data.")
        }
        VaultStateIterator.withSerializationEnv {
            persistentStates.forEach {
                val session = currentDBSession()
                try {
                    val stateAndRef = getStateAndRef(it)

                    addStateParties(session, stateAndRef)

                    // Can get away without checking for AbstractMethodErrors here as these will have already occurred when trying to add
                    // state parties.
                    val myKeys = identityService.stripNotOurKeys(stateAndRef.state.data.participants.map { participant ->
                        participant.owningKey
                    }).toSet()
                    if (!NodeVaultService.isRelevant(stateAndRef.state.data, myKeys)) {
                        it.relevancyStatus = Vault.RelevancyStatus.NOT_RELEVANT
                    }
                } catch (e: VaultStateMigrationException) {
                    logger.warn("An error occurred while migrating a vault state: ${e.message}. Skipping. This will cause the " +
                            "migration to fail.", e)
                    statesSkipped++
                }
            }
        }
        if (statesSkipped > 0) {
            logger.error("$statesSkipped states could not be migrated as there was no class available for them.")
            throw VaultStateMigrationException("Failed to migrate $statesSkipped states in the vault. Check the logs for details of the " +
                "error for each state.")
        }
        logger.info("Finished performing vault state data migration for ${persistentStates.numStates - statesSkipped} states")
    }
}


/**
 * A minimal set of schema for retrieving data from the database.
 *
 * Note that adding an extra schema here may cause migrations to fail if it ends up creating a table before the same table
 * is created in a migration script. As such, this migration must be run after the tables for the following have been created (and,
 * if they are removed in the future, before they are deleted).
 */
object VaultMigrationSchema

object VaultMigrationSchemaV1 : MappedSchema(schemaFamily = VaultMigrationSchema.javaClass, version = 1,
        mappedTypes = listOf(
                DBTransactionStorage.DBTransaction::class.java,
                PersistentIdentityService.PersistentPublicKeyHashToCertificate::class.java,
                PersistentIdentityService.PersistentPartyToPublicKeyHash::class.java,
                PersistentIdentityService.PersistentPublicKeyHashToParty::class.java,
                BasicHSMKeyManagementService.PersistentKey::class.java,
                NodeAttachmentService.DBAttachment::class.java,
                DBNetworkParametersStorage.PersistentNetworkParameters::class.java
        )
)

/**
 * Provides a mechanism for iterating through all persistent vault states.
 *
 * This class ensures that changes to persistent states are periodically committed and flushed. This prevents out of memory issues when
 * there are a large number of states.
 *
 * Currently, this class filters out those persistent states that have entries in the state party table. This behaviour is required for the
 * vault state migration, as entries in this table should not be duplicated. Unconsumed states are also filtered out for performance.
 */
class VaultStateIterator(private val database: CordaPersistence) : Iterator<VaultSchemaV1.VaultStates> {
    companion object {
        val logger = contextLogger()

        private object AMQPInspectorSerializationScheme : AbstractAMQPSerializationScheme(emptyList()) {
            override fun canDeserializeVersion(magic: CordaSerializationMagic, target: SerializationContext.UseCase): Boolean {
                return magic == amqpMagic
            }

            override fun rpcClientSerializerFactory(context: SerializationContext) = throw UnsupportedOperationException()
            override fun rpcServerSerializerFactory(context: SerializationContext) = throw UnsupportedOperationException()
        }

        private fun initialiseSerialization() {
            // Deserialise with the lenient carpenter as we only care for the AMQP field getters
            _inheritableContextSerializationEnv.set(SerializationEnvironment.with(
                    SerializationFactoryImpl().apply {
                        registerScheme(AMQPInspectorSerializationScheme)
                    },
                    p2pContext = AMQP_P2P_CONTEXT.withLenientCarpenter(),
                    storageContext = AMQP_STORAGE_CONTEXT.withLenientCarpenter()
            ))
        }

        private fun disableSerialization() {
            _inheritableContextSerializationEnv.set(null)
        }

        fun withSerializationEnv(block: () -> Unit) {
            val newEnv = if (_allEnabledSerializationEnvs.isEmpty()) {
                initialiseSerialization()
                true
            } else {
                false
            }
            effectiveSerializationEnv.serializationFactory.withCurrentContext(effectiveSerializationEnv.storageContext.withLenientCarpenter()) {
                block()
            }

            if (newEnv) {
                disableSerialization()
            }
        }
    }
    private val criteriaBuilder = database.entityManagerFactory.criteriaBuilder
    val numStates = getTotalStates()

    // Create a query on the vault states that does the following filtering:
    // - Returns only those states without corresponding entries in the state_party table
    // - Returns only unconsumed states (for performance reasons)
    private fun <T>createVaultStatesQuery(returnClass: Class<T>, selection: (Root<VaultSchemaV1.VaultStates>) -> Selection<T>): Query<T> {
        val session = currentDBSession()
        val criteriaQuery = criteriaBuilder.createQuery(returnClass)
        val queryRootStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)
        val subQuery = criteriaQuery.subquery(Long::class.java)
        val subRoot = subQuery.from(VaultSchemaV1.PersistentParty::class.java)
        subQuery.select(criteriaBuilder.count(subRoot))
        subQuery.where(criteriaBuilder.equal(
                subRoot.get<VaultSchemaV1.PersistentStateRefAndKey>(VaultSchemaV1.PersistentParty::compositeKey.name)
                        .get<PersistentStateRef>(VaultSchemaV1.PersistentStateRefAndKey::stateRef.name),
                queryRootStates.get<PersistentStateRef>(VaultSchemaV1.VaultStates::stateRef.name)))
        criteriaQuery.select(selection(queryRootStates))
        criteriaQuery.where(criteriaBuilder.and(
                criteriaBuilder.equal(subQuery, 0),
                criteriaBuilder.equal(queryRootStates.get<Vault.StateStatus>(VaultSchemaV1.VaultStates::stateStatus.name),
                        Vault.StateStatus.UNCONSUMED)))
        return session.createQuery(criteriaQuery)
    }

    private fun getTotalStates(): Long {
        return database.transaction {
            val query = createVaultStatesQuery(Long::class.java, criteriaBuilder::count)
            val result = query.singleResult
            logger.debug("Found $result total states in the vault")
            result
        }
    }

    private val pageSize = 1000
    private var pageNumber = 0
    private var transaction: DatabaseTransaction? = null
    private var currentPage = getNextPage()

    private fun endTransaction() {
        try {
            transaction?.commit()
        } catch (e: Exception) {
            transaction?.rollback()
            logger.error("Failed to commit transaction while iterating vault states: ${e.message}", e)
        } finally {
            transaction?.close()
        }
    }

    private fun getNextPage(): List<VaultSchemaV1.VaultStates> {
        endTransaction()
        transaction = database.newTransaction()
        val query = createVaultStatesQuery(VaultSchemaV1.VaultStates::class.java) { it }
        // The above query excludes states that have entries in the state party table. As the iteration proceeds, each state has entries
        // added to this table. The result is that when the next page is retrieved, any results that were in the previous page are not in
        // the query at all! As such, the next set of states that need processing start at the first result.
        query.firstResult = 0
        query.maxResults = pageSize
        pageNumber++
        val result = query.resultList
        logger.debug("Loaded page $pageNumber of ${(numStates - 1 / pageNumber.toLong()) + 1}. Current page has ${result.size} vault states")
        return result
    }

    private var currentIndex = 0

    override fun hasNext(): Boolean {
        val nextElementPresent = currentIndex + ((pageNumber - 1) * pageSize) < numStates
        if (!nextElementPresent) {
            endTransaction()
        }
        return nextElementPresent
    }

    override fun next(): VaultSchemaV1.VaultStates {
        if (currentIndex == pageSize) {
            currentPage = getNextPage()
            currentIndex = 0
        }
        val stateToReturn = currentPage[currentIndex]
        currentIndex++
        return stateToReturn
    }

    // The rest of this class is an attempt at multithreading that was ultimately scuppered by liquibase not providing a connection pool.
    // This may be useful as a starting point for improving performance of the migration, so is left here. To start using it, remove the
    // serialization environment changes in the execute function in the migration, and change forEach -> parallelForEach.
    private val pool = ForkJoinPool.commonPool()

    private class VaultPageTask(val database: CordaPersistence,
                                val page: List<VaultSchemaV1.VaultStates>,
                                val block: (VaultSchemaV1.VaultStates) -> Unit): RecursiveAction() {

        private val pageSize = page.size
        private val tolerance = 10

        override fun compute() {
            withSerializationEnv {
                if (pageSize > tolerance) {
                    ForkJoinTask.invokeAll(createSubtasks())
                } else {
                    applyBlock()
                }
            }
        }

        private fun createSubtasks(): List<VaultPageTask> {
            return listOf(VaultPageTask(database, page.subList(0, pageSize / 2), block), VaultPageTask(database, page.subList(pageSize / 2, pageSize), block))
        }

        private fun applyBlock() {
            effectiveSerializationEnv.serializationFactory.withCurrentContext(effectiveSerializationEnv.storageContext.withLenientCarpenter()) {
                database.transaction {
                    page.forEach { block(it) }
                }
            }
        }
    }

    private fun hasNextPage(): Boolean {
        val nextPagePresent = pageNumber * pageSize < numStates
        if (!nextPagePresent) {
            endTransaction()
        }
        return nextPagePresent
    }

    /**
     * Iterate through all states in the vault, parallelizing the work on each page of vault states.
     */
    fun parallelForEach(block: (VaultSchemaV1.VaultStates) -> Unit) {
        pool.invoke(VaultPageTask(database, currentPage, block))
        while (hasNextPage()) {
            currentPage = getNextPage()
            pool.invoke(VaultPageTask(database, currentPage, block))
        }
    }
}

class VaultStateMigrationException(msg: String, cause: Exception? = null) : Exception(msg, cause)