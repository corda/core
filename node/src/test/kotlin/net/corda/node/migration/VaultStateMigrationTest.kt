package net.corda.node.migration

import liquibase.database.Database
import liquibase.database.jvm.JdbcConnection
import net.corda.core.contracts.*
import net.corda.core.crypto.*
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.internal.NotaryChangeTransactionBuilder
import net.corda.core.internal.packageName
import net.corda.core.internal.signWithCert
import net.corda.core.node.NetworkParameters
import net.corda.core.node.NotaryInfo
import net.corda.core.node.services.Vault
import net.corda.core.schemas.PersistentStateRef
import net.corda.core.serialization.SerializationDefaults
import net.corda.core.serialization.serialize
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.contextLogger
import net.corda.finance.DOLLARS
import net.corda.finance.contracts.Commodity
import net.corda.finance.contracts.asset.Cash
import net.corda.finance.contracts.asset.Obligation
import net.corda.finance.contracts.asset.OnLedgerAsset
import net.corda.finance.schemas.CashSchemaV1
import net.corda.node.internal.DBNetworkParametersStorage
import net.corda.node.internal.schemas.MemberInfoSchemaV1
import net.corda.node.services.identity.PersistentIdentityService
import net.corda.node.services.keys.BasicHSMKeyManagementService
import net.corda.node.services.network.memberInfo
import net.corda.node.services.persistence.DBTransactionStorage
import net.corda.node.services.vault.VaultSchemaV1
import net.corda.nodeapi.internal.crypto.X509Utilities
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.nodeapi.internal.persistence.DatabaseConfig
import net.corda.nodeapi.internal.persistence.contextTransactionOrNull
import net.corda.nodeapi.internal.persistence.currentDBSession
import net.corda.testing.core.*
import net.corda.testing.internal.configureDatabase
import net.corda.testing.internal.vault.CommodityState
import net.corda.testing.internal.vault.DUMMY_LINEAR_CONTRACT_PROGRAM_ID
import net.corda.testing.internal.vault.DummyLinearContract
import net.corda.testing.internal.vault.DummyLinearStateSchemaV1
import net.corda.testing.node.MockServices
import net.corda.testing.node.MockServices.Companion.makeTestDataSourceProperties
import net.corda.testing.node.TestClock
import net.corda.testing.node.makeTestIdentityService
import org.junit.*
import org.mockito.Mockito
import java.security.KeyPair
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

/**
 * These tests aim to verify that migrating vault states from V3 to later versions works correctly. While these unit tests verify the
 * migrating behaviour is correct (tables populated, columns updated for the right states), it comes with a caveat: they do not test that
 * deserialising states with the attachment classloader works correctly.
 *
 * The reason for this is that it is impossible to do so. There is no real way of writing a unit or integration test to upgrade from one
 * version to another (at the time of writing). These tests simulate a small part of the upgrade process by directly using hibernate to
 * populate a database as a V3 node would, then running the migration class. However, it is impossible to do this for attachments as there
 * is no contract state jar to serialise.
 */
class VaultStateMigrationTest {
    companion object {
        val alice = TestIdentity(ALICE_NAME, 70)
        val bankOfCorda = TestIdentity(BOC_NAME)
        val bob = TestIdentity(BOB_NAME, 80)
        private val charlie = TestIdentity(CHARLIE_NAME, 90)
        val dummyCashIssuer = TestIdentity(CordaX500Name("Snake Oil Issuer", "London", "GB"), 10)
        val dummyNotary = TestIdentity(DUMMY_NOTARY_NAME, 20)
        val ALICE get() = alice.party
        val ALICE_IDENTITY get() = alice.identity
        val BOB get() = bob.party
        val BOB_IDENTITY get() = bob.identity
        val BOC_IDENTITY get() = bankOfCorda.identity
        val BOC_KEY get() = bankOfCorda.keyPair
        val CHARLIE get() = charlie.party
        val DUMMY_NOTARY get() = dummyNotary.party
        val bob2 = TestIdentity(BOB_NAME, 40)
        val BOB2 = bob2.party
        val BOB2_IDENTITY = bob2.identity

        val clock: TestClock = TestClock(Clock.systemUTC())

        @ClassRule
        @JvmField
        val testSerialization = SerializationEnvironmentRule()

        val logger = contextLogger()
    }

    val cordappPackages = listOf(
            "net.corda.finance.contracts",
            CashSchemaV1::class.packageName,
            DummyLinearStateSchemaV1::class.packageName)

    lateinit var liquibaseDB: Database
    lateinit var cordaDB: CordaPersistence
    lateinit var notaryServices: MockServices

    @Before
    fun setUp() {
        val identityService = makeTestIdentityService(dummyNotary.identity, BOB_IDENTITY, ALICE_IDENTITY)
        notaryServices = MockServices(cordappPackages, dummyNotary, identityService, dummyCashIssuer.keyPair, BOC_KEY)
        cordaDB = configureDatabase(
                makeTestDataSourceProperties(),
                DatabaseConfig(),
                notaryServices.identityService::wellKnownPartyFromX500Name,
                notaryServices.identityService::wellKnownPartyFromAnonymous,
                ourName = BOB_IDENTITY.name)
        val liquibaseConnection = Mockito.mock(JdbcConnection::class.java)
        Mockito.`when`(liquibaseConnection.url).thenReturn(cordaDB.jdbcUrl)
        Mockito.`when`(liquibaseConnection.wrappedConnection).thenReturn(cordaDB.dataSource.connection)
        liquibaseDB = Mockito.mock(Database::class.java)
        Mockito.`when`(liquibaseDB.connection).thenReturn(liquibaseConnection)

        saveOurKeys(listOf(bob.keyPair, bob2.keyPair))
        saveAllIdentities(listOf(BOB_IDENTITY, ALICE_IDENTITY, BOC_IDENTITY, dummyNotary.identity, BOB2_IDENTITY))
        addNetworkParameters()
    }

    @After
    fun close() {
        contextTransactionOrNull?.close()
        cordaDB.close()
    }

    private fun addNetworkParameters() {
        cordaDB.transaction {
            val clock = Clock.systemUTC()
            val params = NetworkParameters(
                    1,
                    listOf(NotaryInfo(DUMMY_NOTARY, false), NotaryInfo(CHARLIE, false)),
                    1,
                    1,
                    clock.instant(),
                    1,
                    mapOf(),
                    Duration.ZERO,
                    mapOf()
            )
            val signedParams = params.signWithCert(bob.keyPair.private, BOB_IDENTITY.certificate)
            val persistentParams = DBNetworkParametersStorage.PersistentNetworkParameters(
                    SecureHash.allOnesHash.toString(),
                    params.epoch,
                    signedParams.raw.bytes,
                    signedParams.sig.bytes,
                    signedParams.sig.by.encoded,
                    X509Utilities.buildCertPath(signedParams.sig.parentCertsChain).encoded
            )
            session.save(persistentParams)
        }
    }

    private fun createCashTransaction(cash: Cash, value: Amount<Currency>, owner: AbstractParty): SignedTransaction {
        val tx = TransactionBuilder(DUMMY_NOTARY)
        cash.generateIssue(tx, Amount(value.quantity, Issued(bankOfCorda.ref(1), value.token)), owner, DUMMY_NOTARY)
        return notaryServices.signInitialTransaction(tx, bankOfCorda.party.owningKey)
    }

    private fun createVaultStatesFromTransaction(tx: SignedTransaction, stateStatus: Vault.StateStatus = Vault.StateStatus.UNCONSUMED) {
        cordaDB.transaction {
            tx.coreTransaction.outputs.forEachIndexed { index, state ->
                val constraintInfo = Vault.ConstraintInfo(state.constraint)
                val persistentState = VaultSchemaV1.VaultStates(
                        notary = state.notary,
                        contractStateClassName = state.data.javaClass.name,
                        stateStatus = stateStatus,
                        recordedTime = clock.instant(),
                        relevancyStatus = Vault.RelevancyStatus.RELEVANT, //Always persist as relevant to mimic V3
                        constraintType = constraintInfo.type(),
                        constraintData = constraintInfo.data()
                )
                persistentState.stateRef = PersistentStateRef(tx.id.toString(), index)
                session.save(persistentState)
            }
        }
    }

    private fun saveOurKeys(keys: List<KeyPair>) {
        cordaDB.transaction {
            keys.forEach {
                val persistentKey = BasicHSMKeyManagementService.PersistentKey(it.public, it.private)
                session.save(persistentKey)
            }
        }
    }

    private fun saveAllIdentities(identities: List<PartyAndCertificate>) {
        cordaDB.transaction {
            identities.groupBy { it.name }.forEach { (_, certs) ->
                val persistentIDs = certs.map { PersistentIdentityService.PersistentPublicKeyHashToCertificate(it.owningKey.toStringShort(), it.certPath.encoded) }
                persistentIDs.forEach { session.save(it) }
                val persistentMemberInfo = MemberInfoSchemaV1.PersistentMemberInfo.fromMemberInfo(memberInfo(certs.first().party))
                session.save(persistentMemberInfo)
            }
        }
    }

    private fun storeTransaction(tx: SignedTransaction) {
        cordaDB.transaction {
            val persistentTx = DBTransactionStorage.DBTransaction(
                    txId = tx.id.toString(),
                    stateMachineRunId = null,
                    transaction = tx.serialize(context = SerializationDefaults.STORAGE_CONTEXT).bytes,
                    status = DBTransactionStorage.TransactionStatus.VERIFIED,
                    timestamp = Instant.now()
            )
            session.save(persistentTx)
        }
    }

    private fun getVaultStateCount(relevancyStatus: Vault.RelevancyStatus = Vault.RelevancyStatus.ALL): Long {
        return cordaDB.transaction {
            val criteriaBuilder = cordaDB.entityManagerFactory.criteriaBuilder
            val criteriaQuery = criteriaBuilder.createQuery(Long::class.java)
            val queryRootStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)
            criteriaQuery.select(criteriaBuilder.count(queryRootStates))
            if (relevancyStatus != Vault.RelevancyStatus.ALL) {
                criteriaQuery.where(criteriaBuilder.equal(queryRootStates.get<Vault.RelevancyStatus>("relevancyStatus"), relevancyStatus))
            }
            val query = session.createQuery(criteriaQuery)
            query.singleResult
        }
    }

    private fun getStatePartyCount(): Long {
        return cordaDB.transaction {
            val criteriaBuilder = cordaDB.entityManagerFactory.criteriaBuilder
            val criteriaQuery = criteriaBuilder.createQuery(Long::class.java)
            val queryRootStates = criteriaQuery.from(VaultSchemaV1.PersistentParty::class.java)
            criteriaQuery.select(criteriaBuilder.count(queryRootStates))
            val query = session.createQuery(criteriaQuery)
            query.singleResult
        }
    }

    private fun addCashStates(statesToAdd: Int, owner: AbstractParty, stateStatus: Vault.StateStatus = Vault.StateStatus.UNCONSUMED) {
        val cash = Cash()
        cordaDB.transaction {
            (1..statesToAdd).map { createCashTransaction(cash, it.DOLLARS, owner) }.forEach {
                storeTransaction(it)
                createVaultStatesFromTransaction(it, stateStatus)
            }
        }
    }

    private fun createLinearStateTransaction(idString: String,
                                             parties: List<AbstractParty> = listOf(),
                                             linearString: String = "foo",
                                             linearNumber: Long = 0L,
                                             linearBoolean: Boolean = false): SignedTransaction {
        val tx = TransactionBuilder(notary = dummyNotary.party).apply {
            addOutputState(DummyLinearContract.State(
                    linearId = UniqueIdentifier(idString),
                    participants = parties,
                    linearString = linearString,
                    linearNumber = linearNumber,
                    linearBoolean = linearBoolean,
                    linearTimestamp = clock.instant()), DUMMY_LINEAR_CONTRACT_PROGRAM_ID
            )
            addCommand(dummyCommand())
        }
        return notaryServices.signInitialTransaction(tx)
    }

    private fun addLinearStates(statesToAdd: Int, parties: List<AbstractParty>) {
        cordaDB.transaction {
            (1..statesToAdd).map { createLinearStateTransaction("A".repeat(it), parties) }.forEach {
                storeTransaction(it)
                createVaultStatesFromTransaction(it)
            }
        }
    }

    private fun createCommodityTransaction(amount: Amount<Issued<Commodity>>, owner: AbstractParty): SignedTransaction {
        val txBuilder = TransactionBuilder(notary = dummyNotary.party)
        OnLedgerAsset.generateIssue(txBuilder, TransactionState(CommodityState(amount, owner), Obligation.PROGRAM_ID, dummyNotary.party), Obligation.Commands.Issue())
        return notaryServices.signInitialTransaction(txBuilder)
    }

    private fun addCommodityStates(statesToAdd: Int, owner: AbstractParty) {
        cordaDB.transaction {
            (1..statesToAdd).map {
                createCommodityTransaction(Amount(it.toLong(), Issued(bankOfCorda.ref(2), Commodity.getInstance("FCOJ")!!)), owner)
            }.forEach {
                storeTransaction(it)
                createVaultStatesFromTransaction(it)
            }
        }
    }

    private fun createNotaryChangeTransaction(inputs: List<StateRef>, paramsHash: SecureHash): SignedTransaction {
        val notaryTx = NotaryChangeTransactionBuilder(inputs, DUMMY_NOTARY, CHARLIE, paramsHash).build()
        val notaryKey = DUMMY_NOTARY.owningKey
        val signableData = SignableData(notaryTx.id, SignatureMetadata(3, Crypto.findSignatureScheme(notaryKey).schemeNumberID))
        val notarySignature = notaryServices.keyManagementService.sign(signableData, notaryKey)
        return SignedTransaction(notaryTx, listOf(notarySignature))
    }

    private fun createVaultStatesFromNotaryChangeTransaction(tx: SignedTransaction, inputs: List<TransactionState<ContractState>>) {
        cordaDB.transaction {
            inputs.forEachIndexed { index, state ->
                val constraintInfo = Vault.ConstraintInfo(state.constraint)
                val persistentState = VaultSchemaV1.VaultStates(
                        notary = tx.notary!!,
                        contractStateClassName = state.data.javaClass.name,
                        stateStatus = Vault.StateStatus.UNCONSUMED,
                        recordedTime = clock.instant(),
                        relevancyStatus = Vault.RelevancyStatus.RELEVANT, //Always persist as relevant to mimic V3
                        constraintType = constraintInfo.type(),
                        constraintData = constraintInfo.data()
                )
                persistentState.stateRef = PersistentStateRef(tx.id.toString(), index)
                session.save(persistentState)
            }
        }
    }

    private fun <T> getState(clazz: Class<T>): T {
        return cordaDB.transaction {
            val criteriaBuilder = cordaDB.entityManagerFactory.criteriaBuilder
            val criteriaQuery = criteriaBuilder.createQuery(clazz)
            val queryRootStates = criteriaQuery.from(clazz)
            criteriaQuery.select(queryRootStates)
            val query = session.createQuery(criteriaQuery)
            query.singleResult
        }
    }

    private fun checkStatesEqual(expected: VaultSchemaV1.VaultStates, actual: VaultSchemaV1.VaultStates) {
        assertEquals(expected.notary, actual.notary)
        assertEquals(expected.stateStatus, actual.stateStatus)
        assertEquals(expected.relevancyStatus, actual.relevancyStatus)
    }

    private fun addToStatePartyTable(stateAndRef: StateAndRef<ContractState>) {
        cordaDB.transaction {
            val persistentStateRef = PersistentStateRef(stateAndRef.ref.txhash.toString(), stateAndRef.ref.index)
            val session = currentDBSession()
            stateAndRef.state.data.participants.forEach {
                val persistentParty = VaultSchemaV1.PersistentParty(
                        persistentStateRef,
                        it
                )
                session.save(persistentParty)
            }
        }
    }

    @Test(timeout=300_000)
	fun `Check a simple migration works`() {
        addCashStates(10, BOB)
        addCashStates(10, ALICE)
        assertEquals(20, getVaultStateCount())
        assertEquals(0, getStatePartyCount())
        val migration = VaultStateMigration()
        migration.execute(liquibaseDB)
        assertEquals(20, getVaultStateCount())
        assertEquals(20, getStatePartyCount())
        assertEquals(10, getVaultStateCount(Vault.RelevancyStatus.RELEVANT))
    }

    @Test(timeout=300_000)
	fun `Check state paging works`() {
        addCashStates(1010, BOB)

        assertEquals(0, getStatePartyCount())
        val migration = VaultStateMigration()
        migration.execute(liquibaseDB)
        assertEquals(1010, getStatePartyCount())
        assertEquals(1010, getVaultStateCount())
        assertEquals(0, getVaultStateCount(Vault.RelevancyStatus.NOT_RELEVANT))
    }

    @Test(timeout=300_000)
	fun `Check state fields are correct`() {
        val tx = createCashTransaction(Cash(), 100.DOLLARS, ALICE)
        storeTransaction(tx)
        createVaultStatesFromTransaction(tx)
        val expectedPersistentParty = VaultSchemaV1.PersistentParty(
                PersistentStateRef(tx.id.toString(), 0),
                ALICE
        )
        val state = tx.coreTransaction.outputs.first()
        val constraintInfo = Vault.ConstraintInfo(state.constraint)
        val expectedPersistentState = VaultSchemaV1.VaultStates(
                notary = state.notary,
                contractStateClassName = state.data.javaClass.name,
                stateStatus = Vault.StateStatus.UNCONSUMED,
                recordedTime = clock.instant(),
                relevancyStatus = Vault.RelevancyStatus.NOT_RELEVANT,
                constraintType = constraintInfo.type(),
                constraintData = constraintInfo.data()
        )

        val migration = VaultStateMigration()
        migration.execute(liquibaseDB)
        val persistentStateParty = getState(VaultSchemaV1.PersistentParty::class.java)
        val persistentState = getState(VaultSchemaV1.VaultStates::class.java)
        checkStatesEqual(expectedPersistentState, persistentState)
        assertEquals(expectedPersistentParty.x500Name, persistentStateParty.x500Name)
        assertEquals(expectedPersistentParty.compositeKey, persistentStateParty.compositeKey)
    }

    @Test(timeout=300_000)
	fun `Check the connection is open post migration`() {
        // Liquibase automatically closes the database connection when doing an actual migration. This test ensures the custom migration
        // leaves it open.
        addCashStates(12, ALICE)

        val migration = VaultStateMigration()
        migration.execute(liquibaseDB)
        assertFalse(cordaDB.dataSource.connection.isClosed)
    }

    @Test(timeout=300_000)
	fun `All parties added to state party table`() {
        val stx = createLinearStateTransaction("test", parties = listOf(ALICE, BOB, CHARLIE))
        storeTransaction(stx)
        createVaultStatesFromTransaction(stx)

        val migration = VaultStateMigration()
        migration.execute(liquibaseDB)
        assertEquals(3, getStatePartyCount())
        assertEquals(1, getVaultStateCount())
        assertEquals(0, getVaultStateCount(Vault.RelevancyStatus.NOT_RELEVANT))
    }

    @Test(timeout=300_000)
	fun `State with corresponding transaction missing fails migration`() {
        val cash = Cash()
        val unknownTx = createCashTransaction(cash, 100.DOLLARS, BOB)
        createVaultStatesFromTransaction(unknownTx)

        addCashStates(10, BOB)
        val migration = VaultStateMigration()
        assertFailsWith<VaultStateMigrationException> { migration.execute(liquibaseDB) }
        assertEquals(10, getStatePartyCount())

        // Now add the missing transaction and ensure that the migration succeeds
        storeTransaction(unknownTx)
        migration.execute(liquibaseDB)
        assertEquals(11, getStatePartyCount())
    }

    @Test(timeout=300_000)
	fun `State with unknown ID is handled correctly`() {
        addCashStates(1, CHARLIE)
        addCashStates(10, BOB)
        val migration = VaultStateMigration()
        migration.execute(liquibaseDB)
        assertEquals(11, getStatePartyCount())
        assertEquals(1, getVaultStateCount(Vault.RelevancyStatus.NOT_RELEVANT))
        assertEquals(10, getVaultStateCount(Vault.RelevancyStatus.RELEVANT))
    }

    @Test(expected = VaultStateMigrationException::class)
    fun `Null database causes migration to fail`() {
        val migration = VaultStateMigration()
        // Just check this does not throw an exception
        migration.execute(null)
    }

    @Test(timeout=300_000)
	fun `State with non-owning key for our name marked as relevant`() {
        val tx = createCashTransaction(Cash(), 100.DOLLARS, BOB2)
        storeTransaction(tx)
        createVaultStatesFromTransaction(tx)
        val state = tx.coreTransaction.outputs.first()
        val constraintInfo = Vault.ConstraintInfo(state.constraint)
        val expectedPersistentState = VaultSchemaV1.VaultStates(
                notary = state.notary,
                contractStateClassName = state.data.javaClass.name,
                stateStatus = Vault.StateStatus.UNCONSUMED,
                recordedTime = clock.instant(),
                relevancyStatus = Vault.RelevancyStatus.RELEVANT,
                constraintType = constraintInfo.type(),
                constraintData = constraintInfo.data()
        )
        val migration = VaultStateMigration()
        migration.execute(liquibaseDB)
        val persistentState = getState(VaultSchemaV1.VaultStates::class.java)
        checkStatesEqual(expectedPersistentState, persistentState)
    }

    @Test(timeout=300_000)
	fun `State already in state party table is excluded`() {
        val tx = createCashTransaction(Cash(), 100.DOLLARS, BOB)
        storeTransaction(tx)
        createVaultStatesFromTransaction(tx)
        addToStatePartyTable(tx.coreTransaction.outRef(0))
        addCashStates(5, BOB)
        assertEquals(1, getStatePartyCount())
        val migration = VaultStateMigration()
        migration.execute(liquibaseDB)
        assertEquals(6, getStatePartyCount())
    }

    @Test(timeout=300_000)
	fun `Consumed states are not migrated`() {
        addCashStates(1010, BOB, Vault.StateStatus.CONSUMED)
        assertEquals(0, getStatePartyCount())
        val migration = VaultStateMigration()
        migration.execute(liquibaseDB)
        assertEquals(0, getStatePartyCount())
    }

    @Test(timeout=300_000)
	fun `State created with notary change transaction can be migrated`() {
        // This test is a little bit of a hack - it checks that these states are migrated correctly by looking at params in the database,
        // but these will not be there for V3 nodes. Handling for this must be tested manually.
        val cashTx = createCashTransaction(Cash(), 5.DOLLARS, BOB)
        val cashTx2 = createCashTransaction(Cash(), 10.DOLLARS, BOB)
        val notaryTx = createNotaryChangeTransaction(listOf(StateRef(cashTx.id, 0), StateRef(cashTx2.id, 0)), SecureHash.allOnesHash)
        createVaultStatesFromTransaction(cashTx, stateStatus = Vault.StateStatus.CONSUMED)
        createVaultStatesFromTransaction(cashTx2, stateStatus = Vault.StateStatus.CONSUMED)
        createVaultStatesFromNotaryChangeTransaction(notaryTx, cashTx.coreTransaction.outputs + cashTx2.coreTransaction.outputs)
        storeTransaction(cashTx)
        storeTransaction(cashTx2)
        storeTransaction(notaryTx)
        val migration = VaultStateMigration()
        migration.execute(liquibaseDB)
        assertEquals(2, getStatePartyCount())
    }

    // Used to test migration performance
    @Test(timeout=300_000)
@Ignore
    fun `Migrate large database`() {
        val statesAtOnce = 500L
        val stateMultiplier = 300L
        logger.info("Start adding states to vault")
        (1..stateMultiplier).forEach {
            addCashStates(statesAtOnce.toInt(), BOB)
        }
        logger.info("Finish adding states to vault")
        val migration = VaultStateMigration()
        migration.execute(liquibaseDB)
        assertEquals((statesAtOnce * stateMultiplier), getStatePartyCount())
    }

    private fun makePersistentDataSourceProperties(): Properties {
        val props = Properties()
        props.setProperty("dataSourceClassName", "org.h2.jdbcx.JdbcDataSource")
        props.setProperty("dataSource.url", "jdbc:h2:~/test/persistence;DB_CLOSE_ON_EXIT=TRUE")
        props.setProperty("dataSource.user", "sa")
        props.setProperty("dataSource.password", "")
        return props
    }

    // Used to generate a persistent database for further testing.
    @Test(timeout=300_000)
@Ignore
    fun `Create persistent DB`() {
        val cashStatesToAdd = 1000
        val linearStatesToAdd = 0
        val commodityStatesToAdd = 0
        val stateMultiplier = 10

        cordaDB = configureDatabase(makePersistentDataSourceProperties(), DatabaseConfig(), notaryServices.identityService::wellKnownPartyFromX500Name, notaryServices.identityService::wellKnownPartyFromAnonymous)

        // Starting the database this way runs the migration under test. This is fine for the unit tests (as the changelog table is ignored),
        // but when starting an actual node using these databases the migration will be skipped, as it has an entry in the changelog table.
        // This must therefore be removed.
        cordaDB.dataSource.connection.createStatement().use {
            it.execute("DELETE FROM DATABASECHANGELOG WHERE FILENAME IN ('migration/vault-schema.changelog-v9.xml')")
        }

        for (i in 1..stateMultiplier) {
            addCashStates(cashStatesToAdd, BOB)
            addLinearStates(linearStatesToAdd, listOf(BOB, ALICE))
            addCommodityStates(commodityStatesToAdd, BOB)
        }
        saveOurKeys(listOf(bob.keyPair))
        saveAllIdentities(listOf(BOB_IDENTITY, ALICE_IDENTITY, BOC_IDENTITY, dummyNotary.identity))
        cordaDB.close()
    }

    @Test(timeout=300_000)
@Ignore
    fun `Run on persistent DB`() {
        cordaDB = configureDatabase(makePersistentDataSourceProperties(), DatabaseConfig(), notaryServices.identityService::wellKnownPartyFromX500Name, notaryServices.identityService::wellKnownPartyFromAnonymous)
        val connection = (liquibaseDB.connection as JdbcConnection)
        Mockito.`when`(connection.url).thenReturn(cordaDB.jdbcUrl)
        Mockito.`when`(connection.wrappedConnection).thenReturn(cordaDB.dataSource.connection)
        val migration = VaultStateMigration()
        migration.execute(liquibaseDB)
        cordaDB.close()
    }
}

