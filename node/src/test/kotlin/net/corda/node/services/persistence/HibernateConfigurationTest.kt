package net.corda.node.services.persistence

import com.nhaarman.mockito_kotlin.*
import net.corda.core.contracts.Amount
import net.corda.core.contracts.StateAndRef
import net.corda.core.contracts.StateRef
import net.corda.core.contracts.UniqueIdentifier
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.generateKeyPair
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.node.StatesToRecord
import net.corda.core.node.services.IdentityService
import net.corda.core.node.services.Vault
import net.corda.core.node.services.VaultService
import net.corda.core.schemas.CommonSchemaV1
import net.corda.core.schemas.MappedSchema
import net.corda.core.schemas.PersistentStateRef
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.toBase58String
import net.corda.finance.DOLLARS
import net.corda.finance.POUNDS
import net.corda.finance.SWISS_FRANCS
import net.corda.finance.contracts.asset.Cash
import net.corda.finance.contracts.utils.sumCash
import net.corda.finance.schemas.CashSchemaV1
import net.corda.finance.test.SampleCashSchemaV1
import net.corda.finance.test.SampleCashSchemaV2
import net.corda.finance.test.SampleCashSchemaV3
import net.corda.node.services.api.WritableTransactionStorage
import net.corda.node.services.schema.ContractStateAndRef
import net.corda.node.services.schema.NodeSchemaService
import net.corda.node.services.schema.PersistentStateService
import net.corda.node.services.vault.NodeVaultService
import net.corda.node.services.vault.VaultSchemaV1
import net.corda.node.testing.DummyFungibleContract
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.nodeapi.internal.persistence.DatabaseConfig
import net.corda.nodeapi.internal.persistence.HibernateConfiguration
import net.corda.nodeapi.internal.persistence.HibernateSchemaChangeException
import net.corda.testing.core.*
import net.corda.testing.internal.configureDatabase
import net.corda.testing.internal.vault.DummyDealStateSchemaV1
import net.corda.testing.internal.vault.DummyLinearStateSchemaV1
import net.corda.testing.internal.vault.DummyLinearStateSchemaV2
import net.corda.testing.internal.vault.VaultFiller
import net.corda.testing.node.MockServices
import net.corda.testing.node.MockServices.Companion.makeTestDataSourceProperties
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.`in`
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.hibernate.SessionFactory
import org.junit.*
import java.math.BigDecimal
import java.time.Clock
import java.time.Instant
import java.util.*
import javax.persistence.EntityManager
import javax.persistence.Tuple
import javax.persistence.criteria.CriteriaBuilder

class HibernateConfigurationTest {
    private companion object {
        val ALICE = TestIdentity(ALICE_NAME, 70).party
        val bankOfCorda = TestIdentity(BOC_NAME)
        val CHARLIE = TestIdentity(CHARLIE_NAME, 90).party
        val dummyCashIssuer = TestIdentity(CordaX500Name("Snake Oil Issuer", "London", "GB"), 10)
        val dummyNotary = TestIdentity(DUMMY_NOTARY_NAME, 20)
        val BOC get() = bankOfCorda.party
        val BOC_KEY get() = bankOfCorda.keyPair
    }

    @Rule
    @JvmField
    val testSerialization = SerializationEnvironmentRule()
    lateinit var services: MockServices
    private lateinit var vaultFiller: VaultFiller
    private lateinit var bankServices: MockServices
    private lateinit var issuerServices: MockServices
    private lateinit var notaryServices: MockServices
    lateinit var database: CordaPersistence
    val vault: VaultService get() = services.vaultService

    // Hibernate configuration objects
    lateinit var hibernateConfig: HibernateConfiguration
    private lateinit var hibernatePersister: PersistentStateService
    private lateinit var sessionFactory: SessionFactory
    private lateinit var entityManager: EntityManager
    private lateinit var criteriaBuilder: CriteriaBuilder

    // Identities used
    private lateinit var identity: Party
    private lateinit var issuer: Party
    private lateinit var notary: Party

    // test States
    private lateinit var cashStates: List<StateAndRef<Cash.State>>

    @Before
    fun setUp() {
        val cordappPackages = listOf("net.corda.testing.internal.vault", "net.corda.finance.contracts.asset", "net.corda.finance.schemas")
        bankServices = MockServices(cordappPackages, BOC.name, mock(), BOC_KEY)
        issuerServices = MockServices(cordappPackages, dummyCashIssuer, mock<IdentityService>())
        notaryServices = MockServices(cordappPackages, dummyNotary, mock<IdentityService>())
        notary = notaryServices.myInfo.singleIdentity()
        val dataSourceProps = makeTestDataSourceProperties()
        val identityService = mock<IdentityService>().also { mock ->
            doReturn(null).whenever(mock).wellKnownPartyFromAnonymous(any<AbstractParty>())
            listOf(dummyCashIssuer, dummyNotary).forEach {
                doReturn(it.party).whenever(mock).wellKnownPartyFromAnonymous(it.party)
                doReturn(it.party).whenever(mock).wellKnownPartyFromX500Name(it.name)
            }
        }
        val schemaService = NodeSchemaService(extraSchemas = setOf(CashSchemaV1, SampleCashSchemaV1, SampleCashSchemaV2, SampleCashSchemaV3, DummyLinearStateSchemaV1, DummyLinearStateSchemaV2, DummyDealStateSchemaV1))
        database = configureDatabase(dataSourceProps, DatabaseConfig(), identityService::wellKnownPartyFromX500Name, identityService::wellKnownPartyFromAnonymous, schemaService)
        database.transaction {
            hibernateConfig = database.hibernateConfig

            // `consumeCash` expects we can self-notarise transactions
            services = object : MockServices(cordappPackages, BOB_NAME, mock<IdentityService>().also {
                doReturn(null).whenever(it).verifyAndRegisterIdentity(argThat { name == BOB_NAME })
            }, generateKeyPair(), dummyNotary.keyPair) {
                override val vaultService = NodeVaultService(Clock.systemUTC(), keyManagementService, servicesForResolution, database, schemaService, cordappClassloader).apply { start() }
                override fun recordTransactions(statesToRecord: StatesToRecord, txs: Iterable<SignedTransaction>) {
                    for (stx in txs) {
                        (validatedTransactions as WritableTransactionStorage).addTransaction(stx)
                    }
                    // Refactored to use notifyAll() as we have no other unit test for that method with multiple transactions.
                    vaultService.notifyAll(statesToRecord, txs.map { it.tx })
                }

                override fun jdbcSession() = database.createSession()
            }
            vaultFiller = VaultFiller(services, dummyNotary, notary, ::Random)
            hibernatePersister = PersistentStateService(schemaService)
        }

        identity = services.myInfo.singleIdentity()
        issuer = issuerServices.myInfo.singleIdentity()
        database.transaction {
            val numStates = 10
            cashStates = vaultFiller.fillWithSomeTestCash(100.DOLLARS, issuerServices, numStates, issuer.ref(1), rng = Random(0L)).states.toList()
        }

        sessionFactory = sessionFactoryForSchemas(VaultSchemaV1, CashSchemaV1, SampleCashSchemaV2, SampleCashSchemaV3)
        entityManager = sessionFactory.createEntityManager()
        criteriaBuilder = sessionFactory.criteriaBuilder
    }

    private fun sessionFactoryForSchemas(vararg schemas: MappedSchema) = hibernateConfig.sessionFactoryForSchemas(schemas.toSet())
    private fun consumeCash(amount: Amount<Currency>) = vaultFiller.consumeCash(amount, CHARLIE)
    @After
    fun cleanUp() {
        database.close()
    }

    @Test(timeout=300_000)
	fun `count rows`() {
        // structure query
        val countQuery = criteriaBuilder.createQuery(Long::class.java)
        countQuery.select(criteriaBuilder.count(countQuery.from(VaultSchemaV1.VaultStates::class.java)))

        // execute query
        val countResult = entityManager.createQuery(countQuery).singleResult

        assertThat(countResult).isEqualTo(10)
    }

    @Test(timeout=300_000)
	fun `consumed states`() {
        database.transaction {
            consumeCash(50.DOLLARS)
        }

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(VaultSchemaV1.VaultStates::class.java)
        val vaultStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)
        criteriaQuery.where(criteriaBuilder.equal(
                vaultStates.get<Vault.StateStatus>("stateStatus"), Vault.StateStatus.CONSUMED))

        // execute query
        val queryResults = entityManager.createQuery(criteriaQuery).resultList
        val coins = queryResults.map {
            services.loadState(toStateRef(it.stateRef!!)).data
        }.sumCash()
        assertThat(coins.toDecimal() >= BigDecimal("50.00"))
    }

    @Test(timeout=300_000)
	fun `select by composite primary key`() {
        val issuedStates =
                database.transaction {
                    vaultFiller.fillWithSomeTestLinearStates(8)
                    vaultFiller.fillWithSomeTestLinearStates(2)
                }
        val persistentStateRefs = issuedStates.states.map { PersistentStateRef(it.ref) }.toList()

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(VaultSchemaV1.VaultStates::class.java)
        val vaultStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)
        val compositeKey = vaultStates.get<PersistentStateRef>("stateRef")
        criteriaQuery.where(criteriaBuilder.and(compositeKey.`in`(persistentStateRefs)))

        // execute query
        val queryResults = entityManager.createQuery(criteriaQuery).resultList

        assertThat(queryResults).hasSize(2)
        assertThat(queryResults.first().stateRef?.txId).isEqualTo(issuedStates.states.first().ref.txhash.toString())
        assertThat(queryResults.first().stateRef?.index).isEqualTo(issuedStates.states.first().ref.index)
        assertThat(queryResults.last().stateRef?.txId).isEqualTo(issuedStates.states.last().ref.txhash.toString())
        assertThat(queryResults.last().stateRef?.index).isEqualTo(issuedStates.states.last().ref.index)
    }

    @Test(timeout=300_000)
	fun `distinct contract types`() {
        database.transaction {
            // add 2 more contract types
            vaultFiller.fillWithSomeTestLinearStates(10)
            vaultFiller.fillWithSomeTestDeals(listOf("123", "456", "789"))
        }

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(String::class.java)
        val vaultStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)
        criteriaQuery.select(vaultStates.get("contractStateClassName")).distinct(true)

        // execute query
        val queryResults = entityManager.createQuery(criteriaQuery).resultList
        Assertions.assertThat(queryResults.size).isEqualTo(3)
    }

    @Test(timeout=300_000)
	fun `with sorting`() {
        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(VaultSchemaV1.VaultStates::class.java)
        val vaultStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)

        // order by DESC
        criteriaQuery.orderBy(criteriaBuilder.desc(vaultStates.get<Instant>("recordedTime")))
        val queryResults = entityManager.createQuery(criteriaQuery).resultList
        queryResults.map { println(it.recordedTime) }

        // order by ASC
        criteriaQuery.orderBy(criteriaBuilder.asc(vaultStates.get<Instant>("recordedTime")))
        val queryResultsAsc = entityManager.createQuery(criteriaQuery).resultList
        queryResultsAsc.map { println(it.recordedTime) }
    }

    @Test(timeout=300_000)
	fun `with sorting by state ref desc and asc`() {
        // generate additional state ref indexes
        database.transaction {
            (1..5).forEach { consumeCash(it.DOLLARS) }
        }

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(VaultSchemaV1.VaultStates::class.java)
        val vaultStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)

        val sortByStateRef = vaultStates.get<PersistentStateRef>("stateRef")

        // order by DESC
        criteriaQuery.orderBy(criteriaBuilder.desc(sortByStateRef))
        val queryResults = entityManager.createQuery(criteriaQuery).resultList
        println("DESC by stateRef")
        queryResults.map { println(it.stateRef) }

        // order by ASC
        criteriaQuery.orderBy(criteriaBuilder.asc(sortByStateRef))
        val queryResultsAsc = entityManager.createQuery(criteriaQuery).resultList
        println("ASC by stateRef")
        queryResultsAsc.map { println(it.stateRef) }
    }

    @Test(timeout=300_000)
	fun `with sorting by state ref index and txId desc and asc`() {
        // generate additional state ref indexes
        database.transaction {
            (1..5).forEach { consumeCash(it.DOLLARS) }
        }

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(VaultSchemaV1.VaultStates::class.java)
        val vaultStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)

        val sortByIndex = vaultStates.get<PersistentStateRef>("stateRef").get<String>("index")
        val sortByTxId = vaultStates.get<PersistentStateRef>("stateRef").get<String>("txId")

        // order by DESC
        criteriaQuery.orderBy(criteriaBuilder.desc(sortByIndex), criteriaBuilder.desc(sortByTxId))
        val queryResults = entityManager.createQuery(criteriaQuery).resultList
        println("DESC by index txId")
        queryResults.map { println(it.stateRef) }

        // order by ASC
        criteriaQuery.orderBy(criteriaBuilder.asc(sortByIndex), criteriaBuilder.asc(sortByTxId))
        val queryResultsAsc = entityManager.createQuery(criteriaQuery).resultList
        println("ASC by index txId")
        queryResultsAsc.map { println(it.stateRef) }
    }

    @Test(timeout=300_000)
	fun `with pagination`() {
        // add 100 additional cash entries
        database.transaction {
            vaultFiller.fillWithSomeTestCash(1000.POUNDS, issuerServices, 100, issuer.ref(1), rng = Random(0L))
        }

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(VaultSchemaV1.VaultStates::class.java)
        criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)

        // set pagination
        val query = entityManager.createQuery(criteriaQuery)
        query.firstResult = 10
        query.maxResults = 15

        // execute query
        val queryResults = query.resultList
        Assertions.assertThat(queryResults.size).isEqualTo(15)

        // try towards end
        query.firstResult = 100
        query.maxResults = 15

        val lastQueryResults = query.resultList

        Assertions.assertThat(lastQueryResults.size).isEqualTo(10)
    }

    /**
     *  VaultLinearState is a concrete table, extendible by any Contract extending a LinearState
     */
    @Test(timeout=300_000)
	fun `select by composite primary key on LinearStates`() {
        database.transaction {
            vaultFiller.fillWithSomeTestLinearStates(10)
        }

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(VaultSchemaV1.VaultStates::class.java)

        val vaultStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)
        val vaultLinearStates = criteriaQuery.from(VaultSchemaV1.VaultLinearStates::class.java)

        criteriaQuery.select(vaultStates)
        criteriaQuery.where(criteriaBuilder.equal(vaultStates.get<PersistentStateRef>("stateRef"), vaultLinearStates.get<PersistentStateRef>("stateRef")))

        // execute query
        val queryResults = entityManager.createQuery(criteriaQuery).resultList
        assertThat(queryResults).hasSize(10)
    }

    /**
     *  VaultFungibleState is an abstract entity, which should be extended any Contract extending a FungibleAsset
     */

    /**
     *  CashSchemaV1 = original Cash schema (extending PersistentState)
     */
    @Test(timeout=300_000)
	fun `count CashStates`() {
        // structure query
        val countQuery = criteriaBuilder.createQuery(Long::class.java)
        countQuery.select(criteriaBuilder.count(countQuery.from(CashSchemaV1.PersistentCashState::class.java)))

        // execute query
        val countResult = entityManager.createQuery(countQuery).singleResult

        assertThat(countResult).isEqualTo(10)
    }

    @Test(timeout=300_000)
	fun `select by composite primary key on CashStates`() {
        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(VaultSchemaV1.VaultStates::class.java)
        val vaultStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)
        vaultStates.join<VaultSchemaV1.VaultStates, CashSchemaV1.PersistentCashState>("stateRef")

        // execute query
        val queryResults = entityManager.createQuery(criteriaQuery).resultList
        assertThat(queryResults).hasSize(10)
    }

    @Test(timeout=300_000)
	fun `select and join by composite primary key on CashStates`() {
        database.transaction {
            vaultFiller.fillWithSomeTestLinearStates(5)
            // structure query
            val criteriaQuery = criteriaBuilder.createQuery(VaultSchemaV1.VaultStates::class.java)
            val vaultStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)
            val vaultCashStates = criteriaQuery.from(CashSchemaV1.PersistentCashState::class.java)

            criteriaQuery.select(vaultStates)
            criteriaQuery.where(criteriaBuilder.equal(vaultStates.get<PersistentStateRef>("stateRef"), vaultCashStates.get<PersistentStateRef>("stateRef")))

            // execute query
            val queryResults = entityManager.createQuery(criteriaQuery).resultList
            assertThat(queryResults).hasSize(10)
        }
    }

    @Test(timeout=300_000)
	fun `calculate cash balances`() {
        database.transaction {
            vaultFiller.fillWithSomeTestCash(100.DOLLARS, issuerServices, 10, issuer.ref(1))        // +$100 = $200
            vaultFiller.fillWithSomeTestCash(50.POUNDS, issuerServices, 5, issuer.ref(1))            // £50 = £50
            vaultFiller.fillWithSomeTestCash(25.POUNDS, issuerServices, 5, issuer.ref(1))            // +£25 = £175
            vaultFiller.fillWithSomeTestCash(500.SWISS_FRANCS, issuerServices, 10, issuer.ref(1))   // CHF500 = CHF500
            vaultFiller.fillWithSomeTestCash(250.SWISS_FRANCS, issuerServices, 5, issuer.ref(1))     // +CHF250 = CHF750
        }

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(Tuple::class.java)
        val cashStates = criteriaQuery.from(CashSchemaV1.PersistentCashState::class.java)

        // aggregate function
        criteriaQuery.multiselect(cashStates.get<String>("currency"),
                criteriaBuilder.sum(cashStates.get<Long>("pennies")))
        // group by
        criteriaQuery.groupBy(cashStates.get<String>("currency"))

        // execute query
        val queryResults = entityManager.createQuery(criteriaQuery).resultList

        queryResults.forEach { tuple -> println("${tuple.get(0)} = ${tuple.get(1)}") }

        assertThat(queryResults[0].get(0)).isEqualTo("CHF")
        assertThat(queryResults[0].get(1)).isEqualTo(75000L)
        assertThat(queryResults[1].get(0)).isEqualTo("GBP")
        assertThat(queryResults[1].get(1)).isEqualTo(7500L)
        assertThat(queryResults[2].get(0)).isEqualTo("USD")
        assertThat(queryResults[2].get(1)).isEqualTo(20000L)
    }

    @Test(timeout=300_000)
	fun `calculate cash balance for single currency`() {
        database.transaction {
            vaultFiller.fillWithSomeTestCash(50.POUNDS, issuerServices, 5, issuer.ref(1))            // £50 = £50
            vaultFiller.fillWithSomeTestCash(25.POUNDS, issuerServices, 5, issuer.ref(1))            // +£25 = £175
        }

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(Tuple::class.java)
        val cashStates = criteriaQuery.from(CashSchemaV1.PersistentCashState::class.java)

        // aggregate function
        criteriaQuery.multiselect(cashStates.get<String>("currency"),
                criteriaBuilder.sum(cashStates.get<Long>("pennies")))

        // where
        criteriaQuery.where(criteriaBuilder.equal(cashStates.get<String>("currency"), "GBP"))

        // group by
        criteriaQuery.groupBy(cashStates.get<String>("currency"))

        // execute query
        val queryResults = entityManager.createQuery(criteriaQuery).resultList

        queryResults.forEach { tuple -> println("${tuple.get(0)} = ${tuple.get(1)}") }

        assertThat(queryResults[0].get(0)).isEqualTo("GBP")
        assertThat(queryResults[0].get(1)).isEqualTo(7500L)
    }

    @Test(timeout=300_000)
	fun `calculate and order by cash balance for owner and currency`() {
        database.transaction {
            val bank = bankServices.myInfo.legalIdentities.single()
            vaultFiller.fillWithSomeTestCash(200.DOLLARS, bankServices, 2, bank.ref(1))
            vaultFiller.fillWithSomeTestCash(300.POUNDS, issuerServices, 3, issuer.ref(1))
            vaultFiller.fillWithSomeTestCash(400.POUNDS, bankServices, 4, bank.ref(2))
        }

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(Tuple::class.java)
        val cashStates = criteriaQuery.from(CashSchemaV1.PersistentCashState::class.java)

        // aggregate function
        criteriaQuery.multiselect(cashStates.get<String>("currency"),
                criteriaBuilder.sum(cashStates.get<Long>("pennies")))

        // group by
        criteriaQuery.groupBy(cashStates.get<String>("issuerPartyHash"), cashStates.get<String>("currency"))

        // order by
        criteriaQuery.orderBy(criteriaBuilder.desc(criteriaBuilder.sum(cashStates.get<Long>("pennies"))))

        // execute query
        val queryResults = entityManager.createQuery(criteriaQuery).resultList

        queryResults.forEach { tuple -> println("${tuple.get(0)} = ${tuple.get(1)}") }

        assertThat(queryResults).hasSize(4)
        assertThat(queryResults[0].get(0)).isEqualTo("GBP")
        assertThat(queryResults[0].get(1)).isEqualTo(40000L)
        assertThat(queryResults[1].get(0)).isEqualTo("GBP")
        assertThat(queryResults[1].get(1)).isEqualTo(30000L)
        assertThat(queryResults[2].get(0)).isEqualTo("USD")
        assertThat(queryResults[2].get(1)).isEqualTo(20000L)
        assertThat(queryResults[3].get(0)).isEqualTo("USD")
        assertThat(queryResults[3].get(1)).isEqualTo(10000L)
    }

    /**
     *  CashSchemaV2 = optimised Cash schema (extending FungibleState)
     */
    @Test(timeout=300_000)
	fun `count CashStates in V2`() {
        database.transaction {
            // persist cash states explicitly with V2 schema
            val stateAndRefs = cashStates.map {
                val cashState = it.state.data
                val dummyFungibleState = DummyFungibleContract.State(cashState.amount, cashState.owner)
                ContractStateAndRef(dummyFungibleState, it.ref)
            }
            hibernatePersister.persistStatesWithSchema(stateAndRefs, SampleCashSchemaV2)
        }

        // structure query
        val countQuery = criteriaBuilder.createQuery(Long::class.java)
        countQuery.select(criteriaBuilder.count(countQuery.from(SampleCashSchemaV2.PersistentCashState::class.java)))

        // execute query
        val countResult = entityManager.createQuery(countQuery).singleResult

        assertThat(countResult).isEqualTo(10)
    }

    @Test(timeout=300_000)
	fun `select by composite primary key on CashStates in V2`() {
        database.transaction {
            vaultFiller.fillWithSomeTestLinearStates(5)
            // persist cash states explicitly with V2 schema
            val stateAndRefs = cashStates.map {
                val cashState = it.state.data
                val dummyFungibleState = DummyFungibleContract.State(cashState.amount, cashState.owner)
                ContractStateAndRef(dummyFungibleState, it.ref)
            }
            hibernatePersister.persistStatesWithSchema(stateAndRefs, SampleCashSchemaV2)
        }

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(VaultSchemaV1.VaultStates::class.java)
        val vaultStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)
        val vaultCashStates = criteriaQuery.from(SampleCashSchemaV2.PersistentCashState::class.java)

        criteriaQuery.select(vaultStates)
        criteriaQuery.where(criteriaBuilder.equal(vaultStates.get<PersistentStateRef>("stateRef"), vaultCashStates.get<PersistentStateRef>("stateRef")))

        // execute query
        val queryResults = entityManager.createQuery(criteriaQuery).resultList
        assertThat(queryResults).hasSize(10)
    }

    /**
     *  Represents a 3-way join between:
     *      - VaultStates
     *      - VaultLinearStates
     *      - a concrete LinearState implementation (eg. DummyLinearState)
     */

    /**
     *  DummyLinearStateV1 = original DummyLinearState schema (extending PersistentState)
     */
    @Test(timeout=300_000)
	fun `select by composite primary between VaultStates, VaultLinearStates and DummyLinearStates`() {
        database.transaction {
            vaultFiller.fillWithSomeTestLinearStates(8)
            vaultFiller.fillWithSomeTestDeals(listOf("123", "456", "789"))
            vaultFiller.fillWithSomeTestLinearStates(2)
        }
        val sessionFactory = sessionFactoryForSchemas(VaultSchemaV1, DummyLinearStateSchemaV1)
        val criteriaBuilder = sessionFactory.criteriaBuilder
        val entityManager = sessionFactory.createEntityManager()

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(VaultSchemaV1.VaultStates::class.java)
        val vaultStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)
        val vaultLinearStates = criteriaQuery.from(VaultSchemaV1.VaultLinearStates::class.java)
        val dummyLinearStates = criteriaQuery.from(DummyLinearStateSchemaV1.PersistentDummyLinearState::class.java)

        criteriaQuery.select(vaultStates)
        val joinPredicate1 = criteriaBuilder.equal(vaultStates.get<PersistentStateRef>("stateRef"), vaultLinearStates.get<PersistentStateRef>("stateRef"))
        val joinPredicate2 = criteriaBuilder.and(criteriaBuilder.equal(vaultStates.get<PersistentStateRef>("stateRef"), dummyLinearStates.get<PersistentStateRef>("stateRef")))
        criteriaQuery.where(joinPredicate1, joinPredicate2)

        // execute query
        val queryResults = entityManager.createQuery(criteriaQuery).resultList
        assertThat(queryResults).hasSize(10)
    }

    /**
     *  DummyLinearSchemaV2 = optimised DummyLinear schema (extending LinearState)
     */

    @Test(timeout=300_000)
	fun `three way join by composite primary between VaultStates, VaultLinearStates and DummyLinearStates`() {
        database.transaction {
            vaultFiller.fillWithSomeTestLinearStates(8)
            vaultFiller.fillWithSomeTestDeals(listOf("123", "456", "789"))
            vaultFiller.fillWithSomeTestLinearStates(2)
        }
        val sessionFactory = sessionFactoryForSchemas(VaultSchemaV1, DummyLinearStateSchemaV2)
        val criteriaBuilder = sessionFactory.criteriaBuilder
        val entityManager = sessionFactory.createEntityManager()

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(VaultSchemaV1.VaultStates::class.java)
        val vaultStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)
        val vaultLinearStates = criteriaQuery.from(VaultSchemaV1.VaultLinearStates::class.java)
        val dummyLinearStates = criteriaQuery.from(DummyLinearStateSchemaV2.PersistentDummyLinearState::class.java)

        criteriaQuery.select(vaultStates)
        val joinPredicate1 = criteriaBuilder.equal(vaultStates.get<PersistentStateRef>("stateRef"), vaultLinearStates.get<PersistentStateRef>("stateRef"))
        val joinPredicate2 = criteriaBuilder.and(criteriaBuilder.equal(vaultStates.get<PersistentStateRef>("stateRef"), dummyLinearStates.get<PersistentStateRef>("stateRef")))
        criteriaQuery.where(joinPredicate1, joinPredicate2)

        // execute query
        val queryResults = entityManager.createQuery(criteriaQuery).resultList
        assertThat(queryResults).hasSize(10)
    }

    /**
     *  Composite OR query
     */
    @Test(timeout=300_000)
	fun `composite or query across VaultStates, VaultLinearStates and DummyLinearStates`() {
        val uniqueID456 = UniqueIdentifier("456")
        database.transaction {
            vaultFiller.fillWithSomeTestLinearStates(1, externalId = "123", linearString = "123", linearNumber = 123, linearBoolean = true)
            vaultFiller.fillWithSomeTestLinearStates(1, uniqueIdentifier = uniqueID456)
            vaultFiller.fillWithSomeTestLinearStates(1, externalId = "789")
        }
        val sessionFactory = sessionFactoryForSchemas(VaultSchemaV1, DummyLinearStateSchemaV1)
        val criteriaBuilder = sessionFactory.criteriaBuilder
        val entityManager = sessionFactory.createEntityManager()

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(VaultSchemaV1.VaultStates::class.java)
        val vaultStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)
        val vaultLinearStates = criteriaQuery.from(VaultSchemaV1.VaultLinearStates::class.java)
        val dummyLinearStates = criteriaQuery.from(DummyLinearStateSchemaV1.PersistentDummyLinearState::class.java)

        criteriaQuery.select(vaultStates)
        val joinPredicate1 = criteriaBuilder.equal(vaultStates.get<PersistentStateRef>("stateRef"), vaultLinearStates.get<PersistentStateRef>("stateRef"))
        val joinPredicate2 = criteriaBuilder.and(criteriaBuilder.equal(vaultStates.get<PersistentStateRef>("stateRef"), dummyLinearStates.get<PersistentStateRef>("stateRef")))

        // and predicates on VaultLinearStates
        val andLinearStatesPredicate1 = criteriaBuilder.and(criteriaBuilder.equal(vaultLinearStates.get<String>("externalId"), uniqueID456.externalId))
        val andLinearStatesPredicate2 = criteriaBuilder.and(criteriaBuilder.equal(vaultLinearStates.get<UUID>("uuid"), uniqueID456.id))
        val andLinearStatesPredicate = criteriaBuilder.and(andLinearStatesPredicate1, andLinearStatesPredicate2)

        // and predicates on PersistentDummyLinearState
        val andDummyLinearStatesPredicate1 = criteriaBuilder.and(criteriaBuilder.equal(dummyLinearStates.get<String>("linearString"), "123"))
        val andDummyLinearStatesPredicate2 = criteriaBuilder.and(criteriaBuilder.equal(dummyLinearStates.get<Long>("linearNumber"), 123L))
        val andDummyLinearStatesPredicate3 = criteriaBuilder.and(criteriaBuilder.equal(dummyLinearStates.get<Boolean>("linearBoolean"), true))
        val andDummyLinearStatesPredicate = criteriaBuilder.and(andDummyLinearStatesPredicate1, criteriaBuilder.and(andDummyLinearStatesPredicate2, andDummyLinearStatesPredicate3))

        // or predicates
        val orPredicates = criteriaBuilder.or(andLinearStatesPredicate, andDummyLinearStatesPredicate)

        criteriaQuery.where(joinPredicate1, joinPredicate2, orPredicates)

        // execute query
        val queryResults = entityManager.createQuery(criteriaQuery).resultList
        assertThat(queryResults).hasSize(2)
    }

    /**
     *  Test a OneToOne table mapping
     */
    @Test(timeout=300_000)
	fun `select fungible states by owner party`() {
        database.transaction {
            // persist original cash states explicitly with V3 schema
            val stateAndRefs = cashStates.map {
                val cashState = it.state.data
                val dummyFungibleState = DummyFungibleContract.State(cashState.amount, cashState.owner)
                ContractStateAndRef(dummyFungibleState, it.ref)
            }
            hibernatePersister.persistStatesWithSchema(stateAndRefs, SampleCashSchemaV3)
        }

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(SampleCashSchemaV3.PersistentCashState::class.java)
        criteriaQuery.from(SampleCashSchemaV3.PersistentCashState::class.java)

        // execute query
        val queryResults = entityManager.createQuery(criteriaQuery).resultList
        assertThat(queryResults).hasSize(10)
    }

    /**
     *  Test Query by Party (OneToOne table mapping)
     */
    @Test(timeout=300_000)
	fun `query fungible states by owner party`() {
        database.transaction {
            // persist original cash states explicitly with V3 schema
            val stateAndRefs: MutableList<ContractStateAndRef> = cashStates.map {
                val cashState = it.state.data
                val dummyFungibleState = DummyFungibleContract.State(cashState.amount, cashState.owner)
                ContractStateAndRef(dummyFungibleState, it.ref)
            }.toMutableList()
            vaultFiller.fillWithSomeTestCash(100.DOLLARS, issuerServices, 2, issuer.ref(1), ALICE, Random(0L))
            val cashStates = vaultFiller.fillWithSomeTestCash(100.DOLLARS, services, 2, identity.ref(0)).states
            // persist additional cash states explicitly with V3 schema
            stateAndRefs.addAll(cashStates.map {
                val cashState = it.state.data
                val dummyFungibleState = DummyFungibleContract.State(cashState.amount, cashState.owner)
                ContractStateAndRef(dummyFungibleState, it.ref)
            })
            hibernatePersister.persistStatesWithSchema(stateAndRefs, SampleCashSchemaV3)
        }
        val sessionFactory = sessionFactoryForSchemas(VaultSchemaV1, CommonSchemaV1, SampleCashSchemaV3)
        val criteriaBuilder = sessionFactory.criteriaBuilder
        val entityManager = sessionFactory.createEntityManager()

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(VaultSchemaV1.VaultStates::class.java)

        // select
        val vaultStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)
        criteriaQuery.select(vaultStates)

        // search predicate
        val cashStatesSchema = criteriaQuery.from(SampleCashSchemaV3.PersistentCashState::class.java)

        val queryOwner = identity.name.toString()
        criteriaQuery.where(criteriaBuilder.equal(cashStatesSchema.get<String>("owner"), queryOwner))

        val joinVaultStatesToCash = criteriaBuilder.equal(vaultStates.get<PersistentStateRef>("stateRef"), cashStatesSchema.get<PersistentStateRef>("stateRef"))
        criteriaQuery.where(joinVaultStatesToCash)

        // execute query
        val queryResults = entityManager.createQuery(criteriaQuery).resultList

        queryResults.forEach {
            val cashState = services.loadState(toStateRef(it.stateRef!!)).data as Cash.State
            println("${it.stateRef} with owner: ${cashState.owner.owningKey.toBase58String()}")
        }

        assertThat(queryResults).hasSize(12)
    }

    /**
     *  Test a OneToMany table mapping
     */
    @Test(timeout=300_000)
	fun `select fungible states by participants`() {
        database.transaction {
            // persist cash states explicitly with V3 schema
            val stateAndRefs = cashStates.map {
                val cashState = it.state.data
                val dummyFungibleState = DummyFungibleContract.State(cashState.amount, cashState.owner)
                ContractStateAndRef(dummyFungibleState, it.ref)
            }
            hibernatePersister.persistStatesWithSchema(stateAndRefs, SampleCashSchemaV3)
        }

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(SampleCashSchemaV3.PersistentCashState::class.java)
        criteriaQuery.from(SampleCashSchemaV3.PersistentCashState::class.java)

        // execute query
        val queryResults = entityManager.createQuery(criteriaQuery).resultList

        assertThat(queryResults).hasSize(10)
    }

    /**
     *  Test Query by participants (OneToMany table mapping)
     */
    @Test(timeout=300_000)
	fun `query fungible states by participants`() {
        val firstCashState =
                database.transaction {
                    // persist original cash states explicitly with V3 schema
                    val stateAndRefs: MutableList<ContractStateAndRef> = cashStates.map {
                        val cashState = it.state.data
                        val dummyFungibleState = DummyFungibleContract.State(cashState.amount, cashState.owner)
                        ContractStateAndRef(dummyFungibleState, it.ref)
                    }.toMutableList()

                    val moreCash = vaultFiller.fillWithSomeTestCash(100.DOLLARS, services, 2, identity.ref(0), identity, Random(0L)).states
                    // persist additional cash states explicitly with V3 schema
                    stateAndRefs.addAll(moreCash.map {
                        val cashState = it.state.data
                        val dummyFungibleState = DummyFungibleContract.State(cashState.amount, cashState.owner)
                        ContractStateAndRef(dummyFungibleState, it.ref)
                    })
                    val cashStates = vaultFiller.fillWithSomeTestCash(100.DOLLARS, issuerServices, 2, issuer.ref(1), ALICE, Random(0L)).states
                    // persist additional cash states explicitly with V3 schema
                    stateAndRefs.addAll(cashStates.map {
                        val cashState = it.state.data
                        val dummyFungibleState = DummyFungibleContract.State(cashState.amount, cashState.owner)
                        ContractStateAndRef(dummyFungibleState, it.ref)
                    })
                    hibernatePersister.persistStatesWithSchema(stateAndRefs, SampleCashSchemaV3)
                    cashStates.first()
                }

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(VaultSchemaV1.VaultStates::class.java)

        // select
        val vaultStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)
        criteriaQuery.select(vaultStates)

        // search predicate
        val cashStatesSchema = criteriaQuery.from(SampleCashSchemaV3.PersistentCashState::class.java)

        val queryParticipants = firstCashState.state.data.participants.map { it.nameOrNull().toString() }
        val joinCashStateToParty = cashStatesSchema.joinSet<SampleCashSchemaV3.PersistentCashState, String>("participants")
        criteriaQuery.where(criteriaBuilder.and(joinCashStateToParty.`in`(queryParticipants)))

        val joinVaultStatesToCash = criteriaBuilder.equal(vaultStates.get<PersistentStateRef>("stateRef"), cashStatesSchema.get<PersistentStateRef>("stateRef"))
        criteriaQuery.where(joinVaultStatesToCash)

        // execute query
        val queryResults = entityManager.createQuery(criteriaQuery).resultList
        queryResults.forEach {
            val cashState = services.loadState(toStateRef(it.stateRef!!)).data as Cash.State
            println("${it.stateRef} with owner ${cashState.owner.owningKey.toBase58String()} and participants ${cashState.participants.map { it.owningKey.toBase58String() }}")
        }

        assertThat(queryResults).hasSize(12)
    }

    /**
     * Query with sorting on Common table attribute
     */
    @Test(timeout=300_000)
	fun `with sorting on attribute from common table`() {

        database.transaction {
            vaultFiller.fillWithSomeTestLinearStates(1, externalId = "111")
            vaultFiller.fillWithSomeTestLinearStates(2, externalId = "222")
            vaultFiller.fillWithSomeTestLinearStates(3, externalId = "333")
        }
        val sessionFactory = sessionFactoryForSchemas(VaultSchemaV1, DummyLinearStateSchemaV2)
        val criteriaBuilder = sessionFactory.criteriaBuilder
        val entityManager = sessionFactory.createEntityManager()

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(Tuple::class.java)
        val vaultStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)
        val vaultLinearStates = criteriaQuery.from(VaultSchemaV1.VaultLinearStates::class.java)

        // join
        criteriaQuery.multiselect(vaultStates, vaultLinearStates)
        val joinPredicate = criteriaBuilder.equal(vaultStates.get<PersistentStateRef>("stateRef"), vaultLinearStates.get<PersistentStateRef>("stateRef"))
        criteriaQuery.where(joinPredicate)

        // order by DESC
        criteriaQuery.orderBy(criteriaBuilder.desc(vaultLinearStates.get<String>("externalId")))
        criteriaQuery.orderBy(criteriaBuilder.desc(vaultLinearStates.get<UUID>("uuid")))

        // execute query
        val queryResults = entityManager.createQuery(criteriaQuery).resultList
        queryResults.map {
            val vaultState = it[0] as VaultSchemaV1.VaultStates
            val vaultLinearState = it[1] as VaultSchemaV1.VaultLinearStates
            println("${vaultState.stateRef} : ${vaultLinearState.externalId} ${vaultLinearState.uuid}")
        }

        // order by ASC
        criteriaQuery.orderBy(criteriaBuilder.asc(vaultLinearStates.get<String>("externalId")))
        criteriaQuery.orderBy(criteriaBuilder.asc(vaultLinearStates.get<UUID>("uuid")))

        // execute query
        val queryResultsAsc = entityManager.createQuery(criteriaQuery).resultList
        queryResultsAsc.map {
            val vaultState = it[0] as VaultSchemaV1.VaultStates
            val vaultLinearState = it[1] as VaultSchemaV1.VaultLinearStates
            println("${vaultState.stateRef} : ${vaultLinearState.externalId} ${vaultLinearState.uuid}")
        }

        assertThat(queryResults).hasSize(6)
    }

    /**
     * Query with sorting on Custom table attribute
     */
    @Test(timeout=300_000)
	fun `with sorting on attribute from custom table`() {

        database.transaction {
            vaultFiller.fillWithSomeTestLinearStates(1, externalId = "111")
            vaultFiller.fillWithSomeTestLinearStates(2, externalId = "222")
            vaultFiller.fillWithSomeTestLinearStates(3, externalId = "333")
        }
        val sessionFactory = sessionFactoryForSchemas(VaultSchemaV1, DummyLinearStateSchemaV1)
        val criteriaBuilder = sessionFactory.criteriaBuilder
        val entityManager = sessionFactory.createEntityManager()

        // structure query
        val criteriaQuery = criteriaBuilder.createQuery(Tuple::class.java)
        val vaultStates = criteriaQuery.from(VaultSchemaV1.VaultStates::class.java)
        val vaultLinearStates = criteriaQuery.from(VaultSchemaV1.VaultLinearStates::class.java)
        val dummyLinearStates = criteriaQuery.from(DummyLinearStateSchemaV1.PersistentDummyLinearState::class.java)

        // join
        criteriaQuery.multiselect(vaultStates, vaultLinearStates, dummyLinearStates)
        val joinPredicate1 = criteriaBuilder.equal(vaultStates.get<PersistentStateRef>("stateRef"), vaultLinearStates.get<PersistentStateRef>("stateRef"))
        val joinPredicate2 = criteriaBuilder.and(criteriaBuilder.equal(vaultStates.get<PersistentStateRef>("stateRef"), dummyLinearStates.get<PersistentStateRef>("stateRef")))
        criteriaQuery.where(joinPredicate1, joinPredicate2)

        // order by DESC
        criteriaQuery.orderBy(criteriaBuilder.desc(dummyLinearStates.get<String>("externalId")))
        criteriaQuery.orderBy(criteriaBuilder.desc(dummyLinearStates.get<UUID>("uuid")))

        // execute query
        val queryResults = entityManager.createQuery(criteriaQuery).resultList
        queryResults.map {
            val vaultState = it[0] as VaultSchemaV1.VaultStates
            val _vaultLinearStates = it[1] as VaultSchemaV1.VaultLinearStates
            val _dummyLinearStates = it[2] as DummyLinearStateSchemaV1.PersistentDummyLinearState
            println("${vaultState.stateRef} : [${_dummyLinearStates.externalId} ${_dummyLinearStates.uuid}] : [${_vaultLinearStates.externalId} ${_vaultLinearStates.uuid}]")
        }

        // order by ASC
        criteriaQuery.orderBy(criteriaBuilder.asc(dummyLinearStates.get<String>("externalId")))
        criteriaQuery.orderBy(criteriaBuilder.asc(dummyLinearStates.get<UUID>("uuid")))

        // execute query
        val queryResultsAsc = entityManager.createQuery(criteriaQuery).resultList
        queryResultsAsc.map {
            val vaultState = it[0] as VaultSchemaV1.VaultStates
            val _vaultLinearStates = it[1] as VaultSchemaV1.VaultLinearStates
            val _dummyLinearStates = it[2] as DummyLinearStateSchemaV1.PersistentDummyLinearState
            println("${vaultState.stateRef} : [${_dummyLinearStates.externalId} ${_dummyLinearStates.uuid}] : [${_vaultLinearStates.externalId} ${_vaultLinearStates.uuid}]")
        }

        assertThat(queryResults).hasSize(6)
    }

    /**
     *  Test invoking SQL query using DB connection (session)
     */
    @Test(timeout=300_000)
	fun `test calling an arbitrary JDBC native query`() {
        // DOCSTART JdbcSession
        val nativeQuery = "SELECT v.transaction_id, v.output_index FROM vault_states v WHERE v.state_status = 0"

        database.transaction {
            val jdbcSession = services.jdbcSession()
            val prepStatement = jdbcSession.prepareStatement(nativeQuery)
            val rs = prepStatement.executeQuery()
            // DOCEND JdbcSession
            var count = 0
            while (rs.next()) {
                val stateRef = StateRef(SecureHash.create(rs.getString(1)), rs.getInt(2))
                Assert.assertTrue(cashStates.map { it.ref }.contains(stateRef))
                count++
            }
            Assert.assertEquals(cashStates.count(), count)
        }
    }

    private fun toStateRef(pStateRef: PersistentStateRef): StateRef {
        return StateRef(SecureHash.create(pStateRef.txId), pStateRef.index)
    }

    @Test(timeout=300_000)
	fun `schema change`() {
        fun createNewDB(schemas: Set<MappedSchema>, initialiseSchema: Boolean = true):  CordaPersistence {
            val schemaService = NodeSchemaService(extraSchemas = schemas)
            val dataSourceProps = makeTestDataSourceProperties("aa")
            val identityService = mock<IdentityService>().also { mock ->
                doReturn(null).whenever(mock).wellKnownPartyFromAnonymous(any<AbstractParty>())
                listOf(dummyCashIssuer, dummyNotary).forEach {
                    doReturn(it.party).whenever(mock).wellKnownPartyFromAnonymous(it.party)
                    doReturn(it.party).whenever(mock).wellKnownPartyFromX500Name(it.name)
                }
            }
            database = configureDatabase(dataSourceProps, DatabaseConfig(), identityService::wellKnownPartyFromX500Name, identityService::wellKnownPartyFromAnonymous, schemaService, runMigrationScripts = initialiseSchema, allowHibernateToManageAppSchema = initialiseSchema)
            return database
        }

        createNewDB(setOf(CashSchemaV1, SampleCashSchemaV1)).apply {
            database.transaction {
                vaultFiller.fillWithSomeTestCash(100.DOLLARS, issuerServices, 4, issuer.ref(1), rng = Random(0L))
            }
        }
        createNewDB(setOf(CashSchemaV1, SampleCashSchemaV1, SampleCashSchemaV2), initialiseSchema = false).use {
            assertThatThrownBy {
                it.transaction {
                    vaultFiller.fillWithSomeTestCash(100.DOLLARS, issuerServices, 4, issuer.ref(1), rng = Random(0L))
                }
            }.isInstanceOf(HibernateSchemaChangeException::class.java)
        }
    }
}
