package net.corda.node.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.CollectSignaturesFlow
import net.corda.core.flows.FinalityFlow
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.FlowSession
import net.corda.core.flows.InitiatedBy
import net.corda.core.flows.InitiatingFlow
import net.corda.core.flows.ReceiveFinalityFlow
import net.corda.core.flows.SignTransactionFlow
import net.corda.core.flows.StartableByRPC
import net.corda.core.identity.Party
import net.corda.core.internal.concurrent.transpose
import net.corda.core.messaging.startFlow
import net.corda.core.node.AppServiceHub
import net.corda.core.node.services.CordaService
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.millis
import net.corda.core.utilities.seconds
import net.corda.node.services.statemachine.StaffedFlowHospital
import net.corda.testing.contracts.DummyState
import net.corda.testing.core.ALICE_NAME
import net.corda.testing.core.BOB_NAME
import net.corda.testing.core.DummyCommandData
import net.corda.testing.core.singleIdentity
import net.corda.testing.driver.DriverParameters
import net.corda.testing.driver.NodeHandle
import net.corda.testing.driver.driver
import net.corda.testing.driver.reusableDriver
import org.hibernate.exception.ConstraintViolationException
import org.junit.Before
import org.junit.Test
import java.lang.RuntimeException
import java.sql.Connection
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import javax.persistence.EntityManager
import javax.persistence.PersistenceException
import kotlin.test.assertEquals

@Suppress("TooGenericExceptionCaught", "TooGenericExceptionThrown")
class FlowEntityManagerTest : AbstractFlowEntityManagerTest() {

    @Before
    override fun before() {
        MyService.includeRawUpdates = false
        super.before()
    }

    @Test(timeout = 300_000)
    fun `entities can be saved using entity manager without a flush`() {
        var counter = 0
        StaffedFlowHospital.onFlowDischarged.add { _, _ -> ++counter }

        reusableDriver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {

            val alice = startNode(providedName = ALICE_NAME).getOrThrow()
            recoverBy { resetNode(alice) }
            alice.rpc.startFlow(::EntityManagerSaveEntitiesWithoutAFlushFlow)
                .returnValue.getOrThrow(30.seconds)
            assertEquals(0, counter)
            val entities = alice.rpc.startFlow(::GetCustomEntities).returnValue.getOrThrow()
            assertEquals(3, entities.size)
        }
    }

    @Test(timeout = 300_000)
    fun `entities can be saved using entity manager with a flush`() {
        var counter = 0
        StaffedFlowHospital.onFlowDischarged.add { _, _ -> ++counter }
        reusableDriver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {

            val alice = startNode(providedName = ALICE_NAME).getOrThrow()
            recoverBy { resetNode(alice) }

            alice.rpc.startFlow(::EntityManagerSaveEntitiesWithAFlushFlow)
                .returnValue.getOrThrow(30.seconds)
            assertEquals(0, counter)
            val entities = alice.rpc.startFlow(::GetCustomEntities).returnValue.getOrThrow()
            assertEquals(3, entities.size)
        }
    }

    @Test(timeout = 300_000)
    fun `entities saved inside an entity manager are only committed when a flow suspends`() {
        var counter = 0
        StaffedFlowHospital.onFlowDischarged.add { _, _ -> ++counter }
        reusableDriver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {

            val alice = startNode(providedName = ALICE_NAME).getOrThrow()
            recoverBy { resetNode(alice) }

            var beforeCommitEntities: List<CustomTableEntity>? = null
            EntityManagerSaveEntitiesWithoutAFlushFlow.beforeCommitHook = {
                beforeCommitEntities = it
            }
            var afterCommitEntities: List<CustomTableEntity>? = null
            EntityManagerSaveEntitiesWithoutAFlushFlow.afterCommitHook = {
                afterCommitEntities = it
            }

            alice.rpc.startFlow(::EntityManagerSaveEntitiesWithoutAFlushFlow)
                    .returnValue.getOrThrow(30.seconds)
            assertEquals(0, counter)
            val entities = alice.rpc.startFlow(::GetCustomEntities).returnValue.getOrThrow()
            assertEquals(3, entities.size)
            assertEquals(0, beforeCommitEntities!!.size)
            assertEquals(3, afterCommitEntities!!.size)
        }
    }

    @Test(timeout = 300_000)
    fun `constraint violation without a flush breaks`() {
        reusableDriver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {

            val alice = startNode(providedName = ALICE_NAME).getOrThrow()
            recoverBy { resetNode(alice) }
            alice.rpc.expectFlowFailureAndAssertCreatedEntities(
                flow = ::EntityManagerErrorWithoutAFlushFlow,
                commitStatus = CommitStatus.NO_INTERMEDIATE_COMMIT,
                numberOfDischarges = 3,
                numberOfExpectedEntities = 0
            )
            alice.rpc.expectFlowFailureAndAssertCreatedEntities(
                flow = ::EntityManagerErrorWithoutAFlushFlow,
                commitStatus = CommitStatus.INTERMEDIATE_COMMIT,
                numberOfDischarges = 3,
                numberOfExpectedEntities = 1
            )
        }
    }

    @Test(timeout = 300_000)
    fun `constraint violation with a flush breaks`() {
        reusableDriver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {

            val alice = startNode(providedName = ALICE_NAME).getOrThrow()
            recoverBy { resetNode(alice) }
            alice.rpc.expectFlowFailureAndAssertCreatedEntities(
                flow = ::EntityManagerErrorWithAFlushFlow,
                commitStatus = CommitStatus.NO_INTERMEDIATE_COMMIT,
                numberOfDischarges = 3,
                numberOfExpectedEntities = 0
            )
            alice.rpc.expectFlowFailureAndAssertCreatedEntities(
                flow = ::EntityManagerErrorWithAFlushFlow,
                commitStatus = CommitStatus.INTERMEDIATE_COMMIT,
                numberOfDischarges = 3,
                numberOfExpectedEntities = 1
            )
        }
    }

    @Test(timeout = 300_000)
    fun `constraint violation with a flush that is caught inside an entity manager block saves none of the data inside of it`() {
        reusableDriver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {

            val alice = startNode(providedName = ALICE_NAME).getOrThrow()
            recoverBy { resetNode(alice) }
            // 1 entity saved from the first entity manager block that does not get rolled back
            // even if there is no intermediate commit to the database
            alice.rpc.expectFlowSuccessAndAssertCreatedEntities(
                flow = ::EntityManagerWithAFlushCatchErrorInsideTheEntityManagerFlow,
                commitStatus = CommitStatus.NO_INTERMEDIATE_COMMIT,
                numberOfDischarges = 0,
                numberOfExpectedEntities = 1
            )
            alice.rpc.expectFlowSuccessAndAssertCreatedEntities(
                flow = ::EntityManagerWithAFlushCatchErrorInsideTheEntityManagerFlow,
                commitStatus = CommitStatus.INTERMEDIATE_COMMIT,
                numberOfDischarges = 0,
                numberOfExpectedEntities = 1
            )
        }
    }

    @Test(timeout = 300_000)
    fun `constraint violation with a flush that is caught outside the entity manager block saves none of the data inside of it`() {
        reusableDriver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {

            val alice = startNode(providedName = ALICE_NAME).getOrThrow()
            recoverBy { resetNode(alice) }
            // 1 entity saved from the first entity manager block that does not get rolled back
            // even if there is no intermediate commit to the database
            alice.rpc.expectFlowSuccessAndAssertCreatedEntities(
                flow = ::EntityManagerWithAFlushCatchErrorOutsideTheEntityManagerFlow,
                commitStatus = CommitStatus.NO_INTERMEDIATE_COMMIT,
                numberOfDischarges = 0,
                numberOfExpectedEntities = 1
            )
            alice.rpc.expectFlowSuccessAndAssertCreatedEntities(
                flow = ::EntityManagerWithAFlushCatchErrorOutsideTheEntityManagerFlow,
                commitStatus = CommitStatus.INTERMEDIATE_COMMIT,
                numberOfDischarges = 0,
                numberOfExpectedEntities = 1
            )
        }
    }

    @Test(timeout = 300_000)
    fun `constraint violation within a single entity manager block throws an exception and saves no data`() {
        var dischargeCounter = 0
        StaffedFlowHospital.onFlowDischarged.add { _, _ -> ++dischargeCounter }
        val lock = Semaphore(0)
        StaffedFlowHospital.onFlowKeptForOvernightObservation.add { _, _ -> lock.release() }
        reusableDriver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {

            val alice = startNode(providedName = ALICE_NAME).getOrThrow()
            recoverBy { resetNode(alice) }
            alice.rpc.startFlow(::EntityManagerErrorInsideASingleEntityManagerFlow)
            lock.acquire()
            // Goes straight to observation due to throwing [EntityExistsException]
            assertEquals(0, dischargeCounter)
            val entities = alice.rpc.startFlow(::GetCustomEntities).returnValue.getOrThrow()
            assertEquals(0, entities.size)
        }
    }

    @Test(timeout = 300_000)
    fun `constraint violation on a single entity when saving multiple entities throws an exception and does not save any data within the entity manager block`() {
        reusableDriver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {

            val alice = startNode(providedName = ALICE_NAME).getOrThrow()
            recoverBy { resetNode(alice) }
            alice.rpc.expectFlowFailureAndAssertCreatedEntities(
                flow = ::EntityManagerSavingMultipleEntitiesWithASingleErrorFlow,
                commitStatus = CommitStatus.NO_INTERMEDIATE_COMMIT,
                numberOfDischarges = 3,
                numberOfExpectedEntities = 0
            )
            alice.rpc.expectFlowFailureAndAssertCreatedEntities(
                flow = ::EntityManagerSavingMultipleEntitiesWithASingleErrorFlow,
                commitStatus = CommitStatus.INTERMEDIATE_COMMIT,
                numberOfDischarges = 3,
                numberOfExpectedEntities = 1
            )
        }
    }

    @Test(timeout = 300_000)
    fun `constraint violation on a single entity when saving multiple entities and catching the error does not save any data within the entity manager block`() {
        reusableDriver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {

            val alice = startNode(providedName = ALICE_NAME).getOrThrow()
            recoverBy { resetNode(alice) }
            // 1 entity saved from the first entity manager block that does not get rolled back
            // even if there is no intermediate commit to the database
            alice.rpc.expectFlowSuccessAndAssertCreatedEntities(
                flow = ::EntityManagerSavingMultipleEntitiesWithASingleCaughtErrorFlow,
                commitStatus = CommitStatus.NO_INTERMEDIATE_COMMIT,
                numberOfDischarges = 0,
                numberOfExpectedEntities = 1
            )
            alice.rpc.expectFlowSuccessAndAssertCreatedEntities(
                flow = ::EntityManagerSavingMultipleEntitiesWithASingleCaughtErrorFlow,
                commitStatus = CommitStatus.INTERMEDIATE_COMMIT,
                numberOfDischarges = 0,
                numberOfExpectedEntities = 1
            )
        }
    }

    @Test(timeout = 300_000)
    fun `constraint violation that is caught inside an entity manager and more data is saved afterwards inside a new entity manager should save the extra data`() {
        reusableDriver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {

            val alice = startNode(providedName = ALICE_NAME).getOrThrow()
            recoverBy { resetNode(alice) }
            alice.rpc.expectFlowSuccessAndAssertCreatedEntities(
                flow = ::EntityManagerCatchErrorAndSaveMoreEntitiesInANewEntityManager,
                commitStatus = CommitStatus.NO_INTERMEDIATE_COMMIT,
                numberOfDischarges = 0,
                numberOfExpectedEntities = 3
            )
            alice.rpc.expectFlowSuccessAndAssertCreatedEntities(
                flow = ::EntityManagerCatchErrorAndSaveMoreEntitiesInANewEntityManager,
                commitStatus = CommitStatus.INTERMEDIATE_COMMIT,
                numberOfDischarges = 0,
                numberOfExpectedEntities = 3
            )
        }
    }

    @Test(timeout = 300_000)
    fun `constraint violation that is caught inside an entity manager and more data is saved afterwards inside the same entity manager should not save the extra data`() {
        reusableDriver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {

            val alice = startNode(providedName = ALICE_NAME).getOrThrow()
            recoverBy { resetNode(alice) }
            alice.rpc.expectFlowSuccessAndAssertCreatedEntities(
                flow = ::EntityManagerCatchErrorAndSaveMoreEntitiesInTheSameEntityManager,
                commitStatus = CommitStatus.NO_INTERMEDIATE_COMMIT,
                numberOfDischarges = 0,
                numberOfExpectedEntities = 1
            )
            alice.rpc.expectFlowSuccessAndAssertCreatedEntities(
                flow = ::EntityManagerCatchErrorAndSaveMoreEntitiesInTheSameEntityManager,
                commitStatus = CommitStatus.INTERMEDIATE_COMMIT,
                numberOfDischarges = 0,
                numberOfExpectedEntities = 1
            )
        }
    }

    @Test(timeout = 300_000)
    fun `constraint violation that is caught outside an entity manager and more data is saved afterwards inside a new entity manager should save the extra data`() {
        reusableDriver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {

            val alice = startNode(providedName = ALICE_NAME).getOrThrow()
            recoverBy { resetNode(alice) }
            alice.rpc.expectFlowSuccessAndAssertCreatedEntities(
                flow = ::EntityManagerCatchErrorOutsideTheEntityManagerAndSaveMoreEntitiesInANewEntityManager,
                commitStatus = CommitStatus.NO_INTERMEDIATE_COMMIT,
                numberOfDischarges = 0,
                numberOfExpectedEntities = 3
            )
            alice.rpc.expectFlowSuccessAndAssertCreatedEntities(
                flow = ::EntityManagerCatchErrorOutsideTheEntityManagerAndSaveMoreEntitiesInANewEntityManager,
                commitStatus = CommitStatus.INTERMEDIATE_COMMIT,
                numberOfDischarges = 0,
                numberOfExpectedEntities = 3
            )
        }
    }

    @Test(timeout = 300_000)
    fun `constraint violation that is caught inside an entity manager should allow a flow to continue processing as normal`() {
        var counter = 0
        StaffedFlowHospital.onFlowDischarged.add { _, _ -> ++counter }
        reusableDriver(DriverParameters(startNodesInProcess = true)) {

            val (alice, bob) = listOf(ALICE_NAME, BOB_NAME)
                    .map { startNode(providedName = it) }
                    .transpose()
                    .getOrThrow()
                    .onEach { node->
                        recoverBy { resetNode(node) } }

            val txId =
                alice.rpc.startFlow(::EntityManagerWithFlushCatchAndInteractWithOtherPartyFlow, bob.nodeInfo.singleIdentity())
                    .returnValue.getOrThrow(20.seconds)
            assertEquals(0, counter)
            val txFromVault = alice.rpc.stateMachineRecordedTransactionMappingSnapshot().firstOrNull()?.transactionId
            assertEquals(txId, txFromVault)
            val entity = alice.rpc.startFlow(::GetCustomEntities).returnValue.getOrThrow().single()
            assertEquals(entityWithIdOne, entity)
        }
    }

    @Test(timeout = 300_000)
    fun `data saved from an entity manager vault update should be visible within an entity manager block inside the same database transaction`() {
        MyService.includeRawUpdates = true
        MyService.insertionType = MyService.InsertionType.ENTITY_MANAGER
        var counter = 0
        StaffedFlowHospital.onFlowDischarged.add { _, _ -> ++counter }
        driver(DriverParameters(startNodesInProcess = true)) {

            val alice = startNode(providedName = ALICE_NAME).getOrThrow()

            val entities =
                alice.rpc.startFlow(::EntityManagerWithinTheSameDatabaseTransactionFlow).returnValue.getOrThrow(20.seconds)
            assertEquals(3, entities.size)
            assertEquals(0, counter)
        }
    }

    @Test(timeout = 300_000)
    fun `data saved from a jdbc connection vault update should be visible within an entity manager block inside the same database transaction`() {
        MyService.includeRawUpdates = true
        MyService.insertionType = MyService.InsertionType.CONNECTION
        var counter = 0
        StaffedFlowHospital.onFlowDischarged.add { _, _ -> ++counter }
        driver(DriverParameters(startNodesInProcess = true)) {

            val alice = startNode(providedName = ALICE_NAME).getOrThrow()

            val entities =
                alice.rpc.startFlow(::EntityManagerWithinTheSameDatabaseTransactionFlow).returnValue.getOrThrow(20.seconds)
            assertEquals(3, entities.size)
            assertEquals(0, counter)
        }
    }

    @Test(timeout = 300_000)
    fun `non database error caught outside entity manager does not save entities`() {
        var counter = 0
        StaffedFlowHospital.onFlowDischarged.add { _, _ -> ++counter }

        reusableDriver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {

            val alice = startNode(providedName = ALICE_NAME).getOrThrow()
            recoverBy { resetNode(alice) }
            alice.rpc.startFlow(::EntityManagerSaveAndThrowNonDatabaseErrorFlow)
                .returnValue.getOrThrow(30.seconds)
            assertEquals(0, counter)
            val entities = alice.rpc.startFlow(::GetCustomEntities).returnValue.getOrThrow()
            assertEquals(0, entities.size)

        }
    }

    @Test(timeout = 300_000)
    fun `non database error caught outside entity manager after flush occurs does save entities`() {
        var counter = 0
        StaffedFlowHospital.onFlowDischarged.add { _, _ -> ++counter }

        reusableDriver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {

            val alice = startNode(providedName = ALICE_NAME).getOrThrow()
            recoverBy { resetNode(alice) }
            alice.rpc.startFlow(::EntityManagerSaveFlushAndThrowNonDatabaseErrorFlow)
                .returnValue.getOrThrow(30.seconds)
            assertEquals(0, counter)
            val entities = alice.rpc.startFlow(::GetCustomEntities).returnValue.getOrThrow()
            assertEquals(3, entities.size)
        }
    }

    @Test(timeout = 300_000)
    fun `database error caught inside entity manager non database exception thrown and caught outside entity manager should not save entities`() {
        var counter = 0
        StaffedFlowHospital.onFlowDischarged.add { _, _ -> ++counter }

        reusableDriver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true)) {

            val alice = startNode(providedName = ALICE_NAME).getOrThrow()
            recoverBy { resetNode(alice) }
            alice.rpc.expectFlowSuccessAndAssertCreatedEntities(
                flow = ::EntityManagerCatchDatabaseErrorInsideEntityManagerThrowNonDatabaseErrorAndCatchOutsideFlow,
                commitStatus = CommitStatus.NO_INTERMEDIATE_COMMIT,
                numberOfDischarges = 0,
                numberOfExpectedEntities = 1
            )
            alice.rpc.expectFlowSuccessAndAssertCreatedEntities(
                flow = ::EntityManagerCatchDatabaseErrorInsideEntityManagerThrowNonDatabaseErrorAndCatchOutsideFlow,
                commitStatus = CommitStatus.INTERMEDIATE_COMMIT,
                numberOfDischarges = 0,
                numberOfExpectedEntities = 1
            )
        }
    }

    @StartableByRPC
    class EntityManagerSaveEntitiesWithoutAFlushFlow : FlowLogic<Unit>() {

        companion object {
            var beforeCommitHook: ((entities: List<CustomTableEntity>) -> Unit)? = null
            var afterCommitHook: ((entities: List<CustomTableEntity>) -> Unit)? = null
        }

        @Suspendable
        override fun call() {
            serviceHub.withEntityManager {
                persist(entityWithIdOne)
                persist(entityWithIdTwo)
                persist(entityWithIdThree)
            }
            beforeCommitHook?.invoke(serviceHub.cordaService(MyService::class.java).getEntities())
            sleep(1.millis)
            afterCommitHook?.invoke(serviceHub.cordaService(MyService::class.java).getEntities())
        }
    }

    @StartableByRPC
    class EntityManagerSaveEntitiesWithAFlushFlow : FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            serviceHub.withEntityManager {
                persist(entityWithIdOne)
                persist(entityWithIdTwo)
                persist(entityWithIdThree)
                flush()
            }
            sleep(1.millis)
        }
    }

    @StartableByRPC
    class EntityManagerResetFlow : FlowLogic<Unit>() {

        private fun EntityManager.remove(vararg entitis: CustomTableEntity) {
            entitis.forEach {entity ->
                val saved = find(CustomTableEntity::class.java, entity.id)
                if(saved != null) {
                    remove(saved)
                }

            }
        }

        @Suspendable
        override fun call() {
            serviceHub.withEntityManager {
                remove(entityWithIdOne,
                        anotherEntityWithIdOne,
                        entityWithIdTwo,
                        entityWithIdThree)
            }
        }
    }

    @StartableByRPC
    class EntityManagerErrorWithoutAFlushFlow(private val commitStatus: CommitStatus) : FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            serviceHub.withEntityManager {
                persist(entityWithIdOne)
            }
            if (commitStatus == CommitStatus.INTERMEDIATE_COMMIT) {
                sleep(1.millis)
            }
            serviceHub.withEntityManager {
                persist(anotherEntityWithIdOne)
            }
            sleep(1.millis)
        }
    }

    @StartableByRPC
    class EntityManagerErrorWithAFlushFlow(private val commitStatus: CommitStatus) : FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            serviceHub.withEntityManager {
                persist(entityWithIdOne)
            }
            if (commitStatus == CommitStatus.INTERMEDIATE_COMMIT) {
                sleep(1.millis)
            }
            serviceHub.withEntityManager {
                persist(anotherEntityWithIdOne)
                flush()
            }
            sleep(1.millis)
        }
    }

    @StartableByRPC
    class EntityManagerWithAFlushCatchErrorInsideTheEntityManagerFlow(private val commitStatus: CommitStatus) : FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            serviceHub.withEntityManager {
                persist(entityWithIdOne)
            }
            if (commitStatus == CommitStatus.INTERMEDIATE_COMMIT) {
                sleep(1.millis)
            }
            serviceHub.withEntityManager {
                persist(anotherEntityWithIdOne)
                try {
                    flush()
                } catch (e: PersistenceException) {
                    logger.info("Caught the exception!")
                }
            }
            sleep(1.millis)
        }
    }

    @StartableByRPC
    class EntityManagerWithAFlushCatchErrorOutsideTheEntityManagerFlow(private val commitStatus: CommitStatus) : FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            serviceHub.withEntityManager {
                persist(entityWithIdOne)
            }
            if (commitStatus == CommitStatus.INTERMEDIATE_COMMIT) {
                sleep(1.millis)
            }
            try {
                serviceHub.withEntityManager {
                    persist(anotherEntityWithIdOne)
                    flush()
                }
            } catch (e: PersistenceException) {
                logger.info("Caught the exception!")
            }
            sleep(1.millis)
        }
    }

    @StartableByRPC
    class EntityManagerErrorInsideASingleEntityManagerFlow : FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            serviceHub.withEntityManager {
                persist(entityWithIdOne)
                persist(anotherEntityWithIdOne)
            }
            sleep(1.millis)
        }
    }

    @StartableByRPC
    class EntityManagerSavingMultipleEntitiesWithASingleErrorFlow(private val commitStatus: CommitStatus) : FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            serviceHub.withEntityManager {
                persist(entityWithIdOne)
            }
            if (commitStatus == CommitStatus.INTERMEDIATE_COMMIT) {
                sleep(1.millis)
            }
            serviceHub.withEntityManager {
                persist(anotherEntityWithIdOne)
                persist(entityWithIdTwo)
                persist(entityWithIdThree)
            }
            sleep(1.millis)
        }
    }

    @StartableByRPC
    class EntityManagerSavingMultipleEntitiesWithASingleCaughtErrorFlow(private val commitStatus: CommitStatus) : FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            serviceHub.withEntityManager {
                persist(entityWithIdOne)
            }
            if (commitStatus == CommitStatus.INTERMEDIATE_COMMIT) {
                sleep(1.millis)
            }
            serviceHub.withEntityManager {
                persist(anotherEntityWithIdOne)
                persist(entityWithIdTwo)
                persist(entityWithIdThree)
                try {
                    flush()
                } catch (e: PersistenceException) {
                    logger.info("Caught the exception!")
                }
            }
            sleep(1.millis)
        }
    }

    @StartableByRPC
    class EntityManagerCatchErrorAndSaveMoreEntitiesInANewEntityManager(private val commitStatus: CommitStatus) : FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            serviceHub.withEntityManager {
                persist(entityWithIdOne)
            }
            if (commitStatus == CommitStatus.INTERMEDIATE_COMMIT) {
                sleep(1.millis)
            }
            serviceHub.withEntityManager {
                persist(anotherEntityWithIdOne)
                try {
                    flush()
                } catch (e: PersistenceException) {
                    logger.info("Caught the exception!")
                }
            }
            if (commitStatus == CommitStatus.INTERMEDIATE_COMMIT) {
                sleep(1.millis)
            }
            serviceHub.withEntityManager {
                persist(entityWithIdTwo)
                persist(entityWithIdThree)
            }
            sleep(1.millis)
        }
    }

    @StartableByRPC
    class EntityManagerCatchErrorAndSaveMoreEntitiesInTheSameEntityManager(private val commitStatus: CommitStatus) : FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            serviceHub.withEntityManager {
                persist(entityWithIdOne)
            }
            if (commitStatus == CommitStatus.INTERMEDIATE_COMMIT) {
                sleep(1.millis)
            }
            serviceHub.withEntityManager {
                persist(anotherEntityWithIdOne)
                try {
                    flush()
                } catch (e: PersistenceException) {
                    logger.info("Caught the exception!")
                }
                // These entities are not saved since the transaction is marked for rollback
                try {
                    persist(entityWithIdTwo)
                    persist(entityWithIdThree)
                } catch (e: PersistenceException) {
                    if (e.cause is ConstraintViolationException) {
                        throw e
                    } else {
                        logger.info(
                            """
                            Caught exception from second set of persists inside the same broken entity manager
                            This happens if the database has thrown an exception due to rolling back the db transaction
                        """.trimIndent(), e
                        )
                    }
                }
            }
            sleep(1.millis)
        }
    }

    @StartableByRPC
    class EntityManagerCatchErrorOutsideTheEntityManagerAndSaveMoreEntitiesInANewEntityManager(private val commitStatus: CommitStatus) :
        FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            serviceHub.withEntityManager {
                persist(entityWithIdOne)
            }
            if (commitStatus == CommitStatus.INTERMEDIATE_COMMIT) {
                sleep(1.millis)
            }
            try {
                serviceHub.withEntityManager {
                    persist(anotherEntityWithIdOne)
                }
            } catch (e: PersistenceException) {
                logger.info("Caught the exception!")
            }
            if (commitStatus == CommitStatus.INTERMEDIATE_COMMIT) {
                sleep(1.millis)
            }
            serviceHub.withEntityManager {
                persist(entityWithIdTwo)
                persist(entityWithIdThree)
            }
            sleep(1.millis)
        }
    }

    @StartableByRPC
    class EntityManagerWithFlushCatchAndInteractWithOtherPartyFlow(private val party: Party) : FlowLogic<SecureHash>() {

        @Suspendable
        override fun call(): SecureHash {
            serviceHub.withEntityManager {
                persist(entityWithIdOne)
            }
            serviceHub.withEntityManager {
                persist(anotherEntityWithIdOne)
                try {
                    flush()
                } catch (e: PersistenceException) {
                    logger.info("Caught the exception!")
                }
            }
            return subFlow(CreateATransactionFlow(party))
        }
    }

    @InitiatingFlow
    class CreateATransactionFlow(val party: Party) : FlowLogic<SecureHash>() {
        @Suspendable
        override fun call(): SecureHash {
            val session = initiateFlow(party)
            val tx = TransactionBuilder(serviceHub.networkMapCache.notaryIdentities.first()).apply {
                addOutputState(DummyState(participants = listOf(ourIdentity, party)))
                addCommand(DummyCommandData, ourIdentity.owningKey, party.owningKey)
            }
            val stx = serviceHub.signInitialTransaction(tx)
            val ftx = subFlow(CollectSignaturesFlow(stx, listOf(session)))
            return subFlow(FinalityFlow(ftx, session)).id
        }
    }

    @InitiatedBy(CreateATransactionFlow::class)
    class CreateATransactionResponder(val session: FlowSession) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            val stx = subFlow(object : SignTransactionFlow(session) {
                override fun checkTransaction(stx: SignedTransaction) {
                }
            })
            subFlow(ReceiveFinalityFlow(session, stx.id))
        }
    }

    @StartableByRPC
    class EntityManagerWithinTheSameDatabaseTransactionFlow : FlowLogic<List<CustomTableEntity>>() {

        @Suspendable
        override fun call(): List<CustomTableEntity> {
            val tx = TransactionBuilder(serviceHub.networkMapCache.notaryIdentities.first()).apply {
                addOutputState(DummyState(participants = listOf(ourIdentity)))
                addCommand(DummyCommandData, ourIdentity.owningKey)
            }
            val stx = serviceHub.signInitialTransaction(tx)
            serviceHub.recordTransactions(stx)
            return serviceHub.withEntityManager {
                val criteria = criteriaBuilder.createQuery(CustomTableEntity::class.java)
                criteria.select(criteria.from(CustomTableEntity::class.java))
                createQuery(criteria).resultList
            }
        }
    }

    @StartableByRPC
    class EntityManagerSaveAndThrowNonDatabaseErrorFlow : FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            try {
                serviceHub.withEntityManager {
                    persist(entityWithIdOne)
                    persist(entityWithIdTwo)
                    persist(entityWithIdThree)
                    throw RuntimeException("die")
                }
            } catch (e: RuntimeException) {
                logger.info("Caught error")
            }
            sleep(1.millis)
        }
    }

    @StartableByRPC
    class EntityManagerSaveFlushAndThrowNonDatabaseErrorFlow : FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            try {
                serviceHub.withEntityManager {
                    persist(entityWithIdOne)
                    persist(entityWithIdTwo)
                    persist(entityWithIdThree)
                    flush()
                    throw RuntimeException("die")
                }
            } catch (e: RuntimeException) {
                logger.info("Caught error")
            }
            sleep(1.millis)
        }
    }

    @StartableByRPC
    class EntityManagerCatchDatabaseErrorInsideEntityManagerThrowNonDatabaseErrorAndCatchOutsideFlow(private val commitStatus: CommitStatus) :
        FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            serviceHub.withEntityManager {
                persist(entityWithIdOne)
            }
            if (commitStatus == CommitStatus.INTERMEDIATE_COMMIT) {
                sleep(1.millis)
            }
            try {
                serviceHub.withEntityManager {
                    persist(anotherEntityWithIdOne)
                    try {
                        flush()
                    } catch (e: PersistenceException) {
                        logger.info("Caught the exception!")
                    }
                    throw RuntimeException("die")
                }
            } catch (e: RuntimeException) {
                logger.info("Caught error")
            }
            sleep(1.millis)
        }
    }

    @CordaService
    class MyService(private val services: AppServiceHub) : SingletonSerializeAsToken() {

        companion object {
            var includeRawUpdates = false
            var insertionType = InsertionType.ENTITY_MANAGER
        }

        enum class InsertionType { ENTITY_MANAGER, CONNECTION }

        val executors: ExecutorService = Executors.newFixedThreadPool(1)

        init {
            if (includeRawUpdates) {
                services.register {
                    services.vaultService.rawUpdates.subscribe {
                        if (insertionType == InsertionType.ENTITY_MANAGER) {
                            services.withEntityManager {
                                persist(entityWithIdOne)
                                persist(entityWithIdTwo)
                                persist(entityWithIdThree)
                            }
                        } else {
                            services.jdbcSession().run {
                                insert(entityWithIdOne)
                                insert(entityWithIdTwo)
                                insert(entityWithIdThree)
                            }
                        }
                    }
                }
            }
        }

        private fun Connection.insert(entity: CustomTableEntity) {
            prepareStatement("INSERT INTO $TABLE_NAME VALUES (?, ?, ?)").apply {
                setInt(1, entity.id)
                setString(2, entity.name)
                setString(3, entity.quote)
            }.executeUpdate()
        }

        fun getEntities(): List<CustomTableEntity> {
            return executors.submit<List<CustomTableEntity>> {
                services.database.transaction {
                    session.run {
                        val criteria = criteriaBuilder.createQuery(CustomTableEntity::class.java)
                        criteria.select(criteria.from(CustomTableEntity::class.java))
                        createQuery(criteria).resultList
                    }
                }
            }.get()
        }
    }

    private fun resetNode(node: NodeHandle) {
        node.rpc.startFlow(::EntityManagerResetFlow).returnValue.getOrThrow()
    }
}