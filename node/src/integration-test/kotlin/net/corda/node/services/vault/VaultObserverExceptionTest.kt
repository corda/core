package net.corda.node.services.vault

import co.paralleluniverse.strands.concurrent.Semaphore
import com.r3.dbfailure.contracts.DbFailureContract
import com.r3.dbfailure.workflows.CreateStateFlow
import com.r3.dbfailure.workflows.CreateStateFlow.errorTargetsToNum
import com.r3.dbfailure.workflows.DbListenerService
import com.r3.dbfailure.workflows.DbListenerService.MakeServiceThrowErrorFlow
import com.r3.dbfailure.workflows.SendStateFlow
import com.r3.transactionfailure.workflows.ErrorHandling
import com.r3.transactionfailure.workflows.ErrorHandling.CheckpointAfterErrorFlow
import net.corda.core.CordaRuntimeException
import net.corda.core.contracts.StateAndRef
import net.corda.core.contracts.UniqueIdentifier
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.StartableByRPC
import net.corda.core.identity.Party
import net.corda.core.internal.concurrent.openFuture
import net.corda.core.internal.concurrent.transpose
import net.corda.core.messaging.startFlow
import net.corda.core.node.services.Vault
import net.corda.core.node.services.vault.QueryCriteria
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.seconds
import net.corda.node.services.Permissions
import net.corda.node.services.statemachine.StaffedFlowHospital
import net.corda.notary.jpa.JPAUniquenessProvider
import net.corda.testing.core.ALICE_NAME
import net.corda.testing.core.BOB_NAME
import net.corda.testing.core.singleIdentity
import net.corda.testing.driver.DriverParameters
import net.corda.testing.driver.NodeHandle
import net.corda.testing.driver.OutOfProcess
import net.corda.testing.driver.driver
import net.corda.testing.internal.StateMachineTest
import net.corda.testing.node.User
import net.corda.testing.node.internal.findCordapp
import org.assertj.core.api.Assertions
import org.junit.After
import org.junit.Assert
import org.junit.Test
import java.lang.IllegalStateException
import java.sql.SQLException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import javax.persistence.PersistenceException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class VaultObserverExceptionTest : StateMachineTest() {
    companion object {

        val log = contextLogger()

        private fun testCordapps() = listOf(
                findCordapp("com.r3.dbfailure.contracts"),
                findCordapp("com.r3.dbfailure.workflows"),
                findCordapp("com.r3.dbfailure.schemas"))
    }

    @After
    fun tearDown() {
        DbListenerService.onError = null
        DbListenerService.safeSubscription = true
        DbListenerService.onNextVisited = {}
        DbListenerService.onErrorVisited = null
        DbListenerService.withCustomSafeSubscriber = false
    }

    /**
     * Causing an SqlException via a syntax error in a vault observer causes the flow to hit the
     * DatabsaseEndocrinologist in the FlowHospital and being kept for overnight observation
     */
    @Test(timeout=300_000)
    fun unhandledSqlExceptionFromVaultObserverGetsHospitalised() {
        val testControlFuture = openFuture<Boolean>().toCompletableFuture()

        StaffedFlowHospital.DatabaseEndocrinologist.customConditions.add {
            when (it) {
                is SQLException -> {
                    testControlFuture.complete(true)
                }
            }
            false
        }

        driver(DriverParameters(
                startNodesInProcess = true,
                cordappsForAllNodes = testCordapps())) {
            val aliceUser = User("user", "foo", setOf(Permissions.all()))
            val aliceNode = startNode(providedName = ALICE_NAME, rpcUsers = listOf(aliceUser)).getOrThrow()
            aliceNode.rpc.startFlow(
                CreateStateFlow::Initiator,
                "Syntax Error in Custom SQL",
                CreateStateFlow.errorTargetsToNum(CreateStateFlow.ErrorTarget.ServiceSqlSyntaxError)
            ).returnValue.then { testControlFuture.complete(false) }
            val foundExpectedException = testControlFuture.getOrThrow(30.seconds)

            Assert.assertTrue(foundExpectedException)
        }
    }

    /**
     * Causing an SqlException via a syntax error in a vault observer causes the flow to hit the
     * DatabsaseEndocrinologist in the FlowHospital and being kept for overnight observation - Unsafe subscribe
     */
    @Test(timeout=300_000)
    fun unhandledSqlExceptionFromVaultObserverGetsHospitalisedUnsafeSubscription() {
        DbListenerService.safeSubscription = false
        val testControlFuture = openFuture<Boolean>().toCompletableFuture()

        StaffedFlowHospital.DatabaseEndocrinologist.customConditions.add {
            when (it) {
                is SQLException -> {
                    testControlFuture.complete(true)
                }
            }
            false
        }

        driver(DriverParameters(
            startNodesInProcess = true,
            cordappsForAllNodes = testCordapps())) {
            val aliceUser = User("user", "foo", setOf(Permissions.all()))
            val aliceNode = startNode(providedName = ALICE_NAME, rpcUsers = listOf(aliceUser)).getOrThrow()
            aliceNode.rpc.startFlow(
                CreateStateFlow::Initiator,
                "Syntax Error in Custom SQL",
                CreateStateFlow.errorTargetsToNum(CreateStateFlow.ErrorTarget.ServiceSqlSyntaxError)
            ).returnValue.then { testControlFuture.complete(false) }
            val foundExpectedException = testControlFuture.getOrThrow(30.seconds)

            Assert.assertTrue(foundExpectedException)
        }
    }

    /**
     * None exception thrown from a vault observer can be suppressible in the flow that triggered the observer
     * because the recording of transaction states failed. The flow will be hospitalized.
     * The exception will bring the rx.Observer down.
     */
    @Test(timeout=300_000)
    fun exceptionFromVaultObserverCannotBeSuppressedInFlow() {
        var observation = 0
        val waitUntilHospitalised = Semaphore(0)
        StaffedFlowHospital.onFlowKeptForOvernightObservation.add { _, _ ->
            ++observation
            waitUntilHospitalised.release()
        }

        driver(DriverParameters(
                startNodesInProcess = true,
                cordappsForAllNodes = testCordapps())) {
            val aliceUser = User("user", "foo", setOf(Permissions.all()))
            val aliceNode = startNode(providedName = ALICE_NAME, rpcUsers = listOf(aliceUser)).getOrThrow()
            aliceNode.rpc.startFlow(CreateStateFlow::Initiator, "Exception", CreateStateFlow.errorTargetsToNum(
                    CreateStateFlow.ErrorTarget.ServiceThrowMotherOfAllExceptions,
                    CreateStateFlow.ErrorTarget.FlowSwallowErrors))
            waitUntilHospitalised.acquire() // wait here until flow gets hospitalised
        }

        Assert.assertEquals(1, observation)
    }

    /**
     * None runtime exception thrown from a vault observer can be suppressible in the flow that triggered the observer
     * because the recording of transaction states failed. The flow will be hospitalized.
     * The exception will bring the rx.Observer down.
     */
    @Test(timeout=300_000)
    fun runtimeExceptionFromVaultObserverCannotBeSuppressedInFlow() {
        var observation = 0
        val waitUntilHospitalised = Semaphore(0)
        StaffedFlowHospital.onFlowKeptForOvernightObservation.add { _, _ ->
            ++observation
            waitUntilHospitalised.release()
        }

        driver(DriverParameters(
                startNodesInProcess = true,
                cordappsForAllNodes = testCordapps())) {
            val aliceUser = User("user", "foo", setOf(Permissions.all()))
            val aliceNode = startNode(providedName = ALICE_NAME, rpcUsers = listOf(aliceUser)).getOrThrow()
            aliceNode.rpc.startFlow(CreateStateFlow::Initiator, "InvalidParameterException", CreateStateFlow.errorTargetsToNum(
                CreateStateFlow.ErrorTarget.ServiceThrowInvalidParameter,
                CreateStateFlow.ErrorTarget.FlowSwallowErrors))
            waitUntilHospitalised.acquire() // wait here until flow gets hospitalised
        }

        Assert.assertEquals(1, observation)
    }

    /**
     * If we have a state causing a persistence exception during record transactions (in NodeVaultService#processAndNotify),
     * the flow will be kept in for observation.
     */
    @Test(timeout=300_000)
    fun persistenceExceptionDuringRecordTransactionsGetsKeptForObservation() {
        var counter = 0
        StaffedFlowHospital.DatabaseEndocrinologist.customConditions.add {
            when (it) {
                is PersistenceException -> {
                    ++counter
                    log.info("Got a PersistentException in the flow hospital count = $counter")
                }
            }
            false
        }
        var observation = 0
        StaffedFlowHospital.onFlowKeptForOvernightObservation.add { _, _ ->
            ++observation
        }

        driver(DriverParameters(
                startNodesInProcess = true,
                cordappsForAllNodes = testCordapps())) {
            val aliceUser = User("user", "foo", setOf(Permissions.all()))
            val aliceNode = startNode(providedName = ALICE_NAME, rpcUsers = listOf(aliceUser)).getOrThrow()
            assertFailsWith<TimeoutException>("PersistenceException") {
                aliceNode.rpc.startFlow(CreateStateFlow::Initiator, "EntityManager", errorTargetsToNum(
                        CreateStateFlow.ErrorTarget.TxInvalidState))
                        .returnValue.getOrThrow(30.seconds)
            }
        }
        Assert.assertTrue("Flow has not been to hospital", counter > 0)
        Assert.assertEquals(1, observation)
    }

    /**
     * If we have a state causing a persistence exception during record transactions (in NodeVaultService#processAndNotify),
     * trying to catch and suppress that exception inside the flow does protect the flow, but the new
     * interceptor will fail the flow anyway. The flow will be kept in for observation.
     */
    @Test(timeout=300_000)
    fun persistenceExceptionDuringRecordTransactionsCannotBeSuppressedInFlow() {
        var counter = 0
        StaffedFlowHospital.DatabaseEndocrinologist.customConditions.add {
            when (it) {
                is PersistenceException -> {
                    ++counter
                    log.info("Got a PersistentException in the flow hospital count = $counter")
                }
            }
            false
        }

        driver(DriverParameters(
                startNodesInProcess = true,
                cordappsForAllNodes = testCordapps())) {
            val aliceUser = User("user", "foo", setOf(Permissions.all()))
            val aliceNode = startNode(providedName = ALICE_NAME, rpcUsers = listOf(aliceUser)).getOrThrow()
            val flowHandle = aliceNode.rpc.startFlow(
                    CreateStateFlow::Initiator, "EntityManager",
                    CreateStateFlow.errorTargetsToNum(
                            CreateStateFlow.ErrorTarget.TxInvalidState,
                            CreateStateFlow.ErrorTarget.FlowSwallowErrors))
            val flowResult = flowHandle.returnValue
            assertFailsWith<TimeoutException>("PersistenceException") { flowResult.getOrThrow(30.seconds) }
            Assert.assertTrue("Flow has not been to hospital", counter > 0)
        }
    }

    /**
     * User code throwing a syntax error in a raw vault observer will break the recordTransaction call,
     * therefore handling it in flow code is no good, and the error will be passed to the flow hospital via the
     * interceptor.
     */
    @Test(timeout=300_000)
    fun syntaxErrorInUserCodeInServiceCannotBeSuppressedInFlow() {
        val testControlFuture = openFuture<Boolean>()
        StaffedFlowHospital.onFlowKeptForOvernightObservation.add { _, _ ->
            log.info("Flow has been kept for overnight observation")
            testControlFuture.set(true)
        }

        driver(DriverParameters(
                startNodesInProcess = true,
                cordappsForAllNodes = testCordapps())) {
            val aliceUser = User("user", "foo", setOf(Permissions.all()))
            val aliceNode = startNode(providedName = ALICE_NAME, rpcUsers = listOf(aliceUser)).getOrThrow()
            val flowHandle = aliceNode.rpc.startFlow(CreateStateFlow::Initiator, "EntityManager", CreateStateFlow.errorTargetsToNum(
                    CreateStateFlow.ErrorTarget.ServiceSqlSyntaxError,
                    CreateStateFlow.ErrorTarget.FlowSwallowErrors))
            val flowResult = flowHandle.returnValue
            flowResult.then {
                log.info("Flow has finished")
                testControlFuture.set(false)
            }
            Assert.assertTrue("Flow has not been kept in hospital", testControlFuture.getOrThrow(30.seconds))
        }
    }

    /**
     * User code throwing a syntax error and catching suppressing that within the observer code is fine
     * and should not have any impact on the rest of the flow
     */
    @Test(timeout=300_000)
    fun syntaxErrorInUserCodeInServiceCanBeSuppressedInService() {
        driver(DriverParameters(
                startNodesInProcess = true,
                cordappsForAllNodes = testCordapps())) {
            val aliceUser = User("user", "foo", setOf(Permissions.all()))
            val aliceNode = startNode(providedName = ALICE_NAME, rpcUsers = listOf(aliceUser)).getOrThrow()
            val flowHandle = aliceNode.rpc.startFlow(CreateStateFlow::Initiator, "EntityManager", CreateStateFlow.errorTargetsToNum(
                    CreateStateFlow.ErrorTarget.ServiceSqlSyntaxError,
                    CreateStateFlow.ErrorTarget.ServiceSwallowErrors))
            val flowResult = flowHandle.returnValue
            flowResult.getOrThrow(30.seconds)
        }
    }

    /**
     * Exceptions thrown from a vault observer ,are now wrapped and rethrown as a HospitalizeFlowException.
     * The flow should get hospitalised and any potential following checkpoint should fail.
     * In case of a SQLException or PersistenceException, this was already "breaking" the database transaction
     * and therefore, the next checkpoint was failing.
     */
    @Test(timeout=300_000)
    fun `attempt to checkpoint, following an error thrown in vault observer which gets supressed in flow, will fail`() {
        var counterBeforeFirstCheckpoint = 0
        var counterAfterFirstCheckpoint = 0
        var counterAfterSecondCheckpoint = 0

        ErrorHandling.hookBeforeFirstCheckpoint = { counterBeforeFirstCheckpoint++ }
        ErrorHandling.hookAfterFirstCheckpoint = { counterAfterFirstCheckpoint++ }
        ErrorHandling.hookAfterSecondCheckpoint = { counterAfterSecondCheckpoint++ }

        val waitUntilHospitalised = Semaphore(0)
        StaffedFlowHospital.onFlowKeptForOvernightObservation.add { _, _ ->
            waitUntilHospitalised.release()
        }

        driver(DriverParameters(
                    inMemoryDB = false,
                    startNodesInProcess = true,
                    isDebug = true,
                    cordappsForAllNodes = listOf(findCordapp("com.r3.dbfailure.contracts"),
                                                 findCordapp("com.r3.dbfailure.workflows"),
                                                 findCordapp("com.r3.transactionfailure.workflows"),
                                                 findCordapp("com.r3.dbfailure.schemas")))) {
            val aliceUser = User("user", "foo", setOf(Permissions.all()))
            val node = startNode(providedName = ALICE_NAME, rpcUsers = listOf(aliceUser)).getOrThrow()

            node.rpc.startFlow(::CheckpointAfterErrorFlow, CreateStateFlow.errorTargetsToNum(
                    CreateStateFlow.ErrorTarget.ServiceThrowMotherOfAllExceptions, // throw not persistence exception
                    CreateStateFlow.ErrorTarget.FlowSwallowErrors
                )
            )
            waitUntilHospitalised.acquire()

            // restart node, see if flow retries from correct checkpoint
            node.stop()
            startNode(providedName = ALICE_NAME, rpcUsers = listOf(aliceUser)).getOrThrow()
            waitUntilHospitalised.acquire()

            // check flow retries from correct checkpoint
            assertTrue(counterBeforeFirstCheckpoint == 1)
            assertTrue(counterAfterFirstCheckpoint == 2)
            assertTrue(counterAfterSecondCheckpoint == 0)
        }
    }

    @Test(timeout=300_000)
    fun `vault observer failing with OnErrorFailedException gets hospitalised`() {
        DbListenerService.onError = {
            log.info("Error in rx.Observer#OnError! - Observer will fail with OnErrorFailedException")
            throw it
        }

        var observation = 0
        val waitUntilHospitalised = Semaphore(0)
        StaffedFlowHospital.onFlowKeptForOvernightObservation.add { _, _ ->
            ++observation
            waitUntilHospitalised.release()
        }

        driver(DriverParameters(
                startNodesInProcess = true,
                cordappsForAllNodes = testCordapps())) {
            val aliceUser = User("user", "foo", setOf(Permissions.all()))
            val aliceNode = startNode(providedName = ALICE_NAME, rpcUsers = listOf(aliceUser)).getOrThrow()
            aliceNode.rpc.startFlow(CreateStateFlow::Initiator, "Exception", CreateStateFlow.errorTargetsToNum(
                CreateStateFlow.ErrorTarget.ServiceThrowInvalidParameter,
                CreateStateFlow.ErrorTarget.FlowSwallowErrors))
            waitUntilHospitalised.acquire() // wait here until flow gets hospitalised
        }

        Assert.assertEquals(1, observation)
    }

    @Test(timeout=300_000)
    fun `out of memory error halts JVM, on node restart flow retries, and succeeds`() {
        driver(DriverParameters(inMemoryDB = false, cordappsForAllNodes = testCordapps())) {
            val aliceUser = User("user", "foo", setOf(Permissions.all()))
            val aliceNode = startNode(providedName = ALICE_NAME, rpcUsers = listOf(aliceUser), startInSameProcess = false).getOrThrow()
            aliceNode.rpc.startFlow(::MakeServiceThrowErrorFlow).returnValue.getOrThrow()
            aliceNode.rpc.startFlow(CreateStateFlow::Initiator, "UnrecoverableError", CreateStateFlow.errorTargetsToNum(
                    CreateStateFlow.ErrorTarget.ServiceThrowUnrecoverableError))

            val terminated = (aliceNode as OutOfProcess).process.waitFor(30, TimeUnit.SECONDS)
            if (terminated) {
                aliceNode.stop()
                // starting node within the same process this time to take advantage of threads sharing same heap space
                val testControlFuture = openFuture<Boolean>().toCompletableFuture()
                CreateStateFlow.Initiator.onExitingCall = {
                    testControlFuture.complete(true)
                }
                startNode(providedName = ALICE_NAME, rpcUsers = listOf(aliceUser), startInSameProcess = true).getOrThrow()
                assert(testControlFuture.getOrThrow(30.seconds))
            } else {
                throw IllegalStateException("Out of process node is still up and running!")
            }
        }
    }

    /**
     * An error is thrown inside of the [VaultService.rawUpdates] observable while recording a transaction inside of the initiating node.
     *
     * This causes the transaction to not be saved on the local node but the notary still records the transaction as spent. The transaction
     * also is not send to the counterparty node since it failed before reaching the send. Therefore no subscriber events occur on the
     * counterparty node.
     *
     * More importantly, the observer listening to the [VaultService.rawUpdates] observable should not unsubscribe.
     *
     * Check onNext is visited the correct number of times.
     *
     * This test causes 2 failures inside of the observer to ensure that the observer is still subscribed.
     */
    @Test(timeout=300_000)
    fun `Throw user error in VaultService rawUpdates during FinalityFlow blows up the flow but does not break the Observer - onNext check`() {
        var observationCounter = 0
        StaffedFlowHospital.onFlowKeptForOvernightObservation.add { _, _ -> ++observationCounter }

        val rawUpdatesCount = ConcurrentHashMap<Party, Int>()
        DbListenerService.onNextVisited = { party ->
            if (rawUpdatesCount.putIfAbsent(party, 1) != null) {
                rawUpdatesCount.computeIfPresent(party) { _, count -> count + 1 }
            }
        }

        val user = User("user", "foo", setOf(Permissions.all()))
        driver(DriverParameters(startNodesInProcess = true,
                cordappsForAllNodes = listOf(
                        findCordapp("com.r3.dbfailure.contracts"),
                        findCordapp("com.r3.dbfailure.workflows"),
                        findCordapp("com.r3.dbfailure.schemas")
                ), inMemoryDB = false)
        ) {
            val (aliceNode, bobNode) = listOf(ALICE_NAME, BOB_NAME)
                    .map { startNode(providedName = it,
                            rpcUsers = listOf(user)) }
                    .transpose()
                    .getOrThrow()
            val notary = defaultNotaryHandle.nodeHandles.getOrThrow().first()

            val startErrorInObservableWhenConsumingState = {

                val stateId = aliceNode.rpc.startFlow(
                    CreateStateFlow::Initiator,
                    "AllGood",
                    errorTargetsToNum(CreateStateFlow.ErrorTarget.ServiceSqlSyntaxErrorOnConsumed)
                ).returnValue.getOrThrow(30.seconds)

                println("Created new state")

                val flowHandle = aliceNode.rpc.startFlow(
                    SendStateFlow::PassErroneousOwnableState, // throws at consumed state -> should end up in hospital -> flow should hang
                    stateId,
                    errorTargetsToNum(CreateStateFlow.ErrorTarget.NoError),
                    bobNode.nodeInfo.legalIdentities.first()
                )

                Assertions.assertThatExceptionOfType(TimeoutException::class.java)
                    .isThrownBy { flowHandle.returnValue.getOrThrow(20.seconds) }

                stateId
            }

            assertEquals(0, notary.getNotarisedTransactionIds().size)

            println("First set of flows")
            val stateId = startErrorInObservableWhenConsumingState()
            assertEquals(0, aliceNode.getStatesById(stateId, Vault.StateStatus.CONSUMED).size)
            assertEquals(0, bobNode.getStatesById(stateId, Vault.StateStatus.UNCONSUMED).size)
            assertEquals(1, notary.getNotarisedTransactionIds().size)
            assertEquals(1, observationCounter)
            assertEquals(2, rawUpdatesCount[aliceNode.nodeInfo.singleIdentity()])
            assertEquals(0, rawUpdatesCount.getOrDefault(bobNode.nodeInfo.singleIdentity(), 0))

            println("Second set of flows")
            val stateId2 = startErrorInObservableWhenConsumingState()
            assertEquals(0, aliceNode.getStatesById(stateId2, Vault.StateStatus.CONSUMED).size)
            assertEquals(0, bobNode.getStatesById(stateId2, Vault.StateStatus.UNCONSUMED).size)
            assertEquals(2, notary.getNotarisedTransactionIds().size)
            assertEquals(2, observationCounter)
            assertEquals(4, rawUpdatesCount[aliceNode.nodeInfo.singleIdentity()])
            assertEquals(0, rawUpdatesCount.getOrDefault(bobNode.nodeInfo.singleIdentity(), 0))
        }
    }

    /**
     * An error is thrown inside of the [VaultService.rawUpdates] observable while recording a transaction inside of the initiating node.
     *
     * This causes the transaction to not be saved on the local node but the notary still records the transaction as spent. The transaction
     * also is not send to the counterparty node since it failed before reaching the send. Therefore no subscriber events occur on the
     * counterparty node.
     *
     * More importantly, the observer listening to the [VaultService.rawUpdates] observable should not unsubscribe.
     *
     * Check onNext and onError are visited the correct number of times.
     *
     * This test causes 2 failures inside of the observer to ensure that the observer is still subscribed.
     */
    @Test(timeout=300_000)
    fun `Throw user error in VaultService rawUpdates during FinalityFlow blows up the flow but does not break the Observer - onNext and onError check`() {
        var observationCounter = 0
        StaffedFlowHospital.onFlowKeptForOvernightObservation.add { _, _ -> ++observationCounter }

        val rawUpdatesCount = ConcurrentHashMap<Party, Int>()
        DbListenerService.onNextVisited = { party ->
            if (rawUpdatesCount.putIfAbsent(party, 1) != null) {
                rawUpdatesCount.computeIfPresent(party) { _, count -> count + 1 }
            }
        }

        DbListenerService.onError = {/*just rethrow - we just want to check that onError gets visited by parties*/ throw it}
        DbListenerService.onErrorVisited = { party ->
            if (rawUpdatesCount.putIfAbsent(party, 1) != null) {
                rawUpdatesCount.computeIfPresent(party) { _, count -> count + 1 }
            }
        }

        val user = User("user", "foo", setOf(Permissions.all()))
        driver(DriverParameters(startNodesInProcess = true,
                cordappsForAllNodes = listOf(
                        findCordapp("com.r3.dbfailure.contracts"),
                        findCordapp("com.r3.dbfailure.workflows"),
                        findCordapp("com.r3.dbfailure.schemas")
                ),
                inMemoryDB = false)
        ) {
            val (aliceNode, bobNode) = listOf(ALICE_NAME, BOB_NAME)
                    .map { startNode(providedName = it,
                            rpcUsers = listOf(user)) }
                    .transpose()
                    .getOrThrow()
            val notary = defaultNotaryHandle.nodeHandles.getOrThrow().first()

            val startErrorInObservableWhenConsumingState = {

                val stateId = aliceNode.rpc.startFlow(
                    CreateStateFlow::Initiator,
                    "AllGood",
                    // should be a hospital exception
                    errorTargetsToNum(CreateStateFlow.ErrorTarget.ServiceSqlSyntaxErrorOnConsumed)
                ).returnValue.getOrThrow(30.seconds)

                val flowHandle = aliceNode.rpc.startFlow(
                    SendStateFlow::PassErroneousOwnableState,
                    stateId,
                    errorTargetsToNum(CreateStateFlow.ErrorTarget.NoError),
                    bobNode.nodeInfo.legalIdentities.first()
                )

                Assertions.assertThatExceptionOfType(TimeoutException::class.java)
                    .isThrownBy { flowHandle.returnValue.getOrThrow(20.seconds) }

                stateId
            }

            assertEquals(0, notary.getNotarisedTransactionIds().size)

            val stateId = startErrorInObservableWhenConsumingState()
            assertEquals(0, aliceNode.getStatesById(stateId, Vault.StateStatus.CONSUMED).size)
            assertEquals(0, bobNode.getStatesById(stateId, Vault.StateStatus.UNCONSUMED).size)
            assertEquals(1, notary.getNotarisedTransactionIds().size)
            assertEquals(1, observationCounter)
            assertEquals(3, rawUpdatesCount[aliceNode.nodeInfo.singleIdentity()])
            assertEquals(0, rawUpdatesCount.getOrDefault(bobNode.nodeInfo.singleIdentity(), 0))

            val stateId2 = startErrorInObservableWhenConsumingState()
            assertEquals(0, aliceNode.getStatesById(stateId2, Vault.StateStatus.CONSUMED).size)
            assertEquals(0, bobNode.getStatesById(stateId2, Vault.StateStatus.UNCONSUMED).size)
            assertEquals(2, notary.getNotarisedTransactionIds().size)
            assertEquals(2, observationCounter)
            assertEquals(6, rawUpdatesCount[aliceNode.nodeInfo.singleIdentity()])
            assertEquals(0, rawUpdatesCount.getOrDefault(bobNode.nodeInfo.singleIdentity(), 0))
        }
    }

    /**
     * An error is thrown inside of the [VaultService.rawUpdates] observable while recording a transaction inside of the counterparty node.
     *
     * This causes the transaction to not be saved on the local node but the notary still records the transaction as spent.
     * Observer events are recorded on both the initiating node and the counterparty node.
     *
     * More importantly, the observer listening to the [VaultService.rawUpdates] observable should not unsubscribe.
     *
     * This test causes 2 failures inside of the observer to ensure that the observer is still subscribed.
     */
    @Test(timeout=300_000)
    fun `Throw user error in VaultService rawUpdates during counterparty FinalityFlow blows up the flow but does not break the Observer`() {
        var observationCounter = 0
        // Semaphore is used to wait until [PassErroneousOwnableStateReceiver] gets hospitalized, only after that moment let testing thread assert 'observationCounter'
        val counterPartyHospitalized = Semaphore(0)
        StaffedFlowHospital.onFlowKeptForOvernightObservation.add { _, _ ->
            ++observationCounter
            counterPartyHospitalized.release()
        }

        val rawUpdatesCount = ConcurrentHashMap<Party, Int>()
        DbListenerService.onNextVisited = { party ->
            if (rawUpdatesCount.putIfAbsent(party, 1) != null) {
                rawUpdatesCount.computeIfPresent(party) { _, count -> count + 1 }
            }
        }

        val user = User("user", "foo", setOf(Permissions.all()))
        driver(DriverParameters(startNodesInProcess = true,
                cordappsForAllNodes = listOf(
                        findCordapp("com.r3.dbfailure.contracts"),
                        findCordapp("com.r3.dbfailure.workflows"),
                        findCordapp("com.r3.dbfailure.schemas")
                ),
                inMemoryDB = false)
        ) {
            val (aliceNode, bobNode) = listOf(ALICE_NAME, BOB_NAME)
                    .map { startNode(providedName = it,
                            rpcUsers = listOf(user)) }
                    .transpose()
                    .getOrThrow()
            val notary = defaultNotaryHandle.nodeHandles.getOrThrow().first()

            val startErrorInObservableWhenCreatingSecondState = {

                val stateId = aliceNode.rpc.startFlow(
                    CreateStateFlow::Initiator,
                    "AllGood",
                    errorTargetsToNum(CreateStateFlow.ErrorTarget.NoError)
                ).returnValue.getOrThrow(30.seconds)

                aliceNode.rpc.startFlow(
                    SendStateFlow::PassErroneousOwnableState,
                    stateId,
                    errorTargetsToNum(CreateStateFlow.ErrorTarget.ServiceSqlSyntaxError),
                    bobNode.nodeInfo.legalIdentities.first()
                ).returnValue.getOrThrow(20.seconds)

                stateId
            }

            assertEquals(0, notary.getNotarisedTransactionIds().size)

            val stateId = startErrorInObservableWhenCreatingSecondState()
            assertEquals(1, aliceNode.getStatesById(stateId, Vault.StateStatus.CONSUMED).size)
            assertEquals(0, bobNode.getStatesById(stateId, Vault.StateStatus.UNCONSUMED).size)
            assertEquals(1, notary.getNotarisedTransactionIds().size)
            counterPartyHospitalized.acquire()
            assertEquals(1, observationCounter)
            assertEquals(2, rawUpdatesCount[aliceNode.nodeInfo.singleIdentity()])
            assertEquals(1, rawUpdatesCount[bobNode.nodeInfo.singleIdentity()])

            val stateId2 = startErrorInObservableWhenCreatingSecondState()
            assertEquals(1, aliceNode.getStatesById(stateId2, Vault.StateStatus.CONSUMED).size)
            assertEquals(2, aliceNode.getAllStates(Vault.StateStatus.CONSUMED).size)
            assertEquals(0, bobNode.getStatesById(stateId2, Vault.StateStatus.UNCONSUMED).size)
            assertEquals(2, notary.getNotarisedTransactionIds().size)
            counterPartyHospitalized.acquire()
            assertEquals(2, observationCounter)
            assertEquals(4, rawUpdatesCount[aliceNode.nodeInfo.singleIdentity()])
            assertEquals(2, rawUpdatesCount[bobNode.nodeInfo.singleIdentity()])
        }
    }

    /**
     * An error is thrown inside of the [VaultService.updates] observable while recording a transaction inside of the initiating node.
     *
     * This causes the transaction to not be saved on the local node but the notary still records the transaction as spent. The transaction
     * also is not send to the counterparty node since it failed before reaching the send. Therefore no subscriber events occur on the
     * counterparty node.
     *
     * More importantly, the observer listening to the [VaultService.updates] observable should not unsubscribe.
     *
     * This test causes 2 failures inside of the [rx.Observer] to ensure that the Observer is still subscribed.
     */
    @Test(timeout=300_000)
    fun `Throw user error in VaultService rawUpdates during FinalityFlow blows up the flow but does not break the Observer`() {
        var observationCounter = 0
        StaffedFlowHospital.onFlowKeptForOvernightObservation.add { _, _ -> ++observationCounter }

        val rawUpdatesCount = ConcurrentHashMap<Party, Int>()
        DbListenerService.onNextVisited = { party ->
            if (rawUpdatesCount.putIfAbsent(party, 1) != null) {
                rawUpdatesCount.computeIfPresent(party) { _, count -> count + 1 }
            }
        }

        val user = User("user", "foo", setOf(Permissions.all()))
        driver(DriverParameters(startNodesInProcess = true,
                cordappsForAllNodes = listOf(
                        findCordapp("com.r3.dbfailure.contracts"),
                        findCordapp("com.r3.dbfailure.workflows"),
                        findCordapp("com.r3.dbfailure.schemas")
                ),
                inMemoryDB = false)
        ) {
            val (aliceNode, bobNode) = listOf(ALICE_NAME, BOB_NAME)
                    .map { startNode(providedName = it,
                            rpcUsers = listOf(user)) }
                    .transpose()
                    .getOrThrow()
            val notary = defaultNotaryHandle.nodeHandles.getOrThrow().first()

            val startErrorInObservableWhenConsumingState = {

                val stateId = aliceNode.rpc.startFlow(
                    CreateStateFlow::Initiator,
                    "AllGood",
                    errorTargetsToNum(CreateStateFlow.ErrorTarget.ServiceSqlSyntaxErrorOnConsumed)
                ).returnValue.getOrThrow(30.seconds)

                val flowHandle = aliceNode.rpc.startFlow(
                    SendStateFlow::PassErroneousOwnableState,
                    stateId,
                    errorTargetsToNum(CreateStateFlow.ErrorTarget.NoError),
                    bobNode.nodeInfo.legalIdentities.first()
                )

                Assertions.assertThatExceptionOfType(TimeoutException::class.java)
                    .isThrownBy { flowHandle.returnValue.getOrThrow(20.seconds) }

                stateId
            }

            assertEquals(0, notary.getNotarisedTransactionIds().size)

            val stateId = startErrorInObservableWhenConsumingState()
            assertEquals(0, aliceNode.getStatesById(stateId, Vault.StateStatus.CONSUMED).size)
            assertEquals(1, aliceNode.getStatesById(stateId, Vault.StateStatus.UNCONSUMED).size)
            assertEquals(0, bobNode.getStatesById(stateId, Vault.StateStatus.UNCONSUMED).size)
            assertEquals(1, notary.getNotarisedTransactionIds().size)
            assertEquals(1, observationCounter)
            assertEquals(2, rawUpdatesCount[aliceNode.nodeInfo.singleIdentity()])
            assertEquals(0, rawUpdatesCount.getOrDefault(bobNode.nodeInfo.singleIdentity(), 0))

            val stateId2 = startErrorInObservableWhenConsumingState()
            assertEquals(0, aliceNode.getStatesById(stateId2, Vault.StateStatus.CONSUMED).size)
            assertEquals(2, aliceNode.getAllStates(Vault.StateStatus.UNCONSUMED).size)
            assertEquals(0, bobNode.getStatesById(stateId2, Vault.StateStatus.UNCONSUMED).size)
            assertEquals(2, notary.getNotarisedTransactionIds().size)
            assertEquals(4, rawUpdatesCount[aliceNode.nodeInfo.singleIdentity()])
            assertEquals(0, rawUpdatesCount.getOrDefault(bobNode.nodeInfo.singleIdentity(), 0))
        }
    }

    @Test(timeout=300_000)
    fun `Accessing NodeVaultService rawUpdates from a flow is not allowed` () {
        val user = User("user", "foo", setOf(Permissions.all()))
        driver(DriverParameters(startNodesInProcess = true,
                cordappsForAllNodes = listOf(
                        findCordapp("com.r3.dbfailure.contracts"),
                        findCordapp("com.r3.dbfailure.workflows"),
                        findCordapp("com.r3.dbfailure.schemas")
                ),
                inMemoryDB = false)
        ) {
            val aliceNode = startNode(providedName = ALICE_NAME, rpcUsers = listOf(user)).getOrThrow()

            val flowHandle = aliceNode.rpc.startFlow(::SubscribingRawUpdatesFlow)

            assertFailsWith<CordaRuntimeException>(
                "Flow ${SubscribingRawUpdatesFlow::class.java.name} tried to access VaultService.rawUpdates " +
                        "- Rx.Observables should only be accessed outside the context of a flow "
            ) {
                flowHandle.returnValue.getOrThrow(30.seconds)
            }
        }
    }

    @Test(timeout=300_000)
    fun `Failing Observer wrapped with ResilientSubscriber will survive and be re-called upon flow retry`() {
        var onNextCount = 0
        var onErrorCount = 0
        DbListenerService.onNextVisited = { _ -> onNextCount++ }
        DbListenerService.onError = {/*just rethrow - we just want to check that onError gets visited by parties*/ throw it}
        DbListenerService.onErrorVisited = { _ -> onErrorCount++ }

        val user = User("user", "foo", setOf(Permissions.all()))
        driver(DriverParameters(startNodesInProcess = true,
                cordappsForAllNodes = listOf(
                        findCordapp("com.r3.dbfailure.contracts"),
                        findCordapp("com.r3.dbfailure.workflows"),
                        findCordapp("com.r3.transactionfailure.workflows"),
                        findCordapp("com.r3.dbfailure.schemas")),
                inMemoryDB = false)
        ) {
            val aliceNode = startNode(providedName = ALICE_NAME, rpcUsers = listOf(user)).getOrThrow()

            assertFailsWith<TimeoutException> {
                aliceNode.rpc.startFlow(
                    ErrorHandling::CheckpointAfterErrorFlow,
                    CreateStateFlow.errorTargetsToNum(
                        CreateStateFlow.ErrorTarget.ServiceConstraintViolationException,
                        CreateStateFlow.ErrorTarget.FlowSwallowErrors
                    )
                ).returnValue.getOrThrow(20.seconds)
            }

            assertEquals(4, onNextCount)
            assertEquals(4, onErrorCount)
        }
    }

    @Test(timeout=300_000)
    fun `Users may subscribe to NodeVaultService rawUpdates with their own custom SafeSubscribers`() {
        var onNextCount = 0
        DbListenerService.onNextVisited = { _ -> onNextCount++ }

        val user = User("user", "foo", setOf(Permissions.all()))
        driver(DriverParameters(startNodesInProcess = true,
                cordappsForAllNodes = listOf(
                        findCordapp("com.r3.dbfailure.contracts"),
                        findCordapp("com.r3.dbfailure.workflows"),
                        findCordapp("com.r3.transactionfailure.workflows"),
                        findCordapp("com.r3.dbfailure.schemas")),
                inMemoryDB = false)
        ) {
            // Subscribing with custom SafeSubscriber; the custom SafeSubscriber will not get replaced by a ResilientSubscriber
            // meaning that it will behave as a SafeSubscriber; it will get unsubscribed upon throwing an error.
            // Because we throw a ConstraintViolationException, the Rx Observer will get unsubscribed but the flow will retry
            // from previous checkpoint, however the Observer will no longer be there.
            DbListenerService.withCustomSafeSubscriber = true
            val aliceNode = startNode(providedName = ALICE_NAME, rpcUsers = listOf(user)).getOrThrow()

            aliceNode.rpc.startFlow(
                ErrorHandling::CheckpointAfterErrorFlow,
                CreateStateFlow.errorTargetsToNum(
                    CreateStateFlow.ErrorTarget.ServiceConstraintViolationException,
                    CreateStateFlow.ErrorTarget.FlowSwallowErrors
                )
            ).returnValue.getOrThrow(20.seconds)

            assertEquals(1, onNextCount)
        }
    }

    private fun NodeHandle.getNotarisedTransactionIds(): List<String> {

        @StartableByRPC
        class NotarisedTxs : FlowLogic<List<String>>() {
            override fun call(): List<String> {
                return serviceHub.withEntityManager {
                    val criteriaQuery = this.criteriaBuilder.createQuery(String::class.java)
                    val root = criteriaQuery.from(JPAUniquenessProvider.CommittedTransaction::class.java)
                    criteriaQuery.select(root.get(JPAUniquenessProvider.CommittedTransaction::transactionId.name))
                    val query = this.createQuery(criteriaQuery)
                    query.resultList
                }
            }
        }

        return rpc.startFlowDynamic(NotarisedTxs::class.java).returnValue.getOrThrow()
    }

    private fun NodeHandle.getStatesById(id: UniqueIdentifier?, status: Vault.StateStatus): List<StateAndRef<DbFailureContract.TestState>> {
        return rpc.vaultQueryByCriteria(
            QueryCriteria.LinearStateQueryCriteria(
                linearId = if (id != null) listOf(id) else null,
                status = status
            ), DbFailureContract.TestState::class.java
        ).states
    }

    private fun NodeHandle.getAllStates(status: Vault.StateStatus): List<StateAndRef<DbFailureContract.TestState>> {
        return getStatesById(null, status)
    }

    @StartableByRPC
    class SubscribingRawUpdatesFlow: FlowLogic<Unit>() {
        override fun call() {
            logger.info("Accessing rawUpdates within a flow will throw! ")
            val rawUpdates = serviceHub.vaultService.rawUpdates // throws
            logger.info("Code flow should never reach this logging or the following segment! ")
            rawUpdates.subscribe {
                println("Code flow should never get in here!")
            }
        }
    }
}