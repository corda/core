package net.corda.node.services.statemachine

import co.paralleluniverse.fibers.Suspendable
import co.paralleluniverse.strands.concurrent.Semaphore
import net.corda.core.CordaRuntimeException
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.KilledFlowException
import net.corda.core.internal.FlowIORequest
import net.corda.core.internal.FlowStateMachineHandle
import net.corda.core.internal.concurrent.transpose
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.seconds
import net.corda.node.services.persistence.DBCheckpointStorage
import net.corda.testing.core.ALICE_NAME
import net.corda.testing.internal.StateMachineTest
import net.corda.testing.node.InMemoryMessagingNetwork
import net.corda.testing.node.internal.DUMMY_CONTRACTS_CORDAPP
import net.corda.testing.node.internal.FINANCE_CONTRACTS_CORDAPP
import net.corda.testing.node.internal.InternalMockNetwork
import net.corda.testing.node.internal.InternalMockNodeParameters
import net.corda.testing.node.internal.TestStartedNode
import net.corda.testing.node.internal.startFlow
import net.corda.testing.node.internal.startFlowWithClientId
import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import rx.Observable
import java.sql.SQLTransientConnectionException
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class FlowClientIdTests : StateMachineTest() {

    private lateinit var mockNet: InternalMockNetwork
    private lateinit var aliceNode: TestStartedNode

    @Before
    fun setUpMockNet() {
        mockNet = InternalMockNetwork(
            cordappsForAllNodes = listOf(DUMMY_CONTRACTS_CORDAPP, FINANCE_CONTRACTS_CORDAPP),
            servicePeerAllocationStrategy = InMemoryMessagingNetwork.ServicePeerAllocationStrategy.RoundRobin()
        )

        aliceNode = mockNet.createNode(InternalMockNodeParameters(legalName = ALICE_NAME))
    }

    @After
    fun cleanUp() {
        mockNet.stopNodes()

        ResultFlow.hook = null
        ResultFlow.suspendableHook = null
        UnSerializableResultFlow.firstRun = true
    }

    @Test(timeout = 300_000)
    fun `no new flow starts if the client id provided pre exists`() {
        var counter = 0
        ResultFlow.hook = { counter++ }
        val clientId = UUID.randomUUID().toString()
        aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5)).resultFuture.getOrThrow()
        aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5)).resultFuture.getOrThrow()
        Assert.assertEquals(1, counter)
    }

    @Test(timeout = 300_000)
    fun `flow's result gets persisted if the flow is started with a client id`() {
        val clientId = UUID.randomUUID().toString()
        aliceNode.services.startFlowWithClientId(clientId, ResultFlow(10)).resultFuture.getOrThrow()

        aliceNode.database.transaction {
            assertEquals(1, findRecordsFromDatabase<DBCheckpointStorage.DBFlowResult>().size)
        }
    }

    @Test(timeout = 300_000)
    fun `flow's result is retrievable after flow's lifetime, when flow is started with a client id - different parameters are ignored`() {
        val clientId = UUID.randomUUID().toString()
        val handle0 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
        val clientId0 = handle0.clientId
        val flowId0 = handle0.id
        val result0 = handle0.resultFuture.getOrThrow()

        val handle1 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(10))
        val clientId1 = handle1.clientId
        val flowId1 = handle1.id
        val result1 = handle1.resultFuture.getOrThrow()

        Assert.assertEquals(clientId0, clientId1)
        Assert.assertEquals(flowId0, flowId1)
        Assert.assertEquals(result0, result1)
    }

    @Test(timeout = 300_000)
    fun `if flow's result is not found in the database an IllegalStateException is thrown`() {
        val clientId = UUID.randomUUID().toString()
        val handle0 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
        val flowId0 = handle0.id
        handle0.resultFuture.getOrThrow()

        // manually remove the checkpoint (including DBFlowResult) from the database
        aliceNode.database.transaction {
            aliceNode.internals.checkpointStorage.removeCheckpoint(flowId0)
        }

        assertFailsWith<IllegalStateException> {
            aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
        }
    }

    @Test(timeout = 300_000)
    fun `flow returning null gets retrieved after flow's lifetime when started with client id`() {
        val clientId = UUID.randomUUID().toString()
        aliceNode.services.startFlowWithClientId(clientId, ResultFlow(null)).resultFuture.getOrThrow()

        val flowResult = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(null)).resultFuture.getOrThrow()
        assertNull(flowResult)
    }

    @Test(timeout = 300_000)
    fun `flow returning Unit gets retrieved after flow's lifetime when started with client id`() {
        val clientId = UUID.randomUUID().toString()
        aliceNode.services.startFlowWithClientId(clientId, ResultFlow(Unit)).resultFuture.getOrThrow()

        val flowResult = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(Unit)).resultFuture.getOrThrow()
        assertEquals(Unit, flowResult)
    }

    @Test(timeout = 300_000)
    fun `flow's result is available if reconnect after flow had retried from previous checkpoint, when flow is started with a client id`() {
        var firstRun = true
        ResultFlow.hook = {
            if (firstRun) {
                firstRun = false
                throw SQLTransientConnectionException("connection is not available")
            }
        }

        val clientId = UUID.randomUUID().toString()
        val result0 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5)).resultFuture.getOrThrow()
        val result1 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5)).resultFuture.getOrThrow()
        Assert.assertEquals(result0, result1)
    }

    @Test(timeout = 300_000)
    fun `flow's result is available if reconnect during flow's retrying from previous checkpoint, when flow is started with a client id`() {
        var firstRun = true
        val waitForSecondRequest = Semaphore(0)
        val waitUntilFlowHasRetried = Semaphore(0)
        ResultFlow.suspendableHook = object : FlowLogic<Unit>() {
            @Suspendable
            override fun call() {
                if (firstRun) {
                    firstRun = false
                    throw SQLTransientConnectionException("connection is not available")
                } else {
                    waitUntilFlowHasRetried.release()
                    waitForSecondRequest.acquire()
                }
            }
        }

        var result1 = 0
        val clientId = UUID.randomUUID().toString()
        val handle0 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
        waitUntilFlowHasRetried.acquire()
        val t = thread { result1 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5)).resultFuture.getOrThrow() }

        Thread.sleep(1000)
        waitForSecondRequest.release()
        val result0 = handle0.resultFuture.getOrThrow()
        t.join()
        Assert.assertEquals(result0, result1)
    }

    @Test(timeout = 300_000)
    fun `failing flow's exception is available after flow's lifetime if flow is started with a client id`() {
        var counter = 0
        ResultFlow.hook = {
            counter++
            throw IllegalStateException()
        }
        val clientId = UUID.randomUUID().toString()

        var flowHandle0: FlowStateMachineHandle<Int>? = null
        assertFailsWith<IllegalStateException> {
            flowHandle0 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
            flowHandle0!!.resultFuture.getOrThrow()
        }

        var flowHandle1: FlowStateMachineHandle<Int>? = null
        assertFailsWith<CordaRuntimeException> {
            flowHandle1 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
            flowHandle1!!.resultFuture.getOrThrow()
        }

        // Assert no new flow has started
        assertEquals(flowHandle0!!.id, flowHandle1!!.id)
        assertEquals(1, counter)
    }

    @Test(timeout = 300_000)
    fun `failed flow's exception is available after flow's lifetime on node start if flow was started with a client id`() {
        var counter = 0
        ResultFlow.hook = {
            counter++
            throw IllegalStateException()
        }
        val clientId = UUID.randomUUID().toString()

        var flowHandle0: FlowStateMachineHandle<Int>? = null
        assertFailsWith<IllegalStateException> {
            flowHandle0 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
            flowHandle0!!.resultFuture.getOrThrow()
        }

        aliceNode = mockNet.restartNode(aliceNode)

        var flowHandle1: FlowStateMachineHandle<Int>? = null
        assertFailsWith<CordaRuntimeException> {
            flowHandle1 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
            flowHandle1!!.resultFuture.getOrThrow()
        }

        // Assert no new flow has started
        assertEquals(flowHandle0!!.id, flowHandle1!!.id)
        assertEquals(1, counter)
    }

    @Test(timeout = 300_000)
    fun `killing a flow, removes the flow from the client id mapping`() {
        var counter = 0
        val flowIsRunning = Semaphore(0)
        val waitUntilFlowIsRunning = Semaphore(0)
        ResultFlow.suspendableHook = object : FlowLogic<Unit>() {
            var firstRun = true

            @Suspendable
            override fun call() {
                ++counter
                if (firstRun) {
                    firstRun = false
                    waitUntilFlowIsRunning.release()
                    flowIsRunning.acquire()
                }
            }
        }
        val clientId = UUID.randomUUID().toString()

        var flowHandle0: FlowStateMachineHandle<Int>? = null
        assertFailsWith<KilledFlowException> {
            flowHandle0 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
            waitUntilFlowIsRunning.acquire()
            aliceNode.internals.smm.killFlow(flowHandle0!!.id)
            flowIsRunning.release()
            flowHandle0!!.resultFuture.getOrThrow()
        }

        // a new flow will start since the client id mapping was removed when flow got killed
        val flowHandle1: FlowStateMachineHandle<Int> = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
        flowHandle1.resultFuture.getOrThrow()

        assertNotEquals(flowHandle0!!.id, flowHandle1.id)
        assertEquals(2, counter)
    }

    @Test(timeout = 300_000)
    fun `flow's client id mapping gets removed upon request`() {
        val clientId = UUID.randomUUID().toString()
        var counter = 0
        ResultFlow.hook = { counter++ }
        val flowHandle0 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
        flowHandle0.resultFuture.getOrThrow(20.seconds)
        val removed = aliceNode.smm.removeClientId(clientId)
        // On new request with clientId, after the same clientId was removed, a brand new flow will start with that clientId
        val flowHandle1 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
        flowHandle1.resultFuture.getOrThrow(20.seconds)

        assertTrue(removed)
        Assert.assertNotEquals(flowHandle0.id, flowHandle1.id)
        Assert.assertEquals(flowHandle0.clientId, flowHandle1.clientId)
        Assert.assertEquals(2, counter)
    }

    @Test(timeout = 300_000)
    fun `removing a client id result clears resources properly`() {
        val clientId = UUID.randomUUID().toString()
        aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5)).resultFuture.getOrThrow()
        // assert database status before remove
        aliceNode.services.database.transaction {
            assertEquals(1, findRecordsFromDatabase<DBCheckpointStorage.DBFlowCheckpoint>().size)
            assertEquals(1, findRecordsFromDatabase<DBCheckpointStorage.DBFlowCheckpointBlob>().size)
            assertEquals(1, findRecordsFromDatabase<DBCheckpointStorage.DBFlowResult>().size)
            assertEquals(1, findRecordsFromDatabase<DBCheckpointStorage.DBFlowMetadata>().size)
        }

        aliceNode.smm.removeClientId(clientId)

        // assert database status after remove
        aliceNode.services.database.transaction {
            assertEquals(0, findRecordsFromDatabase<DBCheckpointStorage.DBFlowCheckpoint>().size)
            assertEquals(0, findRecordsFromDatabase<DBCheckpointStorage.DBFlowCheckpointBlob>().size)
            assertEquals(0, findRecordsFromDatabase<DBCheckpointStorage.DBFlowResult>().size)
            assertEquals(0, findRecordsFromDatabase<DBCheckpointStorage.DBFlowMetadata>().size)
        }
    }

    @Test(timeout=300_000)
    fun `removing a client id exception clears resources properly`() {
        val clientId = UUID.randomUUID().toString()
        ResultFlow.hook = { throw IllegalStateException() }
        assertFailsWith<IllegalStateException> {
            aliceNode.services.startFlowWithClientId(clientId, ResultFlow(Unit)).resultFuture.getOrThrow()
        }
        // assert database status before remove
        aliceNode.services.database.transaction {
            assertEquals(1, findRecordsFromDatabase<DBCheckpointStorage.DBFlowCheckpoint>().size)
            assertEquals(1, findRecordsFromDatabase<DBCheckpointStorage.DBFlowCheckpointBlob>().size)
            assertEquals(1, findRecordsFromDatabase<DBCheckpointStorage.DBFlowException>().size)
            assertEquals(1, findRecordsFromDatabase<DBCheckpointStorage.DBFlowMetadata>().size)
        }

        aliceNode.smm.removeClientId(clientId)

        // assert database status after remove
        aliceNode.services.database.transaction {
            assertEquals(0, findRecordsFromDatabase<DBCheckpointStorage.DBFlowCheckpoint>().size)
            assertEquals(0, findRecordsFromDatabase<DBCheckpointStorage.DBFlowCheckpointBlob>().size)
            assertEquals(0, findRecordsFromDatabase<DBCheckpointStorage.DBFlowException>().size)
            assertEquals(0, findRecordsFromDatabase<DBCheckpointStorage.DBFlowMetadata>().size)
        }
    }

    @Test(timeout=300_000)
    fun `flow's client id mapping can only get removed once the flow gets removed`() {
        val clientId = UUID.randomUUID().toString()
        var tries = 0
        val maxTries = 10
        var failedRemovals = 0
        val semaphore = Semaphore(0)
        ResultFlow.suspendableHook = object : FlowLogic<Unit>() {
            @Suspendable
            override fun call() {
                semaphore.acquire()
            }
        }
        val flowHandle0 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))

        var removed = false
        while (!removed) {
            removed = aliceNode.smm.removeClientId(clientId)
            if (!removed) ++failedRemovals
            ++tries
            if (tries >= maxTries) {
                semaphore.release()
                flowHandle0.resultFuture.getOrThrow(20.seconds)
            }
        }

        assertTrue(removed)
        Assert.assertEquals(maxTries, failedRemovals)
    }

    @Test(timeout = 300_000)
    fun `only one flow starts upon concurrent requests with the same client id`() {
        val requests = 2
        val counter = AtomicInteger(0)
        val resultsCounter = AtomicInteger(0)
        ResultFlow.hook = { counter.incrementAndGet() }
        //(aliceNode.smm as SingleThreadedStateMachineManager).concurrentRequests = true

        val clientId = UUID.randomUUID().toString()
        val threads = arrayOfNulls<Thread>(requests)
        for (i in 0 until requests) {
            threads[i] = Thread {
                val result = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5)).resultFuture.getOrThrow()
                resultsCounter.addAndGet(result)
            }
        }

        val beforeCount = AtomicInteger(0)
        SingleThreadedStateMachineManager.beforeClientIDCheck = {
            beforeCount.incrementAndGet()
        }

        val clientIdNotFound = Semaphore(0)
        val waitUntilClientIdNotFound = Semaphore(0)
        SingleThreadedStateMachineManager.onClientIDNotFound = {
            // Only the first request should reach this point
            waitUntilClientIdNotFound.release()
            clientIdNotFound.acquire()
        }

        for (i in 0 until requests) {
            threads[i]!!.start()
        }

        waitUntilClientIdNotFound.acquire()
        for (i in 0 until requests) {
            clientIdNotFound.release()
        }

        for (thread in threads) {
            thread!!.join()
        }
        Assert.assertEquals(1, counter.get())
        Assert.assertEquals(2, beforeCount.get())
        Assert.assertEquals(10, resultsCounter.get())
    }

    @Test(timeout = 300_000)
    fun `on node start -running- flows with client id are hook-able`() {
        val clientId = UUID.randomUUID().toString()
        var firstRun = true
        val flowIsRunning = Semaphore(0)
        val waitUntilFlowIsRunning = Semaphore(0)

        ResultFlow.suspendableHook = object : FlowLogic<Unit>() {
            @Suspendable
            override fun call() {
                waitUntilFlowIsRunning.release()

                if (firstRun) {
                    firstRun = false
                    // high sleeping time doesn't matter because the fiber will get an [Event.SoftShutdown] on node restart, which will wake up the fiber
                    sleep(100.seconds, maySkipCheckpoint = true)
                }

                flowIsRunning.acquire() // make flow wait here to impersonate a running flow
            }
        }

        val flowHandle0 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
        waitUntilFlowIsRunning.acquire()
        val aliceNode = mockNet.restartNode(aliceNode)

        waitUntilFlowIsRunning.acquire()
        // Re-hook a running flow
        val flowHandle1 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
        flowIsRunning.release()

        Assert.assertEquals(flowHandle0.id, flowHandle1.id)
        Assert.assertEquals(clientId, flowHandle1.clientId)
        Assert.assertEquals(5, flowHandle1.resultFuture.getOrThrow(20.seconds))
    }

    @Test(timeout = 300_000)
    fun `on node start -completed- flows with client id are hook-able`() {
        val clientId = UUID.randomUUID().toString()
        var counter = 0
        ResultFlow.hook = {
            counter++
        }

        val flowHandle0 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
        flowHandle0.resultFuture.getOrThrow()
        val aliceNode = mockNet.restartNode(aliceNode)

        // Re-hook a completed flow
        val flowHandle1 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
        val result1 = flowHandle1.resultFuture.getOrThrow(20.seconds)

        Assert.assertEquals(1, counter) // assert flow has run only once
        Assert.assertEquals(flowHandle0.id, flowHandle1.id)
        Assert.assertEquals(clientId, flowHandle1.clientId)
        Assert.assertEquals(5, result1)
    }

    @Test(timeout = 300_000)
    fun `On 'startFlowInternal' throwing, subsequent request with same client id does not get de-duplicated and starts a new flow`() {
        val clientId = UUID.randomUUID().toString()
        var firstRequest = true
        SingleThreadedStateMachineManager.onCallingStartFlowInternal = {
            if (firstRequest) {
                firstRequest = false
                throw IllegalStateException("Yet another one")
            }
        }
        var counter = 0
        ResultFlow.hook = { counter++ }

        assertFailsWith<IllegalStateException> {
            aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
        }

        val flowHandle1 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
        flowHandle1.resultFuture.getOrThrow(20.seconds)

        assertEquals(clientId, flowHandle1.clientId)
        assertEquals(1, counter)
    }

    @Test(timeout = 300_000)
    fun `if flow fails to serialize its result then the result gets converted to an exception result`() {
        val clientId = UUID.randomUUID().toString()
        assertFailsWith<CordaRuntimeException> {
            aliceNode.services.startFlowWithClientId(clientId, ResultFlow<Observable<Unit>>(Observable.empty())).resultFuture.getOrThrow()
        }

        // flow has failed to serialize its result => table 'node_flow_results' should be empty, 'node_flow_exceptions' should get one row instead
        aliceNode.services.database.transaction {
            val checkpointStatus = findRecordsFromDatabase<DBCheckpointStorage.DBFlowCheckpoint>().single().status
            assertEquals(Checkpoint.FlowStatus.FAILED, checkpointStatus)
            assertEquals(0, findRecordsFromDatabase<DBCheckpointStorage.DBFlowResult>().size)
            assertEquals(1, findRecordsFromDatabase<DBCheckpointStorage.DBFlowException>().size)
        }

        assertFailsWith<CordaRuntimeException> {
            aliceNode.services.startFlowWithClientId(clientId, ResultFlow<Observable<Unit>>(Observable.empty())).resultFuture.getOrThrow()
        }
    }

    /**
     * The below test does not follow a valid path. Normally it should error and propagate.
     * However, we want to assert that a flow that fails to serialize its result its retriable.
     */
    @Test(timeout = 300_000)
    fun `flow failing to serialize its result gets retried and succeeds if returning a different result`() {
        val clientId = UUID.randomUUID().toString()
        // before the hospital schedules a [Event.Error] we manually schedule a [Event.RetryFlowFromSafePoint]
        StaffedFlowHospital.onFlowErrorPropagated.add { _, _ ->
            FlowStateMachineImpl.currentStateMachine()!!.scheduleEvent(Event.RetryFlowFromSafePoint)
        }
        val result = aliceNode.services.startFlowWithClientId(clientId, UnSerializableResultFlow()).resultFuture.getOrThrow()
        assertEquals(5, result)
    }

    @Test(timeout = 300_000)
    fun `flow that fails does not retain its checkpoint nor its exception in the database if not started with a client id`() {
        assertFailsWith<IllegalStateException> {
            aliceNode.services.startFlow(ExceptionFlow { IllegalStateException("another exception") }).resultFuture.getOrThrow()
        }

        aliceNode.services.database.transaction {
            assertEquals(0, findRecordsFromDatabase<DBCheckpointStorage.DBFlowCheckpoint>().size)
            assertEquals(0, findRecordsFromDatabase<DBCheckpointStorage.DBFlowCheckpointBlob>().size)
            assertEquals(0, findRecordsFromDatabase<DBCheckpointStorage.DBFlowException>().size)
            assertEquals(0, findRecordsFromDatabase<DBCheckpointStorage.DBFlowMetadata>().size)
        }
    }

    @Test(timeout = 300_000)
    fun `subsequent request to failed flow that cannot find a 'DBFlowException' in the database, fails with 'IllegalStateException'`() {
        ResultFlow.hook = {
            // just throwing a different exception from the one expected out of startFlowWithClientId second call below ([IllegalStateException])
            // to be sure [IllegalStateException] gets thrown from [DBFlowException] that is missing
            throw IllegalArgumentException()
        }
        val clientId = UUID.randomUUID().toString()

        var flowHandle0: FlowStateMachineHandle<Int>? = null
        assertFailsWith<IllegalArgumentException> {
            flowHandle0 = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
            flowHandle0!!.resultFuture.getOrThrow()
        }

        // manually remove [DBFlowException] from the database to impersonate missing [DBFlowException]
        val removed = aliceNode.services.database.transaction {
            aliceNode.internals.checkpointStorage.removeFlowException(flowHandle0!!.id)
        }
        assertTrue(removed)

        val e = assertFailsWith<IllegalStateException> {
            aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5)).resultFuture.getOrThrow()
        }

        assertEquals("Flow's ${flowHandle0!!.id} exception was not found in the database. Something is very wrong.", e.message)
    }

    @Test(timeout=300_000)
    fun `completed flow started with a client id nulls its flow state in database after its lifetime`() {
        val clientId = UUID.randomUUID().toString()
        val flowHandle = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
        flowHandle.resultFuture.getOrThrow()

        aliceNode.services.database.transaction {
            val dbFlowCheckpoint = aliceNode.internals.checkpointStorage.getDBCheckpoint(flowHandle.id)
            assertNull(dbFlowCheckpoint!!.blob!!.flowStack)
        }
    }

    @Test(timeout=300_000)
    fun `failed flow started with a client id nulls its flow state in database after its lifetime`() {
        val clientId = UUID.randomUUID().toString()
        ResultFlow.hook = { throw IllegalStateException() }

        var flowHandle: FlowStateMachineHandle<Int>? = null
        assertFailsWith<IllegalStateException> {
            flowHandle = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(5))
            flowHandle!!.resultFuture.getOrThrow()
        }

        aliceNode.services.database.transaction {
            val dbFlowCheckpoint = aliceNode.internals.checkpointStorage.getDBCheckpoint(flowHandle!!.id)
            assertNull(dbFlowCheckpoint!!.blob!!.flowStack)
        }
    }
    @Test(timeout = 300_000)
    fun `reattachFlowWithClientId can retrieve existing flow future`() {
        val clientId = UUID.randomUUID().toString()
        val flowHandle = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(10))
        val reattachedFlowHandle = aliceNode.smm.reattachFlowWithClientId<Int>(clientId)

        assertEquals(10, flowHandle.resultFuture.getOrThrow(20.seconds))
        assertEquals(clientId, flowHandle.clientId)
        assertEquals(flowHandle.id, reattachedFlowHandle?.id)
        assertEquals(flowHandle.resultFuture.get(), reattachedFlowHandle?.resultFuture?.get())
    }

    @Test(timeout = 300_000)
    fun `reattachFlowWithClientId can retrieve a null result from a flow future`() {
        val clientId = UUID.randomUUID().toString()
        val flowHandle = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(null))
        val reattachedFlowHandle = aliceNode.smm.reattachFlowWithClientId<Int>(clientId)

        assertEquals(null, flowHandle.resultFuture.getOrThrow(20.seconds))
        assertEquals(clientId, flowHandle.clientId)
        assertEquals(flowHandle.id, reattachedFlowHandle?.id)
        assertEquals(flowHandle.resultFuture.get(), reattachedFlowHandle?.resultFuture?.get())
    }

    @Test(timeout = 300_000)
    fun `reattachFlowWithClientId can retrieve result from completed flow`() {
        val clientId = UUID.randomUUID().toString()
        val flowHandle = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(10))

        assertEquals(10, flowHandle.resultFuture.getOrThrow(20.seconds))
        assertEquals(clientId, flowHandle.clientId)

        val reattachedFlowHandle = aliceNode.smm.reattachFlowWithClientId<Int>(clientId)

        assertEquals(flowHandle.id, reattachedFlowHandle?.id)
        assertEquals(flowHandle.resultFuture.get(), reattachedFlowHandle?.resultFuture?.get())
    }

    @Test(timeout = 300_000)
    fun `reattachFlowWithClientId returns null if no flow matches the client id`() {
        assertEquals(null, aliceNode.smm.reattachFlowWithClientId<Int>(UUID.randomUUID().toString()))
    }

    @Test(timeout = 300_000)
    fun `reattachFlowWithClientId can retrieve exception from existing flow future`() {
        ResultFlow.hook = { throw IllegalStateException("Bla bla bla") }
        val clientId = UUID.randomUUID().toString()
        val flowHandle = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(10))
        val reattachedFlowHandle = aliceNode.smm.reattachFlowWithClientId<Int>(clientId)

        assertThatExceptionOfType(IllegalStateException::class.java).isThrownBy {
            flowHandle.resultFuture.getOrThrow(20.seconds)
        }.withMessage("Bla bla bla")

        assertThatExceptionOfType(IllegalStateException::class.java).isThrownBy {
            reattachedFlowHandle?.resultFuture?.getOrThrow()
        }.withMessage("Bla bla bla")
    }

    @Test(timeout = 300_000)
    fun `reattachFlowWithClientId can retrieve exception from completed flow`() {
        ResultFlow.hook = { throw IllegalStateException("Bla bla bla") }
        val clientId = UUID.randomUUID().toString()
        val flowHandle = aliceNode.services.startFlowWithClientId(clientId, ResultFlow(10))

        assertThatExceptionOfType(IllegalStateException::class.java).isThrownBy {
            flowHandle.resultFuture.getOrThrow(20.seconds)
        }.withMessage("Bla bla bla")

        val reattachedFlowHandle = aliceNode.smm.reattachFlowWithClientId<Int>(clientId)

        // [CordaRunTimeException] returned because [IllegalStateException] is not serializable
        assertThatExceptionOfType(CordaRuntimeException::class.java).isThrownBy {
            reattachedFlowHandle?.resultFuture?.getOrThrow()
        }.withMessage("java.lang.IllegalStateException: Bla bla bla")
    }

    @Test(timeout = 300_000)
    fun `finishedFlowsWithClientIds returns completed flows with client ids`() {
        val clientIds = listOf("a", "b", "c", "d", "e")
        val lock = CountDownLatch(1)
        ResultFlow.hook = { clientId ->
            if (clientId == clientIds[3]) {
                throw java.lang.IllegalStateException("This didn't go so well")
            }
            if (clientId == clientIds[4]) {
                lock.await(30, TimeUnit.SECONDS)
            }
        }
        val flows = listOf(
            aliceNode.services.startFlowWithClientId(clientIds[0], ResultFlow(10)),
            aliceNode.services.startFlowWithClientId(clientIds[1], ResultFlow(10)),
            aliceNode.services.startFlowWithClientId(clientIds[2], ResultFlow(10))
        )
        val failedFlow = aliceNode.services.startFlowWithClientId(clientIds[3], ResultFlow(10))
        val runningFlow = aliceNode.services.startFlowWithClientId(clientIds[4], ResultFlow(10))
        flows.map { it.resultFuture }.transpose().getOrThrow(30.seconds)
        assertFailsWith<java.lang.IllegalStateException> { failedFlow.resultFuture.getOrThrow(20.seconds) }

        val finishedFlows = aliceNode.smm.finishedFlowsWithClientIds()

        lock.countDown()

        assertEquals(4, finishedFlows.size)
        assertEquals(3, finishedFlows.filterValues { it }.size)
        assertEquals(1, finishedFlows.filterValues { !it }.size)
        assertEquals(setOf("a", "b", "c", "d"), finishedFlows.map { it.key }.toSet())
        assertTrue(runningFlow.clientId !in finishedFlows.keys)

        assertEquals(
            listOf(10, 10, 10),
            finishedFlows.filterValues { it }.map { aliceNode.smm.reattachFlowWithClientId<Int>(it.key)?.resultFuture?.get() }
        )
        // [CordaRunTimeException] returned because [IllegalStateException] is not serializable
        assertFailsWith<CordaRuntimeException> {
            finishedFlows.filterValues { !it }.map { aliceNode.smm.reattachFlowWithClientId<Int>(it.key)?.resultFuture?.getOrThrow() }
        }
    }
}

internal class ResultFlow<A>(private val result: A): FlowLogic<A>() {
    companion object {
        var hook: ((String?) -> Unit)? = null
        var suspendableHook: FlowLogic<Unit>? = null
    }

    @Suspendable
    override fun call(): A {
        hook?.invoke(stateMachine.clientId)
        suspendableHook?.let { subFlow(it) }
        return result
    }
}

internal class UnSerializableResultFlow: FlowLogic<Any>() {
    companion object {
        var firstRun = true
    }

    @Suspendable
    override fun call(): Any {
        stateMachine.suspend(FlowIORequest.ForceCheckpoint, false)
        return if (firstRun) {
            firstRun = false
            Observable.empty<Any>()
        } else {
            5 // serializable result
        }
    }
}