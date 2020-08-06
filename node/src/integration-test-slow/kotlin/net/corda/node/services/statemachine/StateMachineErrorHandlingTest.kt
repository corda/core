package net.corda.node.services.statemachine

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.FlowSession
import net.corda.core.flows.HospitalizeFlowException
import net.corda.core.flows.InitiatedBy
import net.corda.core.flows.InitiatingFlow
import net.corda.core.flows.StartableByRPC
import net.corda.core.flows.StateMachineRunId
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.list
import net.corda.core.internal.readAllLines
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.startFlow
import net.corda.core.node.AppServiceHub
import net.corda.core.node.services.CordaService
import net.corda.core.serialization.CordaSerializable
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.seconds
import net.corda.core.utilities.unwrap
import net.corda.node.services.Permissions
import net.corda.testing.core.DUMMY_NOTARY_NAME
import net.corda.testing.driver.DriverDSL
import net.corda.testing.driver.DriverParameters
import net.corda.testing.driver.NodeHandle
import net.corda.testing.driver.NodeParameters
import net.corda.testing.driver.driver
import net.corda.testing.driver.internal.OutOfProcessImpl
import net.corda.testing.node.NotarySpec
import net.corda.testing.node.TestCordapp
import net.corda.testing.node.User
import net.corda.testing.node.internal.InternalDriverDSL
import org.jboss.byteman.agent.submit.ScriptText
import org.jboss.byteman.agent.submit.Submit
import org.junit.Before
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals

abstract class StateMachineErrorHandlingTest {

    val rpcUser = User("user1", "test", permissions = setOf(Permissions.all()))
    var counter = 0

    @Before
    fun setup() {
        counter = 0
    }

    internal fun startDriver(notarySpec: NotarySpec = NotarySpec(DUMMY_NOTARY_NAME), dsl: DriverDSL.() -> Unit) {
        driver(
            DriverParameters(
                notarySpecs = listOf(notarySpec),
                startNodesInProcess = false,
                inMemoryDB = false,
                systemProperties = mapOf("co.paralleluniverse.fibers.verifyInstrumentation" to "true")
            )
        ) {
            dsl()
        }
    }

    internal fun DriverDSL.createBytemanNode(nodeProvidedName: CordaX500Name): Pair<NodeHandle, Int> {
        val port = nextPort()
        val bytemanNodeHandle = (this as InternalDriverDSL).startNode(
            NodeParameters(
                providedName = nodeProvidedName,
                rpcUsers = listOf(rpcUser)
            ),
            bytemanPort = port
        )
        return bytemanNodeHandle.getOrThrow() to port
    }

    internal fun DriverDSL.createNode(nodeProvidedName: CordaX500Name): NodeHandle {
        return (this as InternalDriverDSL).startNode(
            NodeParameters(
                providedName = nodeProvidedName,
                rpcUsers = listOf(rpcUser)
            )
        ).getOrThrow()
    }

    internal fun DriverDSL.createNodeAndBytemanNode(
        nodeProvidedName: CordaX500Name,
        bytemanNodeProvidedName: CordaX500Name,
        additionalCordapps: Collection<TestCordapp> = emptyList()
    ): Triple<NodeHandle, NodeHandle, Int> {
        val port = nextPort()
        val nodeHandle = (this as InternalDriverDSL).startNode(
            NodeParameters(
                providedName = nodeProvidedName,
                rpcUsers = listOf(rpcUser),
                additionalCordapps = additionalCordapps
            )
        )
        val bytemanNodeHandle = startNode(
            NodeParameters(
                providedName = bytemanNodeProvidedName,
                rpcUsers = listOf(rpcUser),
                additionalCordapps = additionalCordapps
            ),
            bytemanPort = port
        )
        return Triple(nodeHandle.getOrThrow(), bytemanNodeHandle.getOrThrow(), port)
    }

    internal fun submitBytemanRules(rules: String, port: Int) {
        val submit = Submit("localhost", port)
        submit.addScripts(listOf(ScriptText("Test script", rules)))
    }

    internal fun getBytemanOutput(nodeHandle: NodeHandle): List<String> {
        return nodeHandle.baseDirectory
            .list()
            .first { it.toString().contains("net.corda.node.Corda") && it.toString().contains("stdout.log") }
            .readAllLines()
    }

    internal fun OutOfProcessImpl.stop(timeout: Duration): Boolean {
        return process.run {
            destroy()
            waitFor(timeout.seconds, TimeUnit.SECONDS)
        }.also { onStopCallback() }
    }

    @Suppress("LongParameterList")
    internal fun CordaRPCOps.assertHospitalCounts(
        discharged: Int = 0,
        observation: Int = 0,
        propagated: Int = 0,
        networkMapRefresh: Int = 0,
        dischargedRetry: Int = 0,
        observationRetry: Int = 0,
        propagatedRetry: Int = 0,
        networkMapRefreshRetry: Int = 0
    ) {
        val counts = startFlow(StateMachineErrorHandlingTest::GetHospitalCountersFlow).returnValue.getOrThrow(20.seconds)
        assertEquals(discharged, counts.discharged)
        assertEquals(observation, counts.observation)
        assertEquals(propagated, counts.propagated)
        assertEquals(networkMapRefresh, counts.networkMapRefresh)
        assertEquals(dischargedRetry, counts.dischargeRetry)
        assertEquals(observationRetry, counts.observationRetry)
        assertEquals(propagatedRetry, counts.propagatedRetry)
        assertEquals(networkMapRefreshRetry, counts.networkMapRefreshRetry)
    }

    internal fun CordaRPCOps.assertHospitalCountsAllZero() = assertHospitalCounts()

    internal fun CordaRPCOps.assertNumberOfCheckpoints(
        runnable: Int = 0,
        failed: Int = 0,
        completed: Int = 0,
        hospitalized: Int = 0
    ) {
        val counts = startFlow(StateMachineErrorHandlingTest::GetNumberOfCheckpointsFlow).returnValue.getOrThrow(20.seconds)
        assertEquals(runnable, counts.runnable, "There should be $runnable runnable checkpoints")
        assertEquals(failed, counts.failed, "There should be $failed failed checkpoints")
        assertEquals(completed, counts.completed, "There should be $completed completed checkpoints")
        assertEquals(hospitalized, counts.hospitalized, "There should be $hospitalized hospitalized checkpoints")
    }

    internal fun CordaRPCOps.assertNumberOfCheckpointsAllZero() = assertNumberOfCheckpoints()

    @StartableByRPC
    @InitiatingFlow
    class SendAMessageFlow(private val party: Party) : FlowLogic<String>() {
        @Suspendable
        override fun call(): String {
            val session = initiateFlow(party)
            session.send("hello there")
            logger.info("Finished my flow")
            return "Finished executing test flow - ${this.runId}"
        }
    }

    @InitiatedBy(SendAMessageFlow::class)
    class SendAMessageResponder(private val session: FlowSession) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            session.receive<String>().unwrap { it }
            logger.info("Finished my flow")
        }
    }

    @StartableByRPC
    class ThrowAnErrorFlow : FlowLogic<String>() {
        @Suspendable
        override fun call(): String {
            throwException()
            return "cant get here"
        }

        private fun throwException() {
            logger.info("Throwing exception in flow")
            throw IllegalStateException("throwing exception in flow")
        }
    }

    @StartableByRPC
    class ThrowAHospitalizeErrorFlow : FlowLogic<String>() {
        @Suspendable
        override fun call(): String {
            throwException()
            return "cant get here"
        }

        private fun throwException() {
            logger.info("Throwing exception in flow")
            throw HospitalizeFlowException("throwing exception in flow")
        }
    }

    @StartableByRPC
    class GetNumberOfCheckpointsFlow : FlowLogic<NumberOfCheckpoints>() {
        override fun call() = NumberOfCheckpoints(
            runnable = getNumberOfCheckpointsWithStatus(Checkpoint.FlowStatus.RUNNABLE),
            failed = getNumberOfCheckpointsWithStatus(Checkpoint.FlowStatus.FAILED),
            completed = getNumberOfCheckpointsWithStatus(Checkpoint.FlowStatus.COMPLETED),
            hospitalized = getNumberOfCheckpointsWithStatus(Checkpoint.FlowStatus.HOSPITALIZED)
        )

        private fun getNumberOfCheckpointsWithStatus(status: Checkpoint.FlowStatus): Int {
            return serviceHub.jdbcSession()
                .prepareStatement("select count(*) from node_checkpoints where status = ? and flow_id != ?")
                .apply {
                    setInt(1, status.ordinal)
                    setString(2, runId.uuid.toString())
                }
                .use { ps ->
                    ps.executeQuery().use { rs ->
                        rs.next()
                        rs.getLong(1)
                    }
                }.toInt()
        }
    }

    @CordaSerializable
    data class NumberOfCheckpoints(
        val runnable: Int = 0,
        val failed: Int = 0,
        val completed: Int = 0,
        val hospitalized: Int = 0
    )

    // Internal use for testing only!!
    @StartableByRPC
    class GetHospitalCountersFlow : FlowLogic<HospitalCounts>() {
        override fun call(): HospitalCounts =
            HospitalCounts(
                serviceHub.cordaService(HospitalCounter::class.java).dischargedCounter,
                serviceHub.cordaService(HospitalCounter::class.java).observationCounter,
                serviceHub.cordaService(HospitalCounter::class.java).propagatedCounter,
                serviceHub.cordaService(HospitalCounter::class.java).networkMapRefreshCounter,
                serviceHub.cordaService(HospitalCounter::class.java).dischargeRetryCounter,
                serviceHub.cordaService(HospitalCounter::class.java).observationRetryCounter,
                serviceHub.cordaService(HospitalCounter::class.java).propagatedRetryCounter,
                serviceHub.cordaService(HospitalCounter::class.java).networkMapRefreshRetryCounter
            )
    }

    @CordaSerializable
    data class HospitalCounts(
        val discharged: Int,
        val observation: Int,
        val propagated: Int,
        val networkMapRefresh: Int,
        val dischargeRetry: Int,
        val observationRetry: Int,
        val propagatedRetry: Int,
        val networkMapRefreshRetry: Int
    )

    @Suppress("UNUSED_PARAMETER")
    @CordaService
    class HospitalCounter(services: AppServiceHub) : SingletonSerializeAsToken() {
        var dischargedCounter: Int = 0
        var observationCounter: Int = 0
        var propagatedCounter: Int = 0
        var networkMapRefreshCounter: Int = 0
        var dischargeRetryCounter: Int = 0
        var observationRetryCounter: Int = 0
        var propagatedRetryCounter: Int = 0
        var networkMapRefreshRetryCounter: Int = 0

        init {
            StaffedFlowHospital.onFlowKeptForWaitingForNetworkMapRefresh.add { _, _ ->
                networkMapRefreshCounter++
            }
            StaffedFlowHospital.onFlowDischarged.add { _, _ ->
                dischargedCounter++
            }
            StaffedFlowHospital.onFlowKeptForOvernightObservation.add { _, _ ->
                observationCounter++
            }
            StaffedFlowHospital.onFlowErrorPropagated.add { _, _ ->
                propagatedCounter++
            }
            StaffedFlowHospital.onFlowResuscitated.add { _, _, outcome ->
                when (outcome) {
                    StaffedFlowHospital.Outcome.DISCHARGE -> dischargeRetryCounter++
                    StaffedFlowHospital.Outcome.OVERNIGHT_OBSERVATION -> observationRetryCounter++
                    StaffedFlowHospital.Outcome.UNTREATABLE -> propagatedRetryCounter++
                    StaffedFlowHospital.Outcome.WAITING_FOR_NETWORK_MAP_REFRESH -> networkMapRefreshRetryCounter++
                }
            }
        }
    }

    internal val actionExecutorClassName: String by lazy {
        Class.forName("net.corda.node.services.statemachine.ActionExecutorImpl").name
    }

    internal val stateMachineManagerClassName: String by lazy {
        Class.forName("net.corda.node.services.statemachine.SingleThreadedStateMachineManager").name
    }

    @StartableByRPC
    @InitiatingFlow
    class SendAMessageAndLookIntoHospitalFlow(private val party: Party) : FlowLogic<RefreshData>() {
        @Suspendable
        override fun call(): RefreshData {
            val session = initiateFlow(party)
            session.send("hello there")
            return RefreshData("Finished executing test flow - ${this.runId}",
                    serviceHub.cordaService(Hospital::class.java).dataMap)
        }
    }

    @InitiatedBy(SendAMessageAndLookIntoHospitalFlow::class)
    class SendAMessageResponderFlow(private val session: FlowSession) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            session.receive<String>().unwrap { it }
        }
    }

    @CordaSerializable
    data class RefreshData(val message: String, val data: Map<StateMachineRunId, List<String>>)

    @Suppress("UNUSED_PARAMETER")
    @CordaService
    class Hospital(services: AppServiceHub) : SingletonSerializeAsToken() {
        val dataMap = mutableMapOf<StateMachineRunId, List<String>>()
        init {
            StaffedFlowHospital.onFlowKeptForWaitingForNetworkMapRefresh.add { id, by ->
                dataMap[id] = by
            }
        }
    }
}