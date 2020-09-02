package net.corda.node.services.statemachine

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.FlowSession
import net.corda.core.flows.HospitalizeFlowException
import net.corda.core.flows.InitiatedBy
import net.corda.core.flows.InitiatingFlow
import net.corda.core.flows.StartableByRPC
import net.corda.core.identity.Party
import net.corda.core.internal.concurrent.transpose
import net.corda.core.internal.list
import net.corda.core.internal.readAllLines
import net.corda.core.internal.writeText
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.StateMachineInfo
import net.corda.core.messaging.startFlow
import net.corda.core.node.AppServiceHub
import net.corda.core.node.services.CordaService
import net.corda.core.serialization.CordaSerializable
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.seconds
import net.corda.core.utilities.unwrap
import net.corda.node.services.Permissions
import net.corda.testing.core.ALICE_NAME
import net.corda.testing.core.CHARLIE_NAME
import net.corda.testing.core.DUMMY_NOTARY_NAME
import net.corda.testing.driver.DriverDSL
import net.corda.testing.driver.DriverParameters
import net.corda.testing.driver.NodeHandle
import net.corda.testing.driver.NodeParameters
import net.corda.testing.driver.ReusableDriverDsl
import net.corda.testing.driver.internal.OutOfProcessImpl
import net.corda.testing.driver.reusableDriver
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

    companion object {
        private val bytemanPorts = mutableMapOf<DriverDSL, Int>()
    }

    @Before
    fun setup() {
        counter = 0
    }

    internal fun startDriver(notarySpec: NotarySpec = NotarySpec(DUMMY_NOTARY_NAME), dsl: ReusableDriverDsl.() -> Unit) {
        reusableDriver(
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

    internal fun ReusableDriverDsl.createBytemanNode(): Pair<NodeHandle, Int> {
        val port = bytemanPorts.computeIfAbsent(this) { nextPort() }
        val bytemanNodeHandle = (this as InternalDriverDSL).startNode(
            NodeParameters(
                providedName = CHARLIE_NAME,
                rpcUsers = listOf(rpcUser)
            ),
                bytemanPort = port
        ).getOrThrow()
        recoverBy {
            Submit("localhost", port)
                    .deleteAllRules()
        }
        recoverBy {
            resetCounters(bytemanNodeHandle)
            bytemanNodeHandle.removeBytemanLogs()
        }
        return NodeWithReusableNumberOfStateMachine(bytemanNodeHandle) to port
    }

    internal fun ReusableDriverDsl.createNode(additionalCordapps: Collection<TestCordapp> = emptyList()): NodeHandle {
        val node = startNode(
            NodeParameters(
                providedName = ALICE_NAME,
                rpcUsers = listOf(rpcUser),
                additionalCordapps = additionalCordapps
            )
        ).getOrThrow()
        recoverBy { resetCounters(node) }
        return NodeWithReusableNumberOfStateMachine(node)
    }

    private class NodeWithReusableNumberOfStateMachine(private val node: NodeHandle) : NodeHandle by node {
        private val snapshotsCount = node.rpc.stateMachinesSnapshot().size
        override val rpc: CordaRPCOps
            get() {
                return object: CordaRPCOps by node.rpc {
                    override fun stateMachinesSnapshot(): List<StateMachineInfo> {
                        val currentSnapshots = node.rpc.stateMachinesSnapshot()
                        return currentSnapshots.takeLast(currentSnapshots.size - snapshotsCount)
                    }
                }
            }
    }

    internal fun ReusableDriverDsl.createNodeAndBytemanNode(
            additionalCordapps: Collection<TestCordapp> = emptyList()
    ): Triple<NodeHandle, NodeHandle, Int> {
        val port = bytemanPorts.computeIfAbsent(this) { nextPort() }
        val (alice, charlie) = listOf(
                (this as InternalDriverDSL).startNode(
                        NodeParameters(
                                providedName = ALICE_NAME,
                                rpcUsers = listOf(rpcUser),
                                additionalCordapps = additionalCordapps
                        )),
                startNode(
                        NodeParameters(
                                providedName = CHARLIE_NAME,
                                rpcUsers = listOf(rpcUser),
                                additionalCordapps = additionalCordapps
                        ),
                        bytemanPort = port))
                .transpose()
                .getOrThrow()
                .onEach {  recoverBy { resetCounters(it) } }
                .onEach { recoverBy {
                    it.removeBytemanLogs() } }
                .map { NodeWithReusableNumberOfStateMachine(it) }

        recoverBy {
            Submit("localhost", port)
                    .deleteAllRules()
        }

        return Triple(alice, charlie, port)
    }

    internal fun submitBytemanRules(rules: String, port: Int) {
        val submit = Submit("localhost", port)
        submit.addScripts(listOf(ScriptText("Test script", rules)))
    }

    private fun NodeHandle.getBytemanOutput(): List<String> {
        return baseDirectory.list()
            .filter { "net.corda.node.Corda" in it.toString() && "stdout.log" in it.toString() }
            .flatMap { it.readAllLines() }
    }

    private fun NodeHandle.removeBytemanLogs() {
        baseDirectory.list()
                .filter { "net.corda.node.Corda" in it.toString() }
                .filter { "stdout.log" in it.toString() }
                .forEach { it.writeText("") }
    }

    internal fun OutOfProcessImpl.stop(timeout: Duration): Boolean {
        return process.run {
            destroy()
            waitFor(timeout.seconds, TimeUnit.SECONDS)
        }.also { onStopCallback() }
    }

    internal fun NodeHandle.assertBytemanOutput(string: String, count: Int) {
        assertEquals(count, getBytemanOutput().filter { string in it }.size)
    }

    @Suppress("LongParameterList")
    internal fun CordaRPCOps.assertHospitalCounts(
        discharged: Int = 0,
        observation: Int = 0,
        propagated: Int = 0,
        dischargedRetry: Int = 0,
        observationRetry: Int = 0,
        propagatedRetry: Int = 0
    ) {
        val counts = startFlow(StateMachineErrorHandlingTest::GetHospitalCountersFlow).returnValue.getOrThrow(20.seconds)
        assertEquals(discharged, counts.discharged)
        assertEquals(observation, counts.observation)
        assertEquals(propagated, counts.propagated)
        assertEquals(dischargedRetry, counts.dischargeRetry)
        assertEquals(observationRetry, counts.observationRetry)
        assertEquals(propagatedRetry, counts.propagatedRetry)
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

    private fun resetCounters(node: NodeHandle) {
        node.rpc
                .startFlow(StateMachineErrorHandlingTest::ResetCounters)
                .returnValue
                .getOrThrow(20.seconds)
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
                    }.toInt()
                }
        }
    }

    @StartableByRPC
    class ResetCounters : FlowLogic<Unit>() {
        override fun call() {
            serviceHub.cordaService(HospitalCounter::class.java).reset()
            serviceHub.jdbcSession()
                    .prepareStatement("delete from node_checkpoints where flow_id != ?")
                    .apply {
                        setString(1, runId.uuid.toString())
                    }
                    .use { ps ->
                        ps.executeUpdate()
                    }
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
        @Suspendable
        override fun call(): HospitalCounts =
            HospitalCounts(
                serviceHub.cordaService(HospitalCounter::class.java).dischargedCounter,
                serviceHub.cordaService(HospitalCounter::class.java).observationCounter,
                serviceHub.cordaService(HospitalCounter::class.java).propagatedCounter,
                serviceHub.cordaService(HospitalCounter::class.java).dischargeRetryCounter,
                serviceHub.cordaService(HospitalCounter::class.java).observationRetryCounter,
                serviceHub.cordaService(HospitalCounter::class.java).propagatedRetryCounter
            )
    }

    @CordaSerializable
    data class HospitalCounts(
        val discharged: Int,
        val observation: Int,
        val propagated: Int,
        val dischargeRetry: Int,
        val observationRetry: Int,
        val propagatedRetry: Int
    )

    @Suppress("UNUSED_PARAMETER")
    @CordaService
    class HospitalCounter(services: AppServiceHub) : SingletonSerializeAsToken() {
        var dischargedCounter: Int = 0
        var observationCounter: Int = 0
        var propagatedCounter: Int = 0
        var dischargeRetryCounter: Int = 0
        var observationRetryCounter: Int = 0
        var propagatedRetryCounter: Int = 0

        init {
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
                }
            }
        }

        fun reset() {
            dischargedCounter = 0
            observationCounter = 0
            propagatedCounter = 0
            dischargeRetryCounter = 0
            observationRetryCounter = 0
            propagatedRetryCounter = 0
        }
    }

    internal val actionExecutorClassName: String by lazy {
        Class.forName("net.corda.node.services.statemachine.ActionExecutorImpl").name
    }

    internal val stateMachineManagerClassName: String by lazy {
        Class.forName("net.corda.node.services.statemachine.SingleThreadedStateMachineManager").name
    }
}
