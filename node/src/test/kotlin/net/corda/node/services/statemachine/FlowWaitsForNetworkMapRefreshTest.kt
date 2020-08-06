package net.corda.node.services.statemachine

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.FlowSession
import net.corda.core.flows.InitiatedBy
import net.corda.core.flows.InitiatingFlow
import net.corda.core.flows.PartyNotFoundException
import net.corda.core.flows.StartableByRPC
import net.corda.core.flows.StateMachineRunId
import net.corda.core.identity.Party
import net.corda.core.messaging.startFlow
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.unwrap
import net.corda.node.services.Permissions
import net.corda.testing.core.ALICE_NAME
import net.corda.testing.core.BOB_NAME
import net.corda.testing.core.BOC_NAME
import net.corda.testing.core.CHARLIE_NAME
import net.corda.testing.core.singleIdentity
import net.corda.testing.driver.DriverParameters
import net.corda.testing.driver.driver
import net.corda.testing.node.User
import org.junit.Before
import org.junit.Test
import org.junit.jupiter.api.BeforeEach
import java.security.PublicKey
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class FlowWaitsForNetworkMapRefreshTest {

    private val flowDiagnosedWithNetworkMapWaiting = mutableListOf<StateMachineRunId>()

    @BeforeEach
    fun beforeEach() {
        flowDiagnosedWithNetworkMapWaiting.clear()
    }

    @Before
    fun start() {
        StaffedFlowHospital.onFlowKeptForWaitingForNetworkMapRefresh.add { id, _ -> flowDiagnosedWithNetworkMapWaiting.add(id) }
        HelloExceptionFlow.counter = 0
    }

    @Test(timeout = 300_000)
    fun `flow started with unknown party gets exception after network map refresh as it does not contain missing party`() {
        driver(DriverParameters(
                notarySpecs = emptyList(),
                startNodesInProcess = true
        )) {
            val user = User(
                    "mark",
                    "dadada",
                    setOf(Permissions.startFlow<HelloFlow>())
            )

            val nodeCHandle = startNode(providedName = ALICE_NAME, rpcUsers = listOf(user)).getOrThrow()

            // Party wasn't added to network map, error propagation should start
            assertFailsWith<StateTransitionException> {
                nodeCHandle.rpc.startFlow(::HelloFlow).returnValue.getOrThrow()
            }
        }

        assertEquals(flowDiagnosedWithNetworkMapWaiting.size, 1)
    }

    @Test(timeout = 300_000)
    fun `flow started with unknown party restarts after network map refresh and successfully finishes execution`() {
        driver(DriverParameters(
                notarySpecs = emptyList(),
                startNodesInProcess = true
        )) {
            val user = User(
                    "mark",
                    "dadada",
                    setOf(Permissions.startFlow<HelloExceptionFlow>())
            )

            val nodeAHandle = startNode(providedName = BOB_NAME, rpcUsers = listOf(user)).getOrThrow()
            val nodeBHandle = startNode(providedName = CHARLIE_NAME, rpcUsers = listOf(user)).getOrThrow()

            // Party "added" after first fail, should retry the flow and succeed
            val result = nodeAHandle.rpc.startFlow(::HelloExceptionFlow, nodeBHandle.nodeInfo.singleIdentity())
                    .returnValue
                    .getOrThrow()

            assertEquals("go away", result)
        }

        assertEquals(flowDiagnosedWithNetworkMapWaiting.size, 1)
    }

    @StartableByRPC
    @InitiatingFlow
    class HelloFlow : FlowLogic<Any>() {
        @Suspendable
        override fun call() {
            val nonExistingParty = Party(BOC_NAME, object : PublicKey {
                override fun getAlgorithm(): String = "TEST-512"
                override fun getFormat(): String = "<none>"
                override fun getEncoded() = byteArrayOf()
            })
            return initiateFlow(nonExistingParty).send("hello there")
        }
    }

    @StartableByRPC
    @InitiatingFlow
    class HelloExceptionFlow(val p: Party) : FlowLogic<String>() {
        companion object {
            @Volatile
            var counter = 0
        }

        @Suspendable
        override fun call(): String {
            if (counter == 0) {
                counter += 1
                val partyNotFoundException = PartyNotFoundException("Could not find party: $CHARLIE_NAME", CHARLIE_NAME)
                throw StateTransitionException(partyNotFoundException)
            }
            val partySession = initiateFlow(p)
            partySession.send("hi there")
            return partySession.receive<String>().unwrap { it }
        }
    }

    @InitiatedBy(HelloExceptionFlow::class)
    open class ReceiveHelloExceptionFlow(val session: FlowSession) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            session.receive<String>().unwrap { it }
            session.send("go away")
        }
    }
}
