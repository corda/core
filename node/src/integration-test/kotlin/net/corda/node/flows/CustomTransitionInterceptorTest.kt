package net.corda.node.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.CordaRuntimeException
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.FlowSession
import net.corda.core.flows.InitiatedBy
import net.corda.core.flows.InitiatingFlow
import net.corda.core.flows.StartableByRPC
import net.corda.core.identity.Party
import net.corda.core.internal.concurrent.transpose
import net.corda.core.messaging.startFlow
import net.corda.core.node.AppServiceHub
import net.corda.core.node.services.CordaService
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.getOrThrow
import net.corda.node.services.config.NodeConfiguration
import net.corda.node.services.statemachine.Checkpoint
import net.corda.node.services.statemachine.Event
import net.corda.node.services.statemachine.FlowFiber
import net.corda.node.services.statemachine.StateMachineState
import net.corda.node.services.statemachine.interceptors.CustomTransitionInterceptor
import net.corda.node.services.statemachine.transitions.FlowContinuation
import net.corda.node.services.statemachine.transitions.TransitionResult
import net.corda.testing.core.ALICE_NAME
import net.corda.testing.core.BOB_NAME
import net.corda.testing.core.singleIdentity
import net.corda.testing.driver.DriverParameters
import net.corda.testing.driver.driver
import net.corda.testing.node.internal.enclosedCordapp
import org.junit.Test
import kotlin.reflect.jvm.jvmName
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class CustomTransitionInterceptorTest {

    @Test
    fun `can register custom interceptor`() {
        driver(DriverParameters(startNodesInProcess = true, notarySpecs = emptyList(), cordappsForAllNodes = listOf(enclosedCordapp()))) {

            val (nodeAHandle, nodeBHandle) = listOf(ALICE_NAME, BOB_NAME)
                .map { startNode(providedName = it, customOverrides = mapOf(NodeConfiguration::customTransitionInterceptor.name to MyInterceptor::class.jvmName)) }
                .transpose()
                .getOrThrow()

            nodeAHandle.rpc.startFlow(
                ::MyFlow,
                nodeBHandle.nodeInfo.singleIdentity()
            ).returnValue.getOrThrow()
        }
    }

    @CordaService
    class MyInterceptor(services: AppServiceHub) : SingletonSerializeAsToken(), CustomTransitionInterceptor {

        private companion object {
            val log = contextLogger()
        }

        override fun intercept(
            fiber: FlowFiber,
            previousState: StateMachineState,
            event: Event,
            transition: TransitionResult,
            nextState: StateMachineState,
            continuation: FlowContinuation
        ) {
            log.info("HIT MY INTERCEPTOR")
        }
    }

    @StartableByRPC
    @InitiatingFlow
    class MyFlow(private val party: Party) : FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            val session = initiateFlow(party)
            session.sendAndReceive<String>("respond to me please")
        }
    }

    @InitiatedBy(MyFlow::class)
    class MyResponder(private val session: FlowSession) : FlowLogic<Unit>() {

        @Suspendable
        override fun call() {
            session.receive<String>()
            session.send("here is a reply")
        }
    }
}