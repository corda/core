package net.corda.services.messaging

import net.corda.core.concurrent.CordaFuture
import net.corda.core.identity.CordaX500Name
import net.corda.core.internal.concurrent.openFuture
import net.corda.core.messaging.MessageRecipients
import net.corda.core.messaging.SingleMessageRecipient
import net.corda.core.serialization.CordaSerializable
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.seconds
import net.corda.node.services.messaging.MessagingService
import net.corda.node.services.messaging.ReceivedMessage
import net.corda.node.services.messaging.send
import net.corda.node.services.network.toPartyInfo
import net.corda.testing.driver.DriverDSL
import net.corda.testing.driver.DriverParameters
import net.corda.testing.driver.InProcess
import net.corda.testing.driver.driver
import net.corda.testing.driver.internal.internalServices
import net.corda.testing.node.ClusterSpec
import net.corda.testing.node.NotarySpec
import org.assertj.core.api.Assertions.assertThat
import org.junit.Ignore
import org.junit.Test
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

class P2PMessagingTest {
    private companion object {
        val DISTRIBUTED_SERVICE_NAME = CordaX500Name("DistributedService", "London", "GB")
    }

    @Test(timeout=300_000)
@Ignore
    fun `communicating with a distributed service which we're part of`() {
        startDriverWithDistributedService { distributedService ->
            assertAllNodesAreUsed(distributedService, DISTRIBUTED_SERVICE_NAME, distributedService[0])
        }
    }

    private fun startDriverWithDistributedService(dsl: DriverDSL.(List<InProcess>) -> Unit) {
        driver(DriverParameters(
                startNodesInProcess = true,
                notarySpecs = listOf(NotarySpec(DISTRIBUTED_SERVICE_NAME, cluster = ClusterSpec.Raft(clusterSize = 2)))
        )) {
            dsl(defaultNotaryHandle.nodeHandles.getOrThrow().map { (it as InProcess) })
        }
    }

    private fun assertAllNodesAreUsed(participatingServiceNodes: List<InProcess>, serviceName: CordaX500Name, originatingNode: InProcess) {
        // Setup each node in the distributed service to return back it's NodeInfo so that we can know which node is being used
        participatingServiceNodes.forEach { node ->
            node.respondWith(node.services.myInfo)
        }
        val serviceAddress = originatingNode.services.networkMapCache.run {
            originatingNode.internalServices.networkService.getAddressOfParty(getMemberByName(serviceName)!!.toPartyInfo())
        }
        val participatingNodes = HashSet<Any>()
        // Try several times so that we can be fairly sure that any node not participating is not due to Artemis' selection
        // strategy. 3 attempts for each node seems to be sufficient.
        // This is not testing the distribution of the requests - DistributedServiceTests already does that
        for (it in 1..participatingServiceNodes.size * 3) {
            participatingNodes += originatingNode.receiveFrom(serviceAddress).getOrThrow(10.seconds)
            if (participatingNodes.size == participatingServiceNodes.size) {
                break
            }
        }
        assertThat(participatingNodes).containsOnlyElementsOf(participatingServiceNodes.map { it.services.myInfo })
    }

    private fun InProcess.respondWith(message: Any) {
        internalServices.networkService.addMessageHandler("test.request") { netMessage, _, handler ->
            val request = netMessage.data.deserialize<TestRequest>()
            val response = internalServices.networkService.createMessage("test.response", message.serialize().bytes)
            internalServices.networkService.send(response, request.replyTo)
            handler.afterDatabaseTransaction()
        }
    }

    private fun InProcess.receiveFrom(target: MessageRecipients): CordaFuture<Any> {
        val response = openFuture<Any>()
        internalServices.networkService.runOnNextMessage("test.response") { netMessage ->
            response.set(netMessage.data.deserialize())
        }
        internalServices.networkService.send("test.request", TestRequest(replyTo = internalServices.networkService.myAddress), target)
        return response
    }

    /**
     * Registers a handler for the given topic and session that runs the given callback with the message and then removes
     * itself. This is useful for one-shot handlers that aren't supposed to stick around permanently. Note that this callback
     * doesn't take the registration object, unlike the callback to [MessagingService.addMessageHandler].
     *
     * @param topic identifier for the topic and session to listen for messages arriving on.
     */
    inline fun MessagingService.runOnNextMessage(topic: String, crossinline callback: (ReceivedMessage) -> Unit) {
        val consumed = AtomicBoolean()
        addMessageHandler(topic) { msg, reg, handler ->
            removeMessageHandler(reg)
            check(!consumed.getAndSet(true)) { "Called more than once" }
            check(msg.topic == topic) { "Topic/session mismatch: ${msg.topic} vs $topic" }
            callback(msg)
            handler.afterDatabaseTransaction()
        }
    }

    @CordaSerializable
    private data class TestRequest(val replyTo: SingleMessageRecipient)
}
