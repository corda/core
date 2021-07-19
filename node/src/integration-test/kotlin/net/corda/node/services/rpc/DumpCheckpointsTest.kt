package net.corda.node.services.rpc

import co.paralleluniverse.fibers.Suspendable
import com.fasterxml.jackson.databind.ObjectMapper
import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.containsSubstring
import net.corda.client.rpc.CordaRPCClient
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.StartableByRPC
import net.corda.core.internal.createDirectories
import net.corda.core.internal.div
import net.corda.core.internal.inputStream
import net.corda.core.internal.isRegularFile
import net.corda.core.internal.list
import net.corda.core.internal.readFully
import net.corda.core.messaging.startFlow
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.getOrThrow
import net.corda.node.internal.NodeStartup
import net.corda.node.services.Permissions
import net.corda.node.services.statemachine.CountUpDownLatch
import net.corda.testing.core.ALICE_NAME
import net.corda.testing.driver.DriverParameters
import net.corda.testing.driver.driver
import net.corda.testing.driver.internal.checkpoint.CheckpointRpcHelper.checkpointsRpc
import net.corda.testing.node.User
import net.corda.testing.node.internal.enclosedCordapp
import org.junit.Test
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.zip.ZipInputStream
import kotlin.test.assertEquals

class DumpCheckpointsTest {

    companion object {
        private val dumpCheckPointLatch = CountDownLatch(1)
        private val flowProceedLatch = CountUpDownLatch(1)
    }

    @Test(timeout=300_000)
	fun `verify checkpoint dump via RPC`() {
        val user = User("mark", "dadada", setOf(Permissions.all()))
        driver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true, cordappsForAllNodes = listOf(enclosedCordapp()))) {

            val nodeAHandle = startNode(providedName = ALICE_NAME, rpcUsers = listOf(user)).getOrThrow()

            CordaRPCClient(nodeAHandle.rpcAddress).start(user.username, user.password).use {

                // 1 for GetNumberOfCheckpointsFlow itself
                val checkPointCountFuture = it.proxy.startFlow(::GetNumberOfCheckpointsFlow).returnValue

                val logDirPath = nodeAHandle.baseDirectory / NodeStartup.LOGS_DIRECTORY_NAME
                logDirPath.createDirectories()
                dumpCheckPointLatch.await()
                nodeAHandle.checkpointsRpc.use { checkpointRPCOps -> checkpointRPCOps.dumpCheckpoints() }

                flowProceedLatch.countDown()
                assertEquals(1, checkPointCountFuture.get())
                checkDumpFile(logDirPath)
            }
        }
    }

    @Test(timeout=300_000)
    fun `assert checkpoint dump for WaitForLedgerCommit`() {
        val user = User("mark", "dadada", setOf(Permissions.all()))
        driver(DriverParameters(notarySpecs = emptyList(), startNodesInProcess = true, cordappsForAllNodes = listOf(enclosedCordapp()))) {
            val nodeAHandle = startNode(providedName = ALICE_NAME, rpcUsers = listOf(user)).getOrThrow()

            CordaRPCClient(nodeAHandle.rpcAddress).start(user.username, user.password).use {

                val txHash = SecureHash.randomSHA256()
                it.proxy.startFlow(::WaitForLedgerCommitFlow, txHash)

                Thread.sleep(5000) // wait until the flow gets suspended, waiting for a ledger commit

                val logDirPath = nodeAHandle.baseDirectory / NodeStartup.LOGS_DIRECTORY_NAME
                logDirPath.createDirectories()
                nodeAHandle.checkpointsRpc.use { checkpointRPCOps -> checkpointRPCOps.dumpCheckpoints() }

                val file = logDirPath.list().single { it.isRegularFile() }
                val json = ZipInputStream(file.inputStream()).use { zip ->
                    zip.nextEntry
                    ObjectMapper().readTree(zip)
                }
                assertEquals(txHash.toString(), json["suspendedOn"]["waitForLedgerCommit"].asText())
            }
        }
    }

    private fun checkDumpFile(dir: Path) {
        // The directory supposed to contain a single ZIP file
        val file = dir.list().single { it.isRegularFile() }

        ZipInputStream(file.inputStream()).use { zip ->
            val entry = zip.nextEntry
            assertThat(entry.name, containsSubstring("json"))
            val content = zip.readFully()
            assertThat(String(content), containsSubstring(GetNumberOfCheckpointsFlow::class.java.name))
        }
    }

    @StartableByRPC
    class GetNumberOfCheckpointsFlow : FlowLogic<Int>() {
        @Suspendable
        override fun call(): Int {
            var count = 0
            serviceHub.jdbcSession().prepareStatement("select * from node_checkpoints").use { ps ->
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        count++
                    }
                }
            }
            syncUp()
            return count
        }

        @Suspendable
        private fun syncUp() {
            dumpCheckPointLatch.countDown()
            flowProceedLatch.await()
        }
    }

    @StartableByRPC
    class WaitForLedgerCommitFlow(private val txId: SecureHash) : FlowLogic<SignedTransaction>() {
        @Suspendable
        override fun call(): SignedTransaction {
            return waitForLedgerCommit(txId)
        }
    }
}