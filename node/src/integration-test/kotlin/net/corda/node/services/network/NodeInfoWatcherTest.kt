package net.corda.node.services.network

import com.google.common.jimfs.Configuration
import com.google.common.jimfs.Jimfs
import net.corda.cordform.CordformNode
import net.corda.core.crypto.SignedData
import net.corda.core.internal.createDirectories
import net.corda.core.internal.div
import net.corda.core.node.NodeInfo
import net.corda.core.node.NodeInfoImpl
import net.corda.core.node.services.KeyManagementService
import net.corda.core.serialization.serialize
import net.corda.nodeapi.internal.NodeInfoFilesCopier
import net.corda.testing.*
import net.corda.testing.node.MockKeyManagementService
import net.corda.testing.node.makeTestIdentityService
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.contentOf
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import rx.observers.TestSubscriber
import rx.schedulers.TestScheduler
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class NodeInfoWatcherTest {
    companion object {
        val nodeInfo = NodeInfoImpl(listOf(), listOf(getTestPartyAndCertificate(ALICE)), 0, 0)
    }

    @Rule
    @JvmField
    val testSerialization = SerializationEnvironmentRule()
    @Rule
    @JvmField
    val tempFolder = TemporaryFolder()
    private lateinit var nodeInfoPath: Path
    private val scheduler = TestScheduler()
    private val testSubscriber = TestSubscriber<NodeInfo>()
    private lateinit var keyManagementService: KeyManagementService

    // Object under test
    private lateinit var nodeInfoWatcher: NodeInfoWatcher

    @Before
    fun start() {
        val identityService = makeTestIdentityService()
        keyManagementService = MockKeyManagementService(identityService, ALICE_KEY)
        nodeInfoWatcher = NodeInfoWatcher(tempFolder.root.toPath(), scheduler)
        nodeInfoPath = tempFolder.root.toPath() / CordformNode.NODE_INFO_DIRECTORY
    }

    @Test
    fun `save a NodeInfo`() {
        assertEquals(0,
                tempFolder.root.list().filter { it.startsWith(NodeInfoFilesCopier.NODE_INFO_FILE_NAME_PREFIX) }.size)
        val signedNodeInfo = SignedData(nodeInfo.serialize(), keyManagementService.sign(nodeInfo.serialize().bytes, nodeInfo.legalIdentities.first().owningKey))
        NodeInfoWatcher.saveToFile(tempFolder.root.toPath(), signedNodeInfo)

        val nodeInfoFiles = tempFolder.root.list().filter { it.startsWith(NodeInfoFilesCopier.NODE_INFO_FILE_NAME_PREFIX) }
        assertEquals(1, nodeInfoFiles.size)
        val fileName = nodeInfoFiles.first()
        assertTrue(fileName.startsWith(NodeInfoFilesCopier.NODE_INFO_FILE_NAME_PREFIX))
        val file = (tempFolder.root.path / fileName).toFile()
        // Just check that something is written, another tests verifies that the written value can be read back.
        assertThat(contentOf(file)).isNotEmpty()
    }

    @Test
    fun `save a NodeInfo to JimFs`() {
        val jimFs = Jimfs.newFileSystem(Configuration.unix())
        val jimFolder = jimFs.getPath("/nodeInfo")
        val signedNodeInfo = SignedData(nodeInfo.serialize(), keyManagementService.sign(nodeInfo.serialize().bytes, nodeInfo.legalIdentities.first().owningKey))
        NodeInfoWatcher.saveToFile(jimFolder, signedNodeInfo)
    }

    @Test
    fun `load an empty Directory`() {
        nodeInfoPath.createDirectories()

        val subscription = nodeInfoWatcher.nodeInfoUpdates()
                .subscribe(testSubscriber)
        try {
            advanceTime()

            val readNodes = testSubscriber.onNextEvents.distinct()
            assertEquals(0, readNodes.size)
        } finally {
            subscription.unsubscribe()
        }
    }

    @Test
    fun `load a non empty Directory`() {
        createNodeInfoFileInPath(nodeInfo)

        val subscription = nodeInfoWatcher.nodeInfoUpdates()
                .subscribe(testSubscriber)
        advanceTime()

        try {
            val readNodes = testSubscriber.onNextEvents.distinct()

            assertEquals(1, readNodes.size)
            assertEquals(nodeInfo, readNodes.first())
        } finally {
            subscription.unsubscribe()
        }
    }

    @Test
    fun `polling folder`() {
        nodeInfoPath.createDirectories()

        // Start polling with an empty folder.
        val subscription = nodeInfoWatcher.nodeInfoUpdates()
                .subscribe(testSubscriber)
        try {
            // Ensure the watch service is started.
            advanceTime()
            // Check no nodeInfos are read.
            assertEquals(0, testSubscriber.valueCount)
            createNodeInfoFileInPath(nodeInfo)

            advanceTime()

            // We need the WatchService to report a change and that might not happen immediately.
            testSubscriber.awaitValueCount(1, 5, TimeUnit.SECONDS)
            // The same folder can be reported more than once, so take unique values.
            val readNodes = testSubscriber.onNextEvents.distinct()
            assertEquals(nodeInfo, readNodes.first())
        } finally {
            subscription.unsubscribe()
        }
    }

    private fun advanceTime() {
        scheduler.advanceTimeBy(1, TimeUnit.MINUTES)
    }

    // Write a nodeInfo under the right path.
    private fun createNodeInfoFileInPath(nodeInfo: NodeInfo) {
        val signedNodeInfo = SignedData(nodeInfo.serialize(), keyManagementService.sign(nodeInfo.serialize().bytes, nodeInfo.legalIdentities.first().owningKey))
        NodeInfoWatcher.saveToFile(nodeInfoPath, signedNodeInfo)
    }
}
