package net.corda.nodeapitests.internal.network

import com.typesafe.config.ConfigFactory
import net.corda.core.crypto.secureRandomBytes
import net.corda.core.crypto.sha256
import net.corda.core.identity.CordaX500Name
import net.corda.core.internal.*
import net.corda.core.node.NetworkParameters
import net.corda.core.node.NodeInfo
import net.corda.core.serialization.serialize
import net.corda.node.services.config.NotaryConfig
import net.corda.nodeapi.internal.DEV_ROOT_CA
import net.corda.core.internal.NODE_INFO_DIRECTORY
import net.corda.core.utilities.days
import net.corda.nodeapi.internal.SignedNodeInfo
import net.corda.nodeapi.internal.config.parseAs
import net.corda.nodeapi.internal.config.toConfig
import net.corda.nodeapi.internal.network.NetworkBootstrapper.Companion.DEFAULT_MAX_MESSAGE_SIZE
import net.corda.nodeapi.internal.network.NetworkBootstrapper.Companion.DEFAULT_MAX_TRANSACTION_SIZE
import net.corda.nodeapi.internal.network.NodeInfoFilesCopier.Companion.NODE_INFO_FILE_NAME_PREFIX
import net.corda.testing.core.ALICE_NAME
import net.corda.testing.core.BOB_NAME
import net.corda.testing.core.DUMMY_NOTARY_NAME
import net.corda.testing.core.SerializationEnvironmentRule
import net.corda.testing.core.TestIdentity
import net.corda.coretesting.internal.createNodeInfoAndSigned
import net.corda.nodeapi.internal.network.CopyCordapps
import net.corda.nodeapi.internal.network.NETWORK_PARAMS_FILE_NAME
import net.corda.nodeapi.internal.network.NetworkBootstrapper
import net.corda.nodeapi.internal.network.NetworkParametersOverrides
import net.corda.nodeapi.internal.network.PackageOwner
import net.corda.nodeapi.internal.network.SignedNetworkParameters
import net.corda.nodeapi.internal.network.TestContractsJar
import net.corda.nodeapi.internal.network.verifiedNetworkParametersCert
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.After
import org.junit.AfterClass
import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExpectedException
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.security.PublicKey
import java.time.Duration
import kotlin.streams.toList

class NetworkBootstrapperTest {
    @Rule
    @JvmField
    val tempFolder = TemporaryFolder()

    @Rule
    @JvmField
    val expectedEx: ExpectedException = ExpectedException.none()

    @Rule
    @JvmField
    val testSerialization = SerializationEnvironmentRule()

    companion object {
        private val fakeEmbeddedCorda = fakeFileBytes()
        private val fakeEmbeddedCordaJar = Files.createTempFile("corda", ".jar").write(fakeEmbeddedCorda)

        private fun fakeFileBytes(writeToFile: Path? = null): ByteArray {
            val bytes = secureRandomBytes(128)
            writeToFile?.write(bytes)
            return bytes
        }

        @JvmStatic
        @AfterClass
        fun cleanUp() {
            Files.delete(fakeEmbeddedCordaJar)
        }
    }

    private val contractsJars = hashMapOf<Path, TestContractsJar>()

    private val contractsJarConverterWithAdditionalFlag: (Path, Boolean) -> TestContractsJar = { path: Path, _: Boolean -> contractsJars[path]!! }

    private val bootstrapper = NetworkBootstrapper(
            initSerEnv = false,
            embeddedCordaJar = {
                fakeEmbeddedCordaJar.toUri().toURL()
            },
            nodeInfosGenerator = { nodeDirs ->
                nodeDirs.map { nodeDir ->
                    val name = nodeDir.fakeNodeConfig.myLegalName
                    val file = nodeDir / "$NODE_INFO_FILE_NAME_PREFIX${name.serialize().hash}"
                    if (!file.exists()) {
                        createNodeInfoAndSigned(name).signed.serialize().open().copyTo(file)
                    }
                    file
                }
            },
            contractsJarConverter = contractsJarConverterWithAdditionalFlag
    )

    private val aliceConfig = FakeNodeConfig(ALICE_NAME)
    private val bobConfig = FakeNodeConfig(BOB_NAME)
    private val notaryConfig = FakeNodeConfig(DUMMY_NOTARY_NAME, NotaryConfig(validating = true))

    private var providedCordaJar: ByteArray? = null
    private val configFiles = HashMap<Path, String>()

    @After
    fun `check config files are preserved`() {
        configFiles.forEach { file, text ->
            assertThat(file).hasContent(text)
        }
    }

    @After
    fun `check provided corda jar is preserved`() {
        if (providedCordaJar == null) {
            // Make sure we clean up if we used the embedded jar
            assertThat(rootDir / "corda.jar").doesNotExist()
        } else {
            // Make sure we don't delete it if it was provided by the user
            assertThat(rootDir / "corda.jar").hasBinaryContent(providedCordaJar)
        }
    }

    @Test(timeout=300_000)
	fun `empty dir`() {
        assertThatThrownBy {
            bootstrap()
        }.hasMessage("No nodes found")
    }

    @Test(timeout=300_000)
	fun `single node conf file`() {
        createNodeConfFile("node1", bobConfig)
        bootstrap()
        val networkParameters = assertBootstrappedNetwork(fakeEmbeddedCorda, "node1" to bobConfig)
        networkParameters.run {
            assertThat(epoch).isEqualTo(1)
            assertThat(notaries).isEmpty()
            assertThat(whitelistedContractImplementations).isEmpty()
        }
    }

    @Test(timeout=300_000)
	fun `node conf file and corda jar`() {
        createNodeConfFile("node1", bobConfig)
        val fakeCordaJar = fakeFileBytes(rootDir / "corda.jar")
        bootstrap()
        assertBootstrappedNetwork(fakeCordaJar, "node1" to bobConfig)
    }

    @Test(timeout=300_000)
	fun `single node directory with just node conf file`() {
        createNodeDir("bob", bobConfig)
        bootstrap()
        assertBootstrappedNetwork(fakeEmbeddedCorda, "bob" to bobConfig)
    }

    @Test(timeout=300_000)
	fun `single node directory with node conf file and corda jar`() {
        val nodeDir = createNodeDir("bob", bobConfig)
        val fakeCordaJar = fakeFileBytes(nodeDir / "corda.jar")
        bootstrap()
        assertBootstrappedNetwork(fakeCordaJar, "bob" to bobConfig)
    }

    @Test(timeout=300_000)
	fun `single node directory with just corda jar`() {
        val nodeCordaJar = (rootDir / "alice").createDirectories() / "corda.jar"
        val fakeCordaJar = fakeFileBytes(nodeCordaJar)
        assertThatThrownBy {
            bootstrap()
        }.hasMessageStartingWith("Missing node.conf in node directory alice")
        assertThat(nodeCordaJar).hasBinaryContent(fakeCordaJar)  // Make sure the corda.jar is left untouched
    }

    @Test(timeout=300_000)
	fun `two node conf files, one of which is a notary`() {
        createNodeConfFile("alice", aliceConfig)
        createNodeConfFile("notary", notaryConfig)
        bootstrap()
        val networkParameters = assertBootstrappedNetwork(fakeEmbeddedCorda, "alice" to aliceConfig, "notary" to notaryConfig)
        networkParameters.assertContainsNotary("notary")
    }

    @Test(timeout=300_000)
	fun `two node conf files with the same legal name`() {
        createNodeConfFile("node1", aliceConfig)
        createNodeConfFile("node2", aliceConfig)
        assertThatThrownBy {
            bootstrap()
        }.hasMessageContaining("Nodes must have unique legal names")
    }

    @Test(timeout=300_000)
	fun `one node directory and one node conf file`() {
        createNodeConfFile("alice", aliceConfig)
        createNodeDir("bob", bobConfig)
        bootstrap()
        assertBootstrappedNetwork(fakeEmbeddedCorda, "alice" to aliceConfig, "bob" to bobConfig)
    }

    @Test(timeout=300_000)
	fun `node conf file and CorDapp jar`() {
        createNodeConfFile("alice", aliceConfig)
        val cordappBytes = createFakeCordappJar("sample-app", listOf("contract.class"))
        bootstrap()
        val networkParameters = assertBootstrappedNetwork(fakeEmbeddedCorda, "alice" to aliceConfig)
        assertThat(rootDir / "alice" / "cordapps" / "sample-app.jar").hasBinaryContent(cordappBytes)
        assertThat(networkParameters.whitelistedContractImplementations).isEqualTo(mapOf(
                "contract.class" to listOf(cordappBytes.sha256())
        ))
    }

    @Test(timeout=300_000)
	fun `no copy CorDapps`() {
        createNodeConfFile("alice", aliceConfig)
        val cordappBytes = createFakeCordappJar("sample-app", listOf("contract.class"))
        bootstrap(copyCordapps = CopyCordapps.No)
        val networkParameters = assertBootstrappedNetwork(fakeEmbeddedCorda, "alice" to aliceConfig)
        assertThat(rootDir / "alice" / "cordapps" / "sample-app.jar").doesNotExist()
        assertThat(networkParameters.whitelistedContractImplementations).isEqualTo(mapOf(
                "contract.class" to listOf(cordappBytes.sha256())
        ))
    }

    @Test(timeout=300_000)
	fun `add node to existing network`() {
        createNodeConfFile("alice", aliceConfig)
        bootstrap()
        val networkParameters1 = (rootDir / "alice").networkParameters
        createNodeConfFile("bob", bobConfig)
        bootstrap()
        val networkParameters2 = assertBootstrappedNetwork(fakeEmbeddedCorda, "alice" to aliceConfig, "bob" to bobConfig)
        assertThat(networkParameters1).isEqualTo(networkParameters2)
    }

    @Test(timeout=300_000)
	fun `add notary to existing network`() {
        createNodeConfFile("alice", aliceConfig)
        bootstrap()
        createNodeConfFile("notary", notaryConfig)
        bootstrap()
        val networkParameters = assertBootstrappedNetwork(fakeEmbeddedCorda, "alice" to aliceConfig, "notary" to notaryConfig)
        networkParameters.assertContainsNotary("notary")
        assertThat(networkParameters.epoch).isEqualTo(2)
    }

    @Test(timeout=300_000)
	fun `network parameters overrides`() {
        createNodeConfFile("alice", aliceConfig)
        val minimumPlatformVersion = 2
        val maxMessageSize = 10000
        val maxTransactionSize = 20000
        val eventHorizon = 7.days
        bootstrap(minimumPlatformVerison = minimumPlatformVersion,
                    maxMessageSize = maxMessageSize,
                    maxTransactionSize = maxTransactionSize,
                    eventHorizon = eventHorizon)
        val networkParameters = assertBootstrappedNetwork(fakeEmbeddedCorda, "alice" to aliceConfig)
        assertThat(networkParameters.minimumPlatformVersion).isEqualTo(minimumPlatformVersion)
        assertThat(networkParameters.maxMessageSize).isEqualTo(maxMessageSize)
        assertThat(networkParameters.maxTransactionSize).isEqualTo(maxTransactionSize)
        assertThat(networkParameters.eventHorizon).isEqualTo(eventHorizon)
    }

    private val ALICE = TestIdentity(ALICE_NAME, 70)
    private val BOB = TestIdentity(BOB_NAME, 80)

    private val alicePackageName = "com.example.alice"
    private val bobPackageName = "com.example.bob"

    @Test(timeout=300_000)
	fun `register new package namespace in existing network`() {
        createNodeConfFile("alice", aliceConfig)
        bootstrap(packageOwnership = mapOf(Pair(alicePackageName, ALICE.publicKey)))
        assertContainsPackageOwner("alice", mapOf(Pair(alicePackageName, ALICE.publicKey)))
    }

    @Test(timeout=300_000)
	fun `register additional package namespace in existing network`() {
        createNodeConfFile("alice", aliceConfig)
        bootstrap(packageOwnership = mapOf(Pair(alicePackageName, ALICE.publicKey)))
        assertContainsPackageOwner("alice", mapOf(Pair(alicePackageName, ALICE.publicKey)))
        // register additional package name
        createNodeConfFile("bob", bobConfig)
        bootstrap(packageOwnership = mapOf(Pair(alicePackageName, ALICE.publicKey), Pair(bobPackageName, BOB.publicKey)))
        assertContainsPackageOwner("bob", mapOf(Pair(alicePackageName, ALICE.publicKey), Pair(bobPackageName, BOB.publicKey)))
    }

    @Test(timeout=300_000)
	fun `attempt to register overlapping namespaces in existing network`() {
        createNodeConfFile("alice", aliceConfig)
        val greedyNamespace = "com.example"
        bootstrap(packageOwnership = mapOf(Pair(greedyNamespace, ALICE.publicKey)))
        assertContainsPackageOwner("alice", mapOf(Pair(greedyNamespace, ALICE.publicKey)))
        // register overlapping package name
        createNodeConfFile("bob", bobConfig)
        expectedEx.expect(IllegalArgumentException::class.java)
        expectedEx.expectMessage("Multiple packages added to the packageOwnership overlap.")
        bootstrap(packageOwnership = mapOf(Pair(greedyNamespace, ALICE.publicKey), Pair(bobPackageName, BOB.publicKey)))
    }

    @Test(timeout=300_000)
	fun `unregister single package namespace in network of one`() {
        createNodeConfFile("alice", aliceConfig)
        bootstrap(packageOwnership = mapOf(Pair(alicePackageName, ALICE.publicKey)))
        assertContainsPackageOwner("alice", mapOf(Pair(alicePackageName, ALICE.publicKey)))
        // unregister package name
        bootstrap(packageOwnership = emptyMap())
        assertContainsPackageOwner("alice", emptyMap())
    }

    @Test(timeout=300_000)
	fun `unregister single package namespace in network of many`() {
        createNodeConfFile("alice", aliceConfig)
        bootstrap(packageOwnership = mapOf(Pair(alicePackageName, ALICE.publicKey), Pair(bobPackageName, BOB.publicKey)))
        // unregister package name
        bootstrap(packageOwnership = mapOf(Pair(alicePackageName, ALICE.publicKey)))
        assertContainsPackageOwner("alice", mapOf(Pair(alicePackageName, ALICE.publicKey)))
    }

    @Test(timeout=300_000)
	fun `unregister all package namespaces in existing network`() {
        createNodeConfFile("alice", aliceConfig)
        bootstrap(packageOwnership = mapOf(Pair(alicePackageName, ALICE.publicKey), Pair(bobPackageName, BOB.publicKey)))
        // unregister all package names
        bootstrap(packageOwnership = emptyMap())
        assertContainsPackageOwner("alice", emptyMap())
    }

    private val rootDir get() = tempFolder.root.toPath()

    private fun bootstrap(copyCordapps: CopyCordapps = CopyCordapps.FirstRunOnly,
                          packageOwnership: Map<String, PublicKey>? = emptyMap(),
                          minimumPlatformVerison: Int? = PLATFORM_VERSION,
                          maxMessageSize: Int? = DEFAULT_MAX_MESSAGE_SIZE,
                          maxTransactionSize: Int? = DEFAULT_MAX_TRANSACTION_SIZE,
                          eventHorizon: Duration? = 30.days) {
        providedCordaJar = (rootDir / "corda.jar").let { if (it.exists()) it.readAll() else null }
        bootstrapper.bootstrap(rootDir, copyCordapps, NetworkParametersOverrides(
                minimumPlatformVersion = minimumPlatformVerison,
                maxMessageSize = maxMessageSize,
                maxTransactionSize = maxTransactionSize,
                eventHorizon = eventHorizon,
                packageOwnership = packageOwnership?.map { PackageOwner(it.key, it.value) }
        ))
    }

    private fun createNodeConfFile(nodeDirName: String, config: FakeNodeConfig) {
        writeNodeConfFile(rootDir / "${nodeDirName}_node.conf", config)
    }

    private fun createNodeDir(nodeDirName: String, config: FakeNodeConfig): Path {
        val nodeDir = (rootDir / nodeDirName).createDirectories()
        writeNodeConfFile(nodeDir / "node.conf", config)
        return nodeDir
    }

    private fun writeNodeConfFile(file: Path, config: FakeNodeConfig) {
        val configText = config.toConfig().root().render()
        file.writeText(configText)
        configFiles[file] = configText
    }

    private fun createFakeCordappJar(cordappName: String, contractClassNames: List<String>): ByteArray {
        val cordappJarFile = rootDir / "$cordappName.jar"
        val cordappBytes = fakeFileBytes(cordappJarFile)
        contractsJars[cordappJarFile] = TestContractsJar(cordappBytes.sha256(), contractClassNames)
        return cordappBytes
    }

    private val Path.networkParameters: NetworkParameters
        get() {
            return (this / NETWORK_PARAMS_FILE_NAME).readObject<SignedNetworkParameters>()
                    .verifiedNetworkParametersCert(DEV_ROOT_CA.certificate)
        }

    private val Path.nodeInfoFile: Path
        get() {
            return list { it.filter { it.fileName.toString().startsWith(NODE_INFO_FILE_NAME_PREFIX) }.toList() }.single()
        }

    private val Path.nodeInfo: NodeInfo get() = nodeInfoFile.readObject<SignedNodeInfo>().verified()

    private val Path.fakeNodeConfig: FakeNodeConfig
        get() {
            return ConfigFactory.parseFile((this / "node.conf").toFile()).parseAs(FakeNodeConfig::class)
        }

    private fun assertBootstrappedNetwork(cordaJar: ByteArray, vararg nodes: Pair<String, FakeNodeConfig>): NetworkParameters {
        val networkParameters = (rootDir / nodes[0].first).networkParameters
        val allNodeInfoFiles = nodes.map { (rootDir / it.first).nodeInfoFile }.associateBy({ it }, Path::readAll)

        for ((nodeDirName, config) in nodes) {
            val nodeDir = rootDir / nodeDirName
            assertThat(nodeDir / "corda.jar").hasBinaryContent(cordaJar)
            assertThat(nodeDir.fakeNodeConfig).isEqualTo(config)
            assertThat(nodeDir.networkParameters).isEqualTo(networkParameters)
            // Make sure all the nodes have all of each others' node-info files
            allNodeInfoFiles.forEach { nodeInfoFile, bytes ->
                assertThat(nodeDir / NODE_INFO_DIRECTORY / nodeInfoFile.fileName.toString()).hasBinaryContent(bytes)
            }
        }

        return networkParameters
    }

    private fun NetworkParameters.assertContainsNotary(dirName: String) {
        val notaryParty = (rootDir / dirName).nodeInfo.legalIdentities.single()
        assertThat(notaries).hasSize(1)
        notaries[0].run {
            assertThat(validating).isTrue()
            assertThat(identity.name).isEqualTo(notaryParty.name)
            assertThat(identity.owningKey).isEqualTo(notaryParty.owningKey)
        }
    }

    private fun assertContainsPackageOwner(nodeDirName: String, packageOwners: Map<String, PublicKey>) {
        val networkParams = (rootDir / nodeDirName).networkParameters
        assertThat(networkParams.packageOwnership).isEqualTo(packageOwners)
    }

    data class FakeNodeConfig(val myLegalName: CordaX500Name, val notary: NotaryConfig? = null)
}
