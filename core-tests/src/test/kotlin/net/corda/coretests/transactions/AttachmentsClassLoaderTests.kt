package net.corda.coretests.transactions

import net.corda.core.contracts.Attachment
import net.corda.core.contracts.Contract
import net.corda.core.contracts.TransactionVerificationException
import net.corda.core.crypto.Crypto
import net.corda.core.crypto.SecureHash
import net.corda.core.internal.AttachmentTrustCalculator
import net.corda.core.internal.declaredField
import net.corda.core.internal.hash
import net.corda.core.internal.inputStream
import net.corda.core.node.NetworkParameters
import net.corda.core.node.services.AttachmentId
import net.corda.core.serialization.internal.AttachmentsClassLoader
import net.corda.core.serialization.internal.AttachmentsClassLoaderBuilder
import net.corda.node.services.attachments.NodeAttachmentTrustCalculator
import net.corda.testing.common.internal.testNetworkParameters
import net.corda.testing.core.SerializationEnvironmentRule
import net.corda.testing.core.internal.ContractJarTestUtils.signContractJar
import net.corda.testing.internal.TestingNamedCacheFactory
import net.corda.testing.internal.fakeAttachment
import net.corda.testing.internal.fakeZIPAttachment
import net.corda.testing.internal.services.InternalMockAttachmentStorage
import net.corda.testing.node.internal.FINANCE_CONTRACTS_CORDAPP
import net.corda.testing.services.MockAttachmentStorage
import org.apache.commons.io.IOUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.net.URL
import kotlin.test.assertFailsWith
import kotlin.test.fail
import kotlin.test.assertNotNull

class AttachmentsClassLoaderTests {
    companion object {
        // TODO Update this test to use the new isolated.jar
        val ISOLATED_CONTRACTS_JAR_PATH: URL = AttachmentsClassLoaderTests::class.java.getResource("old-isolated.jar")
        val ISOLATED_CONTRACTS_JAR_PATH_V4: URL = AttachmentsClassLoaderTests::class.java.getResource("isolated-4.0.jar")
        private const val ISOLATED_CONTRACT_CLASS_NAME = "net.corda.finance.contracts.isolated.AnotherDummyContract"

        private fun readAttachment(attachment: Attachment, filepath: String): ByteArray {
            return ByteArrayOutputStream().let {
                attachment.extractFile(filepath, it)
                it.toByteArray()
            }
        }
    }

    @Rule
    @JvmField
    val testSerialization = SerializationEnvironmentRule()

    private lateinit var storage: MockAttachmentStorage
    private lateinit var internalStorage: InternalMockAttachmentStorage
    private lateinit var attachmentTrustCalculator: AttachmentTrustCalculator
    private val networkParameters = testNetworkParameters()
    private val cacheFactory = TestingNamedCacheFactory()

    private fun createClassloader(
        attachment: AttachmentId,
        params: NetworkParameters = networkParameters
    ): AttachmentsClassLoader {
        return createClassloader(listOf(attachment), params)
    }

    private fun createClassloader(
        attachments: List<AttachmentId>,
        params: NetworkParameters = networkParameters
    ): AttachmentsClassLoader {
        return AttachmentsClassLoader(
            attachments.map { storage.openAttachment(it)!! },
            params,
            SecureHash.zeroHash,
            attachmentTrustCalculator::calculate
        )
    }

    @Before
    fun setup() {
        storage = MockAttachmentStorage()
        internalStorage = InternalMockAttachmentStorage(storage)
        attachmentTrustCalculator = NodeAttachmentTrustCalculator(internalStorage, cacheFactory)
    }

    @Test(timeout=300_000)
	fun `Loading AnotherDummyContract without using the AttachmentsClassLoader fails`() {
        assertFailsWith<ClassNotFoundException> {
            Class.forName(ISOLATED_CONTRACT_CLASS_NAME)
        }
    }

    @Test(timeout=300_000)
    fun `test contracts have no permissions for protection domain`() {
        val isolatedId = importAttachment(ISOLATED_CONTRACTS_JAR_PATH.openStream(), "app", "isolated.jar")
        assertNull(System.getSecurityManager())

        createClassloader(isolatedId).use { classLoader ->
            val contractClass = Class.forName(ISOLATED_CONTRACT_CLASS_NAME, true, classLoader)
            val protectionDomain = contractClass.protectionDomain ?: fail("Protection Domain missing")
            val permissions = protectionDomain.permissions ?: fail("Protection domain has no permissions")
            assertThat(permissions.elements().toList()).isEmpty()
            assertTrue(permissions.isReadOnly)
        }
    }

    @Test(timeout=300_000)
	fun `Dynamically load AnotherDummyContract from isolated contracts jar using the AttachmentsClassLoader`() {
        val isolatedId = importAttachment(ISOLATED_CONTRACTS_JAR_PATH.openStream(), "app", "isolated.jar")

        createClassloader(isolatedId).use { classloader ->
            val contractClass = Class.forName(ISOLATED_CONTRACT_CLASS_NAME, true, classloader)
            val contract = contractClass.getDeclaredConstructor().newInstance() as Contract
            assertEquals("helloworld", contract.declaredField<Any?>("magicString").value)
        }
    }

    @Test(timeout=300_000)
	fun `Test non-overlapping contract jar`() {
        val att1 = importAttachment(ISOLATED_CONTRACTS_JAR_PATH.openStream(), "app", "isolated.jar")
        val att2 = importAttachment(ISOLATED_CONTRACTS_JAR_PATH_V4.openStream(), "app", "isolated-4.0.jar")

        assertFailsWith(TransactionVerificationException.OverlappingAttachmentsException::class) {
            createClassloader(listOf(att1, att2)).use {}
        }
    }

    @Test(timeout=300_000)
	fun `Test valid overlapping contract jar`() {
        val isolatedId = importAttachment(ISOLATED_CONTRACTS_JAR_PATH.openStream(), "app", "isolated.jar")
        val signedJar = signContractJar(ISOLATED_CONTRACTS_JAR_PATH, copyFirst = true)
        val isolatedSignedId = importAttachment(signedJar.first.toUri().toURL().openStream(), "app", "isolated-signed.jar")

        // does not throw OverlappingAttachments exception
        createClassloader(listOf(isolatedId, isolatedSignedId)).use {}
    }

    @Test(timeout=300_000)
	fun `Test non-overlapping different contract jars`() {
        val att1 = importAttachment(ISOLATED_CONTRACTS_JAR_PATH.openStream(), "app", "isolated.jar")
        val att2 = importAttachment(FINANCE_CONTRACTS_CORDAPP.jarFile.inputStream(), "app", "finance.jar")

        // does not throw OverlappingAttachments exception
        createClassloader(listOf(att1, att2)).use {}
    }

    @Test(timeout=300_000)
	fun `Load text resources from AttachmentsClassLoader`() {
        val att1 = importAttachment(fakeAttachment("file1.txt", "some data").inputStream(), "app", "file1.jar")
        val att2 = importAttachment(fakeAttachment("file2.txt", "some other data").inputStream(), "app", "file2.jar")

        createClassloader(listOf(att1, att2)).use { cl ->
            val txt = IOUtils.toString(cl.getResourceAsStream("file1.txt"), Charsets.UTF_8.name())
            assertEquals("some data", txt)

            val txt1 = IOUtils.toString(cl.getResourceAsStream("file2.txt"), Charsets.UTF_8.name())
            assertEquals("some other data", txt1)
        }
    }

    @Test(timeout=300_000)
	fun `Test valid overlapping file condition`() {
        val att1 = importAttachment(fakeAttachment("file1.txt", "same data", "file2.txt", "same other data").inputStream(), "app", "file1.jar")
        val att2 = importAttachment(fakeAttachment("file1.txt", "same data", "file3.txt", "same totally different").inputStream(), "app", "file2.jar")

        createClassloader(listOf(att1, att2)).use { cl ->
            val txt = IOUtils.toString(cl.getResourceAsStream("file1.txt"), Charsets.UTF_8.name())
            assertEquals("same data", txt)
        }
    }

    @Test(timeout=300_000)
	fun `No overlapping exception thrown on certain META-INF files`() {
        listOf("meta-inf/manifest.mf", "meta-inf/license", "meta-inf/test.dsa", "meta-inf/test.sf").forEach { path ->
            val att1 = importAttachment(fakeAttachment(path, "some data").inputStream(), "app", "file1.jar")
            val att2 = importAttachment(fakeAttachment(path, "some other data").inputStream(), "app", "file2.jar")

            createClassloader(listOf(att1, att2)).use {}
        }
    }

    @Test(timeout=300_000)
	fun `Overlapping rules for META-INF SerializationWhitelist files`() {
        val att1 = importAttachment(fakeAttachment("meta-inf/services/net.corda.core.serialization.SerializationWhitelist", "some data").inputStream(), "app", "file1.jar")
        val att2 = importAttachment(fakeAttachment("meta-inf/services/net.corda.core.serialization.SerializationWhitelist", "some other data").inputStream(), "app", "file2.jar")

        createClassloader(listOf(att1, att2)).use {}
    }

    @Test(timeout=300_000)
	fun `Overlapping rules for META-INF random service files`() {
        val att1 = importAttachment(fakeAttachment("meta-inf/services/com.example.something", "some data").inputStream(), "app", "file1.jar")
        val att2 = importAttachment(fakeAttachment("meta-inf/services/com.example.something", "some other data").inputStream(), "app", "file2.jar")

        assertFailsWith(TransactionVerificationException.OverlappingAttachmentsException::class) {
            createClassloader(listOf(att1, att2)).use {}
        }
    }

    @Test(timeout=300_000)
	fun `Test overlapping file exception`() {
        val att1 = storage.importAttachment(fakeAttachment("file1.txt", "some data").inputStream(), "app", "file1.jar")
        val att2 = storage.importAttachment(fakeAttachment("file1.txt", "some other data").inputStream(), "app", "file2.jar")

        assertFailsWith(TransactionVerificationException.OverlappingAttachmentsException::class) {
            createClassloader(listOf(att1, att2)).use {}
        }
    }

    @Test(timeout=300_000)
	fun `partial overlaps not possible`() {
        // Cover a previous bug whereby overlap checking had been optimized to only check contract classes, which isn't
        // a valid optimization as code used by the contract class could then be overlapped.
        val att1 = importAttachment(ISOLATED_CONTRACTS_JAR_PATH.openStream(), "app", ISOLATED_CONTRACTS_JAR_PATH.file)
        val att2 = importAttachment(fakeAttachment("net/corda/finance/contracts/isolated/AnotherDummyContract\$State.class", "some attackdata").inputStream(), "app", "file2.jar")
        assertFailsWith(TransactionVerificationException.OverlappingAttachmentsException::class) {
            createClassloader(listOf(att1, att2)).use {}
        }
    }

    @Test(timeout=300_000)
	fun `Check platform independent path handling in attachment jars`() {
        val att1 = importAttachment(fakeAttachment("/folder1/foldera/file1.txt", "some data").inputStream(), "app", "file1.jar")
        val att2 = importAttachment(fakeAttachment("\\folder1\\folderb\\file2.txt", "some other data").inputStream(), "app", "file2.jar")

        val data1a = readAttachment(storage.openAttachment(att1)!!, "/folder1/foldera/file1.txt")
        assertArrayEquals("some data".toByteArray(), data1a)

        val data1b = readAttachment(storage.openAttachment(att1)!!, "\\folder1\\foldera\\file1.txt")
        assertArrayEquals("some data".toByteArray(), data1b)

        val data2a = readAttachment(storage.openAttachment(att2)!!, "\\folder1\\folderb\\file2.txt")
        assertArrayEquals("some other data".toByteArray(), data2a)

        val data2b = readAttachment(storage.openAttachment(att2)!!, "/folder1/folderb/file2.txt")
        assertArrayEquals("some other data".toByteArray(), data2b)
    }

    @Test(timeout=300_000)
	fun `Allow loading untrusted resource jars but only trusted jars that contain class files`() {
        val trustedResourceJar = importAttachment(fakeAttachment("file1.txt", "some data").inputStream(), "app", "file0.jar")
        val untrustedResourceJar = importAttachment(fakeAttachment("file2.txt", "some malicious data").inputStream(), "untrusted", "file1.jar")
        val untrustedClassJar = importAttachment(fakeAttachment("/com/example/something/MaliciousClass.class", "some malicious data").inputStream(), "untrusted", "file2.jar")
        val trustedClassJar = importAttachment(fakeAttachment("/com/example/something/VirtuousClass.class", "some other data").inputStream(), "app", "file3.jar")

        createClassloader(listOf(trustedResourceJar, untrustedResourceJar, trustedClassJar)).use {
            assertFailsWith(TransactionVerificationException.UntrustedAttachmentsException::class) {
                createClassloader(listOf(trustedResourceJar, untrustedResourceJar, trustedClassJar, untrustedClassJar)).use {}
            }
        }
    }

    private fun importAttachment(jar: InputStream, uploader: String, filename: String?): AttachmentId {
        return jar.use { storage.importAttachment(jar, uploader, filename) }
    }

    @Test(timeout=300_000)
	fun `Allow loading an untrusted contract jar if another attachment exists that was signed with the same keys and uploaded by a trusted uploader`() {
        val keyPairA = Crypto.generateKeyPair()
        val keyPairB = Crypto.generateKeyPair()
        val classJar = fakeAttachment(
            "/com/example/something/TrustedClass.class",
            "Signed by someone trusted"
        ).inputStream()
        storage.importContractAttachment(
            listOf("TrustedClass.class"),
            "rpc",
            classJar,
            signers = listOf(keyPairA.public, keyPairB.public)
        )

        val untrustedClassJar = fakeAttachment(
            "/com/example/something/UntrustedClass.class",
            "Signed by someone untrusted"
        ).inputStream()
        val untrustedAttachment = storage.importContractAttachment(
            listOf("UntrustedClass.class"),
            "untrusted",
            untrustedClassJar,
            signers = listOf(keyPairA.public, keyPairB.public)
        )

        createClassloader(untrustedAttachment).use {}
    }

    @Test(timeout=300_000)
	fun `Allow loading an untrusted contract jar if another attachment exists that was signed by a trusted uploader - intersection of keys match existing attachment`() {
        val keyPairA = Crypto.generateKeyPair()
        val keyPairB = Crypto.generateKeyPair()
        val keyPairC = Crypto.generateKeyPair()
        val classJar = fakeAttachment(
            "/com/example/something/TrustedClass.class",
            "Signed by someone trusted"
        ).inputStream()
        storage.importContractAttachment(
            listOf("TrustedClass.class"),
            "rpc",
            classJar,
            signers = listOf(keyPairA.public, keyPairC.public)
        )

        val untrustedClassJar = fakeAttachment(
            "/com/example/something/UntrustedClass.class",
            "Signed by someone untrusted"
        ).inputStream()
        val untrustedAttachment = storage.importContractAttachment(
            listOf("UntrustedClass.class"),
            "untrusted",
            untrustedClassJar,
            signers = listOf(keyPairA.public, keyPairB.public)
        )

        createClassloader(untrustedAttachment).use {}
    }

    @Test(timeout=300_000)
	fun `Cannot load an untrusted contract jar if no other attachment exists that was signed with the same keys`() {
        val keyPairA = Crypto.generateKeyPair()
        val keyPairB = Crypto.generateKeyPair()
        val untrustedClassJar = fakeAttachment(
            "/com/example/something/UntrustedClass.class",
            "Signed by someone untrusted"
        ).inputStream()
        val untrustedAttachment = storage.importContractAttachment(
            listOf("UntrustedClass.class"),
            "untrusted",
            untrustedClassJar,
            signers = listOf(keyPairA.public, keyPairB.public)
        )

        assertFailsWith(TransactionVerificationException.UntrustedAttachmentsException::class) {
            createClassloader(untrustedAttachment).use {}
        }
    }

    @Test(timeout=300_000)
	fun `Cannot load an untrusted contract jar if no other attachment exists that was signed with the same keys and uploaded by a trusted uploader`() {
        val keyPairA = Crypto.generateKeyPair()
        val keyPairB = Crypto.generateKeyPair()
        val classJar = fakeAttachment(
            "/com/example/something/UntrustedClass.class",
            "Signed by someone untrusted with the same keys"
        ).inputStream()
        storage.importContractAttachment(
            listOf("UntrustedClass.class"),
            "untrusted",
            classJar,
            signers = listOf(keyPairA.public, keyPairB.public)
        )

        val untrustedClassJar = fakeAttachment(
            "/com/example/something/UntrustedClass.class",
            "Signed by someone untrusted"
        ).inputStream()
        val untrustedAttachment = storage.importContractAttachment(
            listOf("UntrustedClass.class"),
            "untrusted",
            untrustedClassJar,
            signers = listOf(keyPairA.public, keyPairB.public)
        )

        assertFailsWith(TransactionVerificationException.UntrustedAttachmentsException::class) {
            createClassloader(untrustedAttachment).use {}
        }
    }

    @Test(timeout=300_000)
	fun `Attachments with inherited trust do not grant trust to attachments being loaded (no chain of trust)`() {
        val keyPairA = Crypto.generateKeyPair()
        val keyPairB = Crypto.generateKeyPair()
        val keyPairC = Crypto.generateKeyPair()
        val classJar = fakeAttachment(
            "/com/example/something/TrustedClass.class",
            "Signed by someone untrusted with the same keys"
        ).inputStream()
        storage.importContractAttachment(
            listOf("TrustedClass.class"),
            "app",
            classJar,
            signers = listOf(keyPairA.public)
        )

        val inheritedTrustClassJar = fakeAttachment(
            "/com/example/something/UntrustedClass.class",
            "Signed by someone who inherits trust"
        ).inputStream()
        val inheritedTrustAttachment = storage.importContractAttachment(
            listOf("UntrustedClass.class"),
            "untrusted",
            inheritedTrustClassJar,
            signers = listOf(keyPairB.public, keyPairA.public)
        )

        val untrustedClassJar = fakeAttachment(
            "/com/example/something/UntrustedClass.class",
            "Signed by someone untrusted"
        ).inputStream()
        val untrustedAttachment = storage.importContractAttachment(
            listOf("UntrustedClass.class"),
            "untrusted",
            untrustedClassJar,
            signers = listOf(keyPairB.public, keyPairC.public)
        )

        // pass the inherited trust attachment through the classloader first to ensure it does not affect the next loaded attachment
        createClassloader(inheritedTrustAttachment).use {
            assertFailsWith(TransactionVerificationException.UntrustedAttachmentsException::class) {
                createClassloader(untrustedAttachment).use {}
            }
        }
    }

    @Test(timeout=300_000)
	fun `Cannot load an untrusted contract jar if it is signed by a blacklisted key even if there is another attachment signed by the same keys that is trusted`() {
        val keyPairA = Crypto.generateKeyPair()
        val keyPairB = Crypto.generateKeyPair()

        attachmentTrustCalculator = NodeAttachmentTrustCalculator(
            InternalMockAttachmentStorage(storage),
            cacheFactory,
            blacklistedAttachmentSigningKeys = listOf(keyPairA.public.hash)
        )

        val classJar = fakeAttachment(
            "/com/example/something/TrustedClass.class",
            "Signed by someone trusted"
        ).inputStream()
        storage.importContractAttachment(
            listOf("TrustedClass.class"),
            "rpc",
            classJar,
            signers = listOf(keyPairA.public, keyPairB.public)
        )

        val untrustedClassJar = fakeAttachment(
            "/com/example/something/UntrustedClass.class",
            "Signed by someone untrusted"
        ).inputStream()
        val untrustedAttachment = storage.importContractAttachment(
            listOf("UntrustedClass.class"),
            "untrusted",
            untrustedClassJar,
            signers = listOf(keyPairA.public, keyPairB.public)
        )

        assertFailsWith(TransactionVerificationException.UntrustedAttachmentsException::class) {
            createClassloader(untrustedAttachment).use {}
        }
    }

    @Test(timeout=300_000)
	fun `Allow loading a trusted attachment that is signed by a blacklisted key`() {
        val keyPairA = Crypto.generateKeyPair()

        attachmentTrustCalculator = NodeAttachmentTrustCalculator(
            InternalMockAttachmentStorage(storage),
            cacheFactory,
            blacklistedAttachmentSigningKeys = listOf(keyPairA.public.hash)
        )

        val classJar = fakeAttachment(
            "/com/example/something/TrustedClass.class",
            "Signed by someone trusted"
        ).inputStream()
        val trustedAttachment = storage.importContractAttachment(
            listOf("TrustedClass.class"),
            "rpc",
            classJar,
            signers = listOf(keyPairA.public)
        )

        createClassloader(trustedAttachment).use {}
    }

    @Test(timeout=300_000)
    fun `Load both JAR and ZIP archives into the attachments classloader`() {
        val isolatedId = importAttachment(ISOLATED_CONTRACTS_JAR_PATH.openStream(), "app", "isolated.jar")
        val zip = importAttachment(fakeZIPAttachment("importantDoc.pdf", "I am a pdf!").inputStream(), "app", "importantDoc")

        AttachmentsClassLoaderBuilder.withAttachmentsClassloaderContext(listOf(isolatedId, zip).map { storage.openAttachment(it)!! },
                                                                 testNetworkParameters(), SecureHash.allOnesHash, { true }) { classloader ->

            // The importantDoc is available in the tx classloader.
            val importantDoc = classloader.getResourceAsStream("importantDoc.pdf")
            assertNotNull(importantDoc)

            // The importantDoc is not available in the tx code classloader.
            val importantDocParent = classloader.parent.getResourceAsStream("importantDoc.pdf")
            assertNull(importantDocParent)

            // The contract is available in the tx code classloader.
            val contract = classloader.parent.loadClass(ISOLATED_CONTRACT_CLASS_NAME)
            assertNotNull(contract)
        }
    }
}
