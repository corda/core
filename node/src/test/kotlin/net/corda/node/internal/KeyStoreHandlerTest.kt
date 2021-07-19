package net.corda.node.internal

import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.whenever
import net.corda.core.crypto.CompositeKey
import net.corda.core.crypto.Crypto
import net.corda.core.identity.CordaX500Name
import net.corda.core.internal.div
import net.corda.coretesting.internal.rigorousMock
import net.corda.coretesting.internal.stubs.CertificateStoreStubs
import net.corda.node.services.config.NodeConfiguration
import net.corda.node.services.config.NotaryConfig
import net.corda.node.services.config.configureDevKeyAndTrustStores
import net.corda.nodeapi.internal.DEV_CA_KEY_STORE_PASS
import net.corda.nodeapi.internal.DEV_INTERMEDIATE_CA
import net.corda.nodeapi.internal.DEV_ROOT_CA
import net.corda.nodeapi.internal.crypto.CertificateAndKeyPair
import net.corda.nodeapi.internal.crypto.CertificateType
import net.corda.nodeapi.internal.crypto.X509Utilities
import net.corda.nodeapi.internal.crypto.X509Utilities.CORDA_CLIENT_CA
import net.corda.nodeapi.internal.crypto.X509Utilities.CORDA_CLIENT_TLS
import net.corda.nodeapi.internal.crypto.X509Utilities.CORDA_ROOT_CA
import net.corda.nodeapi.internal.crypto.X509Utilities.DISTRIBUTED_NOTARY_COMPOSITE_KEY_ALIAS
import net.corda.nodeapi.internal.crypto.X509Utilities.DISTRIBUTED_NOTARY_KEY_ALIAS
import net.corda.nodeapi.internal.crypto.X509Utilities.NODE_IDENTITY_KEY_ALIAS
import net.corda.nodeapi.internal.cryptoservice.CryptoService
import net.corda.nodeapi.internal.cryptoservice.bouncycastle.BCCryptoService
import net.corda.testing.core.ALICE_NAME
import net.corda.testing.core.BOB_NAME
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.Before
import org.junit.Ignore
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.security.KeyPair
import java.security.PublicKey

class KeyStoreHandlerTest {
    @Rule
    @JvmField
    val tempFolder = TemporaryFolder()

    private val certificateDir get() = tempFolder.root.toPath() / "certificates"

    private val config = rigorousMock<NodeConfiguration>()

    private val keyStore get() = config.signingCertificateStore.get()

    private lateinit var cryptoService: BCCryptoService

    private lateinit var keyStoreHandler: KeyStoreHandler

    @Before
    fun before() {
        val signingCertificateStore = CertificateStoreStubs.Signing.withCertificatesDirectory(certificateDir)
        val p2pSslOptions = CertificateStoreStubs.P2P.withCertificatesDirectory(certificateDir)
        p2pSslOptions.configureDevKeyAndTrustStores(ALICE_NAME, signingCertificateStore, certificateDir)

        config.also {
            doReturn(false).whenever(it).devMode
            doReturn(signingCertificateStore).whenever(it).signingCertificateStore
            doReturn(p2pSslOptions).whenever(it).p2pSslOptions
            doReturn(ALICE_NAME).whenever(it).myLegalName
            doReturn(null).whenever(it).notary
            doReturn(certificateDir).whenever(it).certificatesDirectory
        }
        cryptoService = BCCryptoService(ALICE_NAME.x500Principal, signingCertificateStore)
        keyStoreHandler = KeyStoreHandler(config, cryptoService)
    }

    @Ignore("Keystore is created on-demand with membership groups as a temporary solution")
    @Test(timeout = 300_000)
    fun `missing node keystore`() {
        val signingCertificateStore = CertificateStoreStubs.Signing.withCertificatesDirectory(certificateDir,
                certificateStoreFileName = "invalid.jks")
        doReturn(signingCertificateStore).whenever(config).signingCertificateStore

        assertThatThrownBy {
            keyStoreHandler.init()
        }.hasMessageContaining("One or more keyStores (identity or TLS) or trustStore not found.")
    }

    @Ignore("Keystore is created on-demand with membership groups as a temporary solution")
    @Test(timeout = 300_000)
    fun `missing truststore`() {
        val p2pSslOptions = CertificateStoreStubs.P2P.withCertificatesDirectory(certificateDir, trustStoreFileName = "invalid.jks")
        doReturn(p2pSslOptions).whenever(config).p2pSslOptions

        assertThatThrownBy {
            keyStoreHandler.init()
        }.hasMessageContaining("One or more keyStores (identity or TLS) or trustStore not found.")
    }

    @Ignore("Keystore is created on-demand with membership groups as a temporary solution")
    @Test(timeout = 300_000)
    fun `missing TLS keystore`() {
        val p2pSslOptions = CertificateStoreStubs.P2P.withCertificatesDirectory(certificateDir, keyStoreFileName = "invalid.jks")
        doReturn(p2pSslOptions).whenever(config).p2pSslOptions

        assertThatThrownBy {
            keyStoreHandler.init()
        }.hasMessageContaining("One or more keyStores (identity or TLS) or trustStore not found.")
    }

    @Test(timeout = 300_000)
    fun `invalid node keystore password`() {
        val signingCertificateStore = CertificateStoreStubs.Signing.withCertificatesDirectory(certificateDir, password = "invalid")
        doReturn(signingCertificateStore).whenever(config).signingCertificateStore

        assertThatThrownBy {
            keyStoreHandler.init()
        }.hasMessageContaining("At least one of the keystores or truststore passwords does not match configuration")
    }

    @Test(timeout = 300_000)
    fun `invalid truststore password`() {
        val p2pSslOptions = CertificateStoreStubs.P2P.withCertificatesDirectory(certificateDir, trustStorePassword = "invalid")
        doReturn(p2pSslOptions).whenever(config).p2pSslOptions

        assertThatThrownBy {
            keyStoreHandler.init()
        }.hasMessageContaining("At least one of the keystores or truststore passwords does not match configuration")
    }

    @Test(timeout = 300_000)
    fun `invalid TLS keystore password`() {
        val p2pSslOptions = CertificateStoreStubs.P2P.withCertificatesDirectory(certificateDir, keyStorePassword = "invalid")
        doReturn(p2pSslOptions).whenever(config).p2pSslOptions

        assertThatThrownBy {
            keyStoreHandler.init()
        }.hasMessageContaining("At least one of the keystores or truststore passwords does not match configuration")
    }

    @Test(timeout = 300_000)
    fun `missing trusted root in a truststore`() {
        config.p2pSslOptions.trustStore.get().update {
            internal.deleteEntry(CORDA_ROOT_CA)
        }

        assertThatThrownBy {
            keyStoreHandler.init()
        }.hasMessageContaining("Alias for trustRoot key not found. Please ensure you have an updated trustStore file")
    }

    @Test(timeout = 300_000)
    fun `missing TLS alias`() {
        config.p2pSslOptions.keyStore.get().update {
            internal.deleteEntry(CORDA_CLIENT_TLS)
        }

        assertThatThrownBy {
            keyStoreHandler.init()
        }.hasMessageContaining("Alias for TLS key not found. Please ensure you have an updated TLS keyStore file")
    }

    @Test(timeout = 300_000)
    fun `load TLS certificate with untrusted root`() {
        val keyPair = Crypto.generateKeyPair()
        val tlsKeyPair = Crypto.generateKeyPair()
        val untrustedRoot = X509Utilities.createSelfSignedCACertificate(ALICE_NAME.x500Principal, keyPair)
        val tlsCert = X509Utilities.createCertificate(CertificateType.TLS, untrustedRoot, keyPair, ALICE_NAME.x500Principal,
                tlsKeyPair.public)

        config.p2pSslOptions.keyStore.get().update {
            setPrivateKey(CORDA_CLIENT_TLS, tlsKeyPair.private, listOf(tlsCert, untrustedRoot), config.p2pSslOptions.keyStore.entryPassword)
        }

        assertThatThrownBy {
            keyStoreHandler.init()
        }.hasMessageContaining("TLS certificate must chain to the trusted root")
    }

    @Test(timeout = 300_000)
    fun `valid trust root is returned`() {
        val expectedRoot = config.p2pSslOptions.trustStore.get()[CORDA_ROOT_CA]
        val actualRoot = keyStoreHandler.init().first()

        assertThat(actualRoot).isEqualTo(expectedRoot)
    }

    @Test(timeout = 300_000)
    fun `valid multiple trust roots are returned`() {
        val trustStore = config.p2pSslOptions.trustStore.get()
        trustStore["$CORDA_ROOT_CA-2"] = X509Utilities.createSelfSignedCACertificate(ALICE_NAME.x500Principal, Crypto.generateKeyPair())
        trustStore["non-root"] = X509Utilities.createSelfSignedCACertificate(BOB_NAME.x500Principal, Crypto.generateKeyPair())

        assertThat(keyStoreHandler.init()).containsExactlyInAnyOrder(trustStore[CORDA_ROOT_CA], trustStore["$CORDA_ROOT_CA-2"])
    }

    @Test(timeout = 300_000)
    fun `keystore creation in dev mode`() {
        val devCertificateDir = tempFolder.root.toPath() / "certificates-dev"
        val signingCertificateStore = CertificateStoreStubs.Signing.withCertificatesDirectory(devCertificateDir)
        val p2pSslOptions = CertificateStoreStubs.P2P.withCertificatesDirectory(devCertificateDir)
        val devCryptoService = BCCryptoService(config.myLegalName.x500Principal, signingCertificateStore)

        doReturn(true).whenever(config).devMode
        doReturn(signingCertificateStore).whenever(config).signingCertificateStore
        doReturn(p2pSslOptions).whenever(config).p2pSslOptions
        doReturn(devCertificateDir).whenever(config).certificatesDirectory

        assertThat(devCryptoService.containsKey(NODE_IDENTITY_KEY_ALIAS)).isFalse()

        KeyStoreHandler(config, devCryptoService).init()

        assertThat(config.p2pSslOptions.trustStore.get().contains(CORDA_ROOT_CA)).isTrue()
        assertThat(config.p2pSslOptions.keyStore.get().contains(CORDA_CLIENT_TLS)).isTrue()
        assertThat(config.signingCertificateStore.get().contains(NODE_IDENTITY_KEY_ALIAS)).isTrue()
        assertThat(devCryptoService.containsKey(NODE_IDENTITY_KEY_ALIAS)).isTrue()
    }

    @Test(timeout = 300_000)
    fun `load node identity`() {
        keyStoreHandler.init()

        val certificate = keyStore[NODE_IDENTITY_KEY_ALIAS]
        assertThat(keyStoreHandler.nodeIdentity.certificate).isEqualTo(certificate)
        assertThat(keyStoreHandler.notaryIdentity).isNull()
        assertThat(keyStoreHandler.signingKeys).containsExactly(KeyAndAlias(certificate.publicKey, NODE_IDENTITY_KEY_ALIAS))
    }

    @Test(timeout = 300_000)
    fun `load node identity without node CA`() {
        assertThat(keyStore[CORDA_CLIENT_CA]).isNotNull
        keyStore.update { internal.deleteEntry(CORDA_CLIENT_CA) }

        keyStoreHandler.init()

        val certificate = keyStore[NODE_IDENTITY_KEY_ALIAS]
        assertThat(keyStoreHandler.nodeIdentity.certificate).isEqualTo(certificate)
        assertThat(keyStoreHandler.notaryIdentity).isNull()
        assertThat(keyStoreHandler.signingKeys).containsExactly(KeyAndAlias(certificate.publicKey, NODE_IDENTITY_KEY_ALIAS))
    }

    @Test(timeout = 300_000)
    fun `load node identity with missing alias`() {
        keyStore.update { internal.deleteEntry(NODE_IDENTITY_KEY_ALIAS) }

        assertThatThrownBy {
            keyStoreHandler.init()
        }.hasMessageContaining("node identity key is not in the keyStore file")
    }

    @Test(timeout = 300_000)
    fun `load node identity with missing key in CryptoService`() {
        val cryptoServiceMock = rigorousMock<CryptoService>()
        doReturn(false).whenever(cryptoServiceMock).containsKey(NODE_IDENTITY_KEY_ALIAS)
        keyStoreHandler = KeyStoreHandler(config, cryptoServiceMock)

        assertThatThrownBy {
            keyStoreHandler.init()
        }.hasMessageContaining("Key for node identity alias '$NODE_IDENTITY_KEY_ALIAS' not found in CryptoService")
    }

    @Test(timeout = 300_000)
    fun `load node identity with untrusted root`() {
        val untrustedRoot = X509Utilities.createSelfSignedCACertificate(ALICE_NAME.x500Principal, Crypto.generateKeyPair())

        keyStore.update {
            val privateKey = getPrivateKey(NODE_IDENTITY_KEY_ALIAS, DEV_CA_KEY_STORE_PASS)
            val certificates = getCertificateChain(NODE_IDENTITY_KEY_ALIAS)
            setPrivateKey(NODE_IDENTITY_KEY_ALIAS, privateKey, certificates.dropLast(1) + untrustedRoot, DEV_CA_KEY_STORE_PASS)
        }

        assertThatThrownBy {
            keyStoreHandler.init()
        }.hasMessageContaining("Certificate for node identity must chain to the trusted root")
    }

    @Test(timeout = 300_000)
    fun `load node identity with wrong legal name`() {
        doReturn(BOB_NAME).whenever(config).myLegalName

        assertThatThrownBy {
            keyStoreHandler.init()
        }.hasMessageContaining("The configured legalName").hasMessageContaining("doesn't match what's in the key store")
    }

    @Test(timeout = 300_000)
    fun `load node identity with wrong certificate path`() {
        keyStore.update {
            val privateKey = getPrivateKey(NODE_IDENTITY_KEY_ALIAS, DEV_CA_KEY_STORE_PASS)
            val certificates = getCertificateChain(NODE_IDENTITY_KEY_ALIAS)
            setPrivateKey(NODE_IDENTITY_KEY_ALIAS, privateKey, certificates.take(1) + certificates.drop(2), DEV_CA_KEY_STORE_PASS)
        }

        assertThatThrownBy {
            keyStoreHandler.init()
        }.hasMessageContaining("Cert path failed to validate")
    }

    @Test(timeout = 300_000)
    fun `load old style notary identity`() {
        val notaryConfig = rigorousMock<NotaryConfig>()
        doReturn(null).whenever(notaryConfig).serviceLegalName
        doReturn(notaryConfig).whenever(config).notary

        keyStoreHandler.init()

        val certificate = keyStore[NODE_IDENTITY_KEY_ALIAS]
        assertThat(keyStoreHandler.nodeIdentity.certificate).isEqualTo(certificate)
        assertThat(keyStoreHandler.notaryIdentity).isNotNull
        assertThat(keyStoreHandler.notaryIdentity!!.certificate).isEqualTo(certificate)
        assertThat(keyStoreHandler.signingKeys).containsExactly(KeyAndAlias(certificate.publicKey, NODE_IDENTITY_KEY_ALIAS))
    }

    private fun createNotaryCertificate(publicKey: PublicKey, name: CordaX500Name) = X509Utilities.createCertificate(
            CertificateType.SERVICE_IDENTITY,
            DEV_INTERMEDIATE_CA.certificate,
            DEV_INTERMEDIATE_CA.keyPair,
            name.x500Principal,
            publicKey)

    private fun generateIdentity(alias: String, name: CordaX500Name, type: CertificateType, parentAlias: String? = null): PublicKey {
        val keyPair = Crypto.generateKeyPair()
        val (parent, chain) = if (parentAlias != null) {
            keyStore.query {
                val parentCert = getCertificate(parentAlias)
                val parentKey = getPrivateKey(parentAlias, DEV_CA_KEY_STORE_PASS)
                CertificateAndKeyPair(parentCert, KeyPair(parentCert.publicKey, parentKey)) to getCertificateChain(parentAlias)
            }
        } else {
            DEV_INTERMEDIATE_CA to listOf(DEV_INTERMEDIATE_CA.certificate, DEV_ROOT_CA.certificate)
        }
        val certificate = X509Utilities.createCertificate(type, parent.certificate, parent.keyPair, name.x500Principal, keyPair.public)
        keyStore.update {
            setPrivateKey(alias, keyPair.private, listOf(certificate) + chain, DEV_CA_KEY_STORE_PASS)
        }
        cryptoService.resyncKeystore()
        return keyPair.public
    }

    @Test(timeout = 300_000)
    fun `load notary identity`() {
        val notaryConfig = rigorousMock<NotaryConfig>()
        doReturn(BOB_NAME).whenever(notaryConfig).serviceLegalName
        doReturn(notaryConfig).whenever(config).notary

        generateIdentity(DISTRIBUTED_NOTARY_KEY_ALIAS, BOB_NAME, CertificateType.SERVICE_IDENTITY)

        keyStoreHandler.init()

        val nodeCert = keyStore[NODE_IDENTITY_KEY_ALIAS]
        val notaryCert = keyStore[DISTRIBUTED_NOTARY_KEY_ALIAS]
        assertThat(keyStoreHandler.nodeIdentity.certificate).isEqualTo(nodeCert)
        assertThat(keyStoreHandler.notaryIdentity).isNotNull
        assertThat(keyStoreHandler.notaryIdentity!!.certificate).isEqualTo(notaryCert)
        assertThat(keyStoreHandler.signingKeys).containsExactly(
                KeyAndAlias(nodeCert.publicKey, NODE_IDENTITY_KEY_ALIAS),
                KeyAndAlias(notaryCert.publicKey, DISTRIBUTED_NOTARY_KEY_ALIAS)
        )
    }

    @Test(timeout = 300_000)
    fun `load notary identity with wrong legal name`() {
        val notaryConfig = rigorousMock<NotaryConfig>()
        doReturn(BOB_NAME).whenever(notaryConfig).serviceLegalName
        doReturn(notaryConfig).whenever(config).notary

        generateIdentity(DISTRIBUTED_NOTARY_KEY_ALIAS, ALICE_NAME, CertificateType.SERVICE_IDENTITY)

        assertThatThrownBy {
            keyStoreHandler.init()
        }.hasMessageContaining("The configured legalName").hasMessageContaining("doesn't match what's in the key store")
    }

    @Test(timeout = 300_000)
    fun `load notary composite identity`() {
        val notaryConfig = rigorousMock<NotaryConfig>()
        doReturn(BOB_NAME).whenever(notaryConfig).serviceLegalName
        doReturn(notaryConfig).whenever(config).notary

        val notaryKey = generateIdentity(DISTRIBUTED_NOTARY_KEY_ALIAS, BOB_NAME, CertificateType.SERVICE_IDENTITY)
        val compositeKey = CompositeKey.Builder().addKey(notaryKey).build()
        keyStore[DISTRIBUTED_NOTARY_COMPOSITE_KEY_ALIAS] = createNotaryCertificate(compositeKey, BOB_NAME)

        keyStoreHandler.init()

        val nodeCert = keyStore[NODE_IDENTITY_KEY_ALIAS]
        assertThat(keyStoreHandler.nodeIdentity.certificate).isEqualTo(nodeCert)
        assertThat(keyStoreHandler.notaryIdentity).isNotNull
        assertThat(keyStoreHandler.notaryIdentity!!.certificate).isEqualTo(keyStore[DISTRIBUTED_NOTARY_COMPOSITE_KEY_ALIAS])
        assertThat(keyStoreHandler.signingKeys).containsExactly(
                KeyAndAlias(nodeCert.publicKey, NODE_IDENTITY_KEY_ALIAS),
                KeyAndAlias(notaryKey, DISTRIBUTED_NOTARY_KEY_ALIAS)
        )
    }

    @Test(timeout = 300_000)
    fun `load notary composite identity with wrong legal name`() {
        val notaryConfig = rigorousMock<NotaryConfig>()
        doReturn(BOB_NAME).whenever(notaryConfig).serviceLegalName
        doReturn(notaryConfig).whenever(config).notary

        val notaryKey = generateIdentity(DISTRIBUTED_NOTARY_KEY_ALIAS, BOB_NAME, CertificateType.SERVICE_IDENTITY)
        val compositeKey = CompositeKey.Builder().addKey(notaryKey).build()
        keyStore[DISTRIBUTED_NOTARY_COMPOSITE_KEY_ALIAS] = createNotaryCertificate(compositeKey, ALICE_NAME)

        assertThatThrownBy {
            keyStoreHandler.init()
        }.hasMessageContaining("The configured legalName").hasMessageContaining("doesn't match what's in the key store")
    }
}