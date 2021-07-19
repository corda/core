package net.corda.coretests.crypto

import net.corda.core.crypto.Crypto
import net.corda.core.identity.CordaX500Name
import net.corda.nodeapi.internal.crypto.CertificateType
import net.corda.nodeapi.internal.crypto.X509KeyStore
import net.corda.nodeapi.internal.crypto.X509Utilities
import net.corda.testing.internal.createDevIntermediateCaCertPath
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.GeneralName
import org.bouncycastle.asn1.x509.GeneralSubtree
import org.bouncycastle.asn1.x509.NameConstraints
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.Test
import java.security.UnrecoverableKeyException
import java.security.cert.CertPathValidator
import java.security.cert.CertPathValidatorException
import java.security.cert.PKIXParameters
import javax.security.auth.x500.X500Principal
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class X509NameConstraintsTest {

    companion object {
        private const val storePassword = "storePassword"
        private const val keyPassword = "entryPassword"
    }

    private fun makeKeyStores(subjectName: X500Name, nameConstraints: NameConstraints): Pair<X509KeyStore, X509KeyStore> {
        val (rootCa, intermediateCa) = createDevIntermediateCaCertPath()

        val trustStore = X509KeyStore(storePassword).apply {
            setCertificate(X509Utilities.CORDA_ROOT_CA, rootCa.certificate)
        }

        val keyStore = X509KeyStore(storePassword).apply {
            val nodeCaKeyPair = Crypto.generateKeyPair(X509Utilities.DEFAULT_TLS_SIGNATURE_SCHEME)
            val nodeCaCert = X509Utilities.createCertificate(
                    CertificateType.NODE_CA,
                    intermediateCa.certificate,
                    intermediateCa.keyPair,
                    CordaX500Name("Corda Client CA", "R3 Ltd", "London", "GB").x500Principal,
                    nodeCaKeyPair.public,
                    nameConstraints = nameConstraints)
            val tlsKeyPair = Crypto.generateKeyPair(X509Utilities.DEFAULT_TLS_SIGNATURE_SCHEME)
            val tlsCert = X509Utilities.createCertificate(
                    CertificateType.TLS,
                    nodeCaCert,
                    nodeCaKeyPair,
                    X500Principal(subjectName.encoded),
                    tlsKeyPair.public)
            setPrivateKey(X509Utilities.CORDA_CLIENT_TLS, tlsKeyPair.private, listOf(tlsCert, nodeCaCert, intermediateCa.certificate, rootCa.certificate), keyPassword)
        }

        return Pair(keyStore, trustStore)
    }

    @Test(timeout=300_000)
	fun `illegal common name`() {
        val acceptableNames = listOf("CN=Bank A TLS, O=Bank A", "CN=Bank A")
                .map { GeneralSubtree(GeneralName(X500Name(it))) }.toTypedArray()

        val nameConstraints = NameConstraints(acceptableNames, arrayOf())
        val pathValidator = CertPathValidator.getInstance("PKIX")

        assertFailsWith(CertPathValidatorException::class) {
            val (keystore, trustStore) = makeKeyStores(X500Name("CN=Bank B"), nameConstraints)
            val params = PKIXParameters(trustStore.internal)
            params.isRevocationEnabled = false
            val certPath = X509Utilities.buildCertPath(keystore.getCertificateChain(X509Utilities.CORDA_CLIENT_TLS))
            pathValidator.validate(certPath, params)
        }

        assertTrue {
            val (keystore, trustStore) = makeKeyStores(X500Name("CN=Bank A TLS, O=Bank A"), nameConstraints)
            val params = PKIXParameters(trustStore.internal)
            params.isRevocationEnabled = false
            val certPath = X509Utilities.buildCertPath(keystore.getCertificateChain(X509Utilities.CORDA_CLIENT_TLS))
            pathValidator.validate(certPath, params)
            true
        }

        assertTrue {
            val (keystore, trustStore) = makeKeyStores(X500Name("CN=Bank A"), nameConstraints)
            val params = PKIXParameters(trustStore.internal)
            params.isRevocationEnabled = false
            val certPath = X509Utilities.buildCertPath(keystore.getCertificateChain(X509Utilities.CORDA_CLIENT_TLS))
            pathValidator.validate(certPath, params)
            true
        }
    }

    @Test(timeout=300_000)
    fun `x500 name with correct cn and extra attribute`() {
        // Do not use Security.addProvider(BouncyCastleProvider()) to avoid EdDSA signature disruption in other tests.
        Crypto.findProvider(BouncyCastleProvider.PROVIDER_NAME)
        val acceptableNames = listOf("CN=Bank A TLS, UID=", "O=Bank A")
                .map { GeneralSubtree(GeneralName(X500Name(it))) }.toTypedArray()

        val nameConstraints = NameConstraints(acceptableNames, arrayOf())
        val pathValidator = CertPathValidator.getInstance("PKIX", BouncyCastleProvider.PROVIDER_NAME)

        assertFailsWith(CertPathValidatorException::class) {
            val (keystore, trustStore) = makeKeyStores(X500Name("CN=Bank A"), nameConstraints)
            val params = PKIXParameters(trustStore.internal)
            params.isRevocationEnabled = false
            val certPath = X509Utilities.buildCertPath(keystore.getCertificateChain(X509Utilities.CORDA_CLIENT_TLS))
            pathValidator.validate(certPath, params)
        }

        assertFailsWith(CertPathValidatorException::class) {
            val (keystore, trustStore) = makeKeyStores(X500Name("CN=Bank A, UID=12345"), nameConstraints)
            val params = PKIXParameters(trustStore.internal)
            params.isRevocationEnabled = false
            val certPath = X509Utilities.buildCertPath(keystore.getCertificateChain(X509Utilities.CORDA_CLIENT_TLS))
            pathValidator.validate(certPath, params)
        }

        assertTrue {
            val (keystore, trustStore) = makeKeyStores(X500Name("CN=Bank A TLS, UID=, E=me@email.com, C=GB"), nameConstraints)
            val params = PKIXParameters(trustStore.internal)
            params.isRevocationEnabled = false
            val certPath = X509Utilities.buildCertPath(keystore.getCertificateChain(X509Utilities.CORDA_CLIENT_TLS))
            pathValidator.validate(certPath, params)
            true
        }

        assertTrue {
            val (keystore, trustStore) = makeKeyStores(X500Name("O=Bank A, UID=, E=me@email.com, C=GB"), nameConstraints)
            val params = PKIXParameters(trustStore.internal)
            params.isRevocationEnabled = false
            val certPath = X509Utilities.buildCertPath(keystore.getCertificateChain(X509Utilities.CORDA_CLIENT_TLS))
            pathValidator.validate(certPath, params)
            true
        }
    }

    @Test(timeout=300_000)
	fun `test private key retrieval`() {
        val acceptableNames = listOf("CN=Bank A TLS, UID=", "O=Bank A")
                .map { GeneralSubtree(GeneralName(X500Name(it))) }.toTypedArray()

        val nameConstraints = NameConstraints(acceptableNames, arrayOf())
        val (keystore, _) = makeKeyStores(X500Name("CN=Bank A"), nameConstraints)

        val privateKey = keystore.getPrivateKey(X509Utilities.CORDA_CLIENT_TLS, keyPassword)
        assertEquals(X509Utilities.DEFAULT_TLS_SIGNATURE_SCHEME.algorithmName, privateKey.algorithm)
    }

    @Test
    fun `test private key retrieval with invalid key`() {
        val acceptableNames = listOf("CN=Bank A TLS, UID=", "O=Bank A")
                .map { GeneralSubtree(GeneralName(X500Name(it))) }.toTypedArray()

        val nameConstraints = NameConstraints(acceptableNames, arrayOf())
        val (keystore, _) = makeKeyStores(X500Name("CN=Bank A"), nameConstraints)

        assertFailsWith(UnrecoverableKeyException::class) {
            keystore.getPrivateKey(X509Utilities.CORDA_CLIENT_TLS, "gibberish")
        }
    }
}
