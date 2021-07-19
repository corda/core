package net.corda.node.internal

import net.corda.core.crypto.Crypto
import net.corda.core.crypto.toStringShort
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.internal.uncheckedCast
import net.corda.core.node.services.KeyManagementService
import net.corda.core.utilities.contextLogger
import net.corda.node.services.config.NodeConfiguration
import net.corda.node.services.config.configureWithDevSSLCertificate
import net.corda.node.services.network.MGM_KEY_ENTROPY
import net.corda.node.services.network.MGM_NAME
import net.corda.nodeapi.internal.config.CertificateStore
import net.corda.nodeapi.internal.crypto.X509Utilities
import net.corda.nodeapi.internal.crypto.X509Utilities.CORDA_CLIENT_TLS
import net.corda.nodeapi.internal.crypto.X509Utilities.DISTRIBUTED_NOTARY_COMPOSITE_KEY_ALIAS
import net.corda.nodeapi.internal.crypto.X509Utilities.DISTRIBUTED_NOTARY_KEY_ALIAS
import net.corda.nodeapi.internal.crypto.X509Utilities.NODE_IDENTITY_KEY_ALIAS
import net.corda.nodeapi.internal.crypto.checkValidity
import net.corda.nodeapi.internal.cryptoservice.CryptoService
import net.corda.nodeapi.internal.cryptoservice.bouncycastle.BCCryptoService
import java.io.IOException
import java.math.BigInteger
import java.nio.file.NoSuchFileException
import java.security.GeneralSecurityException
import java.security.PublicKey
import java.security.cert.X509Certificate
import java.util.Collections.unmodifiableSet

data class KeyAndAlias(val key: PublicKey, val alias: String)

class KeyStoreHandler(private val configuration: NodeConfiguration, private val cryptoService: CryptoService) {
    companion object {
        private val log = contextLogger()
    }

    private lateinit var _nodeIdentity: PartyAndCertificate
    val nodeIdentity: PartyAndCertificate get() = _nodeIdentity

    private var _notaryIdentity: PartyAndCertificate? = null
    val notaryIdentity: PartyAndCertificate? get() = _notaryIdentity

    private val _signingKeys: MutableSet<KeyAndAlias> = mutableSetOf()
    val signingKeys: Set<KeyAndAlias> get() = _signingKeys.toSet()

    private lateinit var _mgmParty: Party
    val mgmParty: Party get() = _mgmParty

    private lateinit var trustRoots: Set<X509Certificate>

    private lateinit var nodeKeyStore: CertificateStore

    /**
     * Initialize key-stores and load identities.
     * @param devModeKeyEntropy entropy for legal identity key derivation
     * @return trust root certificates
     */
    fun init(devModeKeyEntropy: BigInteger? = null): Set<X509Certificate> {
        val mgmKey = Crypto.deriveKeyPairFromEntropy(Crypto.DEFAULT_SIGNATURE_SCHEME, MGM_KEY_ENTROPY)
        _mgmParty = Party(MGM_NAME, mgmKey.public)

        val entropy = MGM_KEY_ENTROPY.takeIf { configuration.myLegalName == MGM_NAME } ?: devModeKeyEntropy
        val certStores = withExceptionHandling {
            // Creating keystores with dev certificates if they are not explicitly provided.
            configuration.configureWithDevSSLCertificate(cryptoService, entropy)
            // configureWithDevSSLCertificate writes directly to keystore files, so
            // we should re-synchronise BCCryptoService with the updated keystore file.
            if (cryptoService is BCCryptoService) {
                cryptoService.resyncKeystore()
            }
            getCertificateStores()
        }
        trustRoots = validateKeyStores(certStores)
        nodeKeyStore = certStores.nodeKeyStore
        loadIdentities()
        return unmodifiableSet(trustRoots)
    }

    @Suppress("ThrowsCount")
    inline fun <T> withExceptionHandling(block: () -> T): T {
        return try {
            block()
        } catch (e: IOException) {
            when {
                e is NoSuchFileException -> throw IllegalArgumentException(
                        "One or more keyStores (identity or TLS) or trustStore not found. " +
                                "Please either copy your existing keys and certificates from another node, " +
                                "or if you don't have one yet, fill out the config file and run corda.jar initial-registration.", e)
                e.cause is GeneralSecurityException -> throw IllegalArgumentException(
                        "At least one of the keystores or truststore passwords does not match configuration.", e)
                else -> throw e
            }
        }
    }

    private data class AllCertificateStores(val trustStore: CertificateStore,
                                            val sslKeyStore: CertificateStore,
                                            val nodeKeyStore: CertificateStore)

    private fun getCertificateStores(): AllCertificateStores {
        val sslKeyStore = configuration.p2pSslOptions.keyStore.get()
        val nodeKeyStore = configuration.signingCertificateStore.get()
        val trustStore = configuration.p2pSslOptions.trustStore.get()
        return AllCertificateStores(trustStore, sslKeyStore, nodeKeyStore)
    }

    private fun validateKeyStores(certStores: AllCertificateStores): Set<X509Certificate> {
        // Check that trustStore contains the correct key-alias entry.
        val trustRoots = certStores.trustStore.filter { it.first.startsWith(X509Utilities.CORDA_ROOT_CA) }.map {
            log.info("Loaded trusted root certificate: ${it.second.publicKey.toStringShort()}, alias: ${it.first}")
            it.second
        }.toSet()
        require(trustRoots.isNotEmpty()) {
            "Alias for trustRoot key not found. Please ensure you have an updated trustStore file."
        }

        certStores.sslKeyStore.let {
            val tlsKeyAlias = CORDA_CLIENT_TLS

            // Check that TLS keyStore contains the correct key-alias entry.
            require(tlsKeyAlias in it) {
                "Alias for TLS key not found. Please ensure you have an updated TLS keyStore file."
            }

            // Check TLS certificate validity and print warning for expiry within next 30 days.
            it[tlsKeyAlias].checkValidity({
                "TLS certificate for alias '$tlsKeyAlias' is expired."
            }, { daysToExpiry ->
                log.warn("TLS certificate for alias '$tlsKeyAlias' will expire in $daysToExpiry day(s).")
            })

            // Check TLS cert path chains to the trusted root.
            val sslCertChainRoot = it.query { getCertificateChain(tlsKeyAlias) }.last()
            require(sslCertChainRoot in trustRoots) { "TLS certificate must chain to the trusted root." }
        }

        return trustRoots
    }

    /**
     * Loads node legal identity and notary service identity.
     */
    private fun loadIdentities() {
        val identityKeyAlias = NODE_IDENTITY_KEY_ALIAS

        _nodeIdentity = loadIdentity(identityKeyAlias, configuration.myLegalName)
        _notaryIdentity = configuration.notary?.let {
            loadNotaryIdentity(it.serviceLegalName, _nodeIdentity)
        }
    }

    /**
     * Load key from CryptoService, so it can be used by KeyManagementService.
     */
    private fun loadKeyFromCryptoService(alias: String) {
        check(cryptoService.containsKey(alias)) {
            "Key for node identity alias '$alias' not found in CryptoService."
        }
        val key = cryptoService.getPublicKey(alias)!!
        log.info("Loaded node identity key: ${key.toStringShort()}, alias: $alias")
        _signingKeys.add(KeyAndAlias(key, alias))
    }

    /**
     * Loads the node's legal identity (or notary's service identity) certificate, public key and alias.
     *
     * If identity certificate has been renewed, the result will also contain previous public keys and aliases,
     * so they can still be used by [KeyManagementService] for signing.
     */
    private fun loadIdentity(alias: String, legalName: CordaX500Name): PartyAndCertificate {
        require(alias in nodeKeyStore) {
            "Alias '$alias' for node identity key is not in the keyStore file."
        }
        loadKeyFromCryptoService(alias)

        val certificate = nodeKeyStore.query { getCertificate(alias) }
        val certificates = nodeKeyStore.query { getCertificateChain(alias) }
        check(certificates.first() == certificate) {
            "Certificates from key store do not line up!"
        }
        check(certificates.last() in trustRoots) {
            "Certificate for node identity must chain to the trusted root."
        }

        val subject = CordaX500Name.build(certificates.first().subjectX500Principal)
        check(subject == legalName) {
            "The configured legalName '$legalName' doesn't match what's in the key store: $subject"
        }

        val identity = PartyAndCertificate(X509Utilities.buildCertPath(certificates))
        X509Utilities.validateCertPath(trustRoots, identity.certPath)
        return identity
    }

    /**
     * Loads notary service identity. In the case of the experimental RAFT and BFT notary clusters, this loads the pre-generated
     * cluster identity that all worker nodes share. In the case of a simple single notary, this loads the notary service identity
     * that is generated during initial registration and is used to sign notarisation requests.
     **/
    private fun loadNotaryIdentity(serviceLegalName: CordaX500Name?, nodeIdentity: PartyAndCertificate): PartyAndCertificate {
        val notaryKeyAlias = DISTRIBUTED_NOTARY_KEY_ALIAS
        val notaryCompositeKeyAlias = DISTRIBUTED_NOTARY_COMPOSITE_KEY_ALIAS

        if (serviceLegalName == null) {
            // The only case where the notaryIdentity will be the node's legal identity is for existing single notary services running
            // an older version. Current single notary services (V4.6+) sign requests using a separate notary service identity so the
            // notary identity will be different from the node's legal identity.

            // This check is here to ensure that a user does not accidentally/intentionally remove the serviceLegalName configuration
            // parameter after a notary has been registered. If that was possible then notary would start and sign incoming requests
            // with the node's legal identity key, corrupting the data.
            check(!nodeKeyStore.contains(notaryKeyAlias)) {
                "The notary service key exists in the key store but no notary service legal name has been configured. " +
                        "Either include the relevant 'notary.serviceLegalName' configuration or validate this key is not necessary " +
                        "and remove from the key store."
            }
            return nodeIdentity
        }

        // First load notary service identity that is generated during initial registration, then lookup for a composite identity.
        // If alias for composite key does not exist, we assume the notary is CFT, and each cluster member shares the same notary key pair.
        val serviceIdentity = loadIdentity(notaryKeyAlias, serviceLegalName)
        if (notaryCompositeKeyAlias in nodeKeyStore) {
            return loadCompositeIdentity(notaryCompositeKeyAlias, serviceLegalName, serviceIdentity)
        }
        return serviceIdentity
    }

    /**
     * Loads composite identity certificate for the provided [alias]. Composite identity can be stored as a certificate-only entry
     * without associated signing key. Certificate chain is copied from the [baseIdentity].
     **/
    private fun loadCompositeIdentity(alias: String, legalName: CordaX500Name, baseIdentity: PartyAndCertificate): PartyAndCertificate {
        val certificate = nodeKeyStore.query { getCertificate(alias) }
        val subject = CordaX500Name.build(certificate.subjectX500Principal)
        check(subject == legalName) {
            "The configured legalName '$legalName' doesn't match what's in the key store: $subject"
        }

        // We have to create the certificate chain for the composite key manually, this is because we don't have a keystore
        // provider that understand compositeKey-privateKey combo. The cert chain is created using the composite key certificate +
        // the tail of the private key certificates, as they are both signed by the same certificate chain.
        val certificates: List<X509Certificate> = uncheckedCast(listOf(certificate) + baseIdentity.certPath.certificates.drop(1))

        val identity = PartyAndCertificate(X509Utilities.buildCertPath(certificates))
        X509Utilities.validateCertPath(trustRoots, identity.certPath)
        return identity
    }
}