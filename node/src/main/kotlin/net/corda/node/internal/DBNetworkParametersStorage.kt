package net.corda.node.internal

import net.corda.core.crypto.SecureHash
import net.corda.core.identity.Party
import net.corda.core.internal.DigitalSignatureWithCert
import net.corda.core.internal.NamedCacheFactory
import net.corda.core.internal.NetworkParametersStorage
import net.corda.core.internal.SignedDataWithCert
import net.corda.core.node.NetworkParameters
import net.corda.core.node.NotaryInfo
import net.corda.core.serialization.SerializedBytes
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.serialization.deserialize
import net.corda.core.utilities.MAX_HASH_HEX_SIZE
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.trace
import net.corda.node.services.network.NetworkMapClient
import net.corda.node.utilities.AppendOnlyPersistentMap
import net.corda.nodeapi.internal.crypto.X509CertificateFactory
import net.corda.nodeapi.internal.crypto.X509Utilities
import net.corda.nodeapi.internal.network.SignedNetworkParameters
import net.corda.nodeapi.internal.network.verifiedNetworkParametersCert
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.nodeapi.internal.persistence.NODE_DATABASE_PREFIX
import org.apache.commons.lang3.ArrayUtils
import java.security.cert.X509Certificate
import javax.persistence.*

class DBNetworkParametersStorage(
        cacheFactory: NamedCacheFactory,
        private val database: CordaPersistence,
        // TODO It's very inefficient solution (at least at the beginning when node joins without historical data)
        // We could have historic parameters endpoint or always add parameters as an attachment to the transaction.
        private val networkMapClient: NetworkMapClient?
) : NetworkParametersStorage, SingletonSerializeAsToken() {
    private lateinit var trustRoot: X509Certificate

    companion object {
        private val log = contextLogger()

        fun createParametersMap(cacheFactory: NamedCacheFactory): AppendOnlyPersistentMap<SecureHash, SignedDataWithCert<NetworkParameters>, PersistentNetworkParameters, String> {
            return AppendOnlyPersistentMap(
                    cacheFactory = cacheFactory,
                    name = "NodeParametersStorage_networkParametersByHash",
                    toPersistentEntityKey = { it.toString() },
                    fromPersistentEntity = {
                        Pair(
                                SecureHash.create(it.hash),
                                it.signedNetworkParameters
                        )
                    },
                    toPersistentEntity = { key: SecureHash, value: SignedDataWithCert<NetworkParameters> ->
                        PersistentNetworkParameters(key.toString(), value.verified().epoch, value.raw.bytes, value.sig.bytes, value.sig.by.encoded,
                                X509Utilities.buildCertPath(value.sig.parentCertsChain).encoded)
                    },
                    persistentEntityClass = PersistentNetworkParameters::class.java
            )
        }
    }

    override fun setCurrentParameters(currentSignedParameters: SignedDataWithCert<NetworkParameters>, trustRoot: X509Certificate) {
        this.trustRoot = trustRoot
        saveParameters(currentSignedParameters)
        _currentHash = currentSignedParameters.raw.hash
    }

    private lateinit var _currentHash: SecureHash
    override val currentHash: SecureHash get() = _currentHash
    // TODO Have network map serve special "starting" parameters as parameters for resolution for older transactions?
    override val defaultHash: SecureHash get() = currentHash

    private val hashToParameters = createParametersMap(cacheFactory)

    override fun lookup(hash: SecureHash): NetworkParameters? {
        return database.transaction { hashToParameters[hash]?.raw?.deserialize() } ?: tryDownloadUnknownParameters(hash)
    }

    override fun getEpochFromHash(hash: SecureHash): Int? = lookup(hash)?.epoch

    override fun lookupSigned(hash: SecureHash): SignedDataWithCert<NetworkParameters>? {
        return database.transaction { hashToParameters[hash] }
    }

    override fun hasParameters(hash: SecureHash): Boolean = hash in hashToParameters

    override fun saveParameters(signedNetworkParameters: SignedNetworkParameters) {
        log.trace { "Saving new network parameters to network parameters storage." }
        val networkParameters = signedNetworkParameters.verifiedNetworkParametersCert(trustRoot)
        val hash = signedNetworkParameters.raw.hash
        log.trace { "Parameters to save $networkParameters with hash $hash" }
        database.transaction {
            hashToParameters.addWithDuplicatesAllowed(hash, signedNetworkParameters, false)
        }
    }

    private fun tryDownloadUnknownParameters(parametersHash: SecureHash): NetworkParameters? {
        return if (networkMapClient != null) {
            try {
                val signedParams = networkMapClient.getNetworkParameters(parametersHash)
                val networkParameters = signedParams.verifiedNetworkParametersCert(trustRoot)
                saveParameters(signedParams)
                networkParameters
            } catch (e: Exception) {
                log.warn("Failed to download historical network parameters with hash $parametersHash", e)
                null
            }
        } else {
            log.warn("Tried to download historical network parameters with hash $parametersHash, but network map url isn't configured")
            null
        }
    }

    /**
     * Try to obtain notary info from the current network parameters. If not found, look through historical ones.
     */
    override fun getHistoricNotary(party: Party): NotaryInfo? {
        val currentParameters = lookup(currentHash)
                ?: throw IllegalStateException("Unable to obtain NotaryInfo – current network parameters not set.")
        val inCurrentParams = currentParameters.notaries.singleOrNull { it.identity == party }
        if (inCurrentParams != null) return inCurrentParams
        return hashToParameters.allPersisted.use {
            it.flatMap { (_, signedNetParams) -> signedNetParams.raw.deserialize().notaries.stream() }
                    .filter { it.identity == party }
                    .findFirst()
                    .orElse(null)
        }
    }

    @Entity
    @Table(name = "${NODE_DATABASE_PREFIX}network_parameters")
    class PersistentNetworkParameters(
            @Id
            @Column(name = "hash", length = MAX_HASH_HEX_SIZE, nullable = false)
            val hash: String = "",

            @Column(name = "epoch", nullable = false)
            val epoch: Int = 0,

            // Stored as serialized bytes because network parameters structure evolves over time.
            @Lob
            @Column(name = "parameters_bytes", nullable = false)
            val networkParametersBytes: ByteArray = ArrayUtils.EMPTY_BYTE_ARRAY,

            @Lob
            @Column(name = "signature_bytes", nullable = false)
            private val signature: ByteArray = ArrayUtils.EMPTY_BYTE_ARRAY,

            // First certificate in the certificate chain.
            @Lob
            @Column(name = "cert", nullable = false)
            private val certificate: ByteArray = ArrayUtils.EMPTY_BYTE_ARRAY,

            // Parent certificate path (the first one is stored separately), so node is agnostic to certificate hierarchy.
            @Lob
            @Column(name = "parent_cert_path", nullable = false)
            private val certPath: ByteArray = ArrayUtils.EMPTY_BYTE_ARRAY
    ) {
        val networkParameters: NetworkParameters get() = networkParametersBytes.deserialize()
        val signedNetworkParameters: SignedDataWithCert<NetworkParameters>
            get() {
                val certChain = X509CertificateFactory().delegate.generateCertPath(certPath.inputStream())
                        .certificates.map { it as X509Certificate }
                val signWithCert = DigitalSignatureWithCert(X509CertificateFactory().generateCertificate(certificate.inputStream()), certChain, signature)
                return SignedDataWithCert(SerializedBytes(networkParametersBytes), signWithCert)
            }
    }
}
