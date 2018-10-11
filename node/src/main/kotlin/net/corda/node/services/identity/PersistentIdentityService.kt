package net.corda.node.services.identity

import net.corda.core.crypto.SecureHash
import net.corda.core.identity.*
import net.corda.core.internal.hash
import net.corda.core.node.services.UnknownAnonymousPartyException
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.utilities.MAX_HASH_HEX_SIZE
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.debug
import net.corda.node.services.api.IdentityServiceInternal
import net.corda.node.utilities.AppendOnlyPersistentMap
import net.corda.node.utilities.NamedCacheFactory
import net.corda.nodeapi.internal.crypto.X509CertificateFactory
import net.corda.nodeapi.internal.crypto.x509Certificates
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.nodeapi.internal.persistence.NODE_DATABASE_PREFIX
import org.apache.commons.lang.ArrayUtils.EMPTY_BYTE_ARRAY
import java.security.InvalidAlgorithmParameterException
import java.security.PublicKey
import java.security.cert.*
import javax.annotation.concurrent.ThreadSafe
import javax.persistence.Column
import javax.persistence.Entity
import javax.persistence.Id
import javax.persistence.Lob

/**
 * An identity service that stores parties and their identities to a key value tables in the database. The entries are
 * cached for efficient lookup.
 */
@ThreadSafe
class PersistentIdentityService(cacheFactory: NamedCacheFactory) : SingletonSerializeAsToken(), IdentityServiceInternal {
    companion object {
        private val log = contextLogger()

        fun createPKMap(cacheFactory: NamedCacheFactory): AppendOnlyPersistentMap<SecureHash, PartyAndCertificate, PersistentIdentity, String> {
            return AppendOnlyPersistentMap(
                    cacheFactory = cacheFactory,
                    name = "PersistentIdentityService_partyByKey",
                    toPersistentEntityKey = { it.toString() },
                    fromPersistentEntity = {
                        Pair(
                                SecureHash.parse(it.publicKeyHash),
                                PartyAndCertificate(X509CertificateFactory().delegate.generateCertPath(it.identity.inputStream()))
                        )
                    },
                    toPersistentEntity = { key: SecureHash, value: PartyAndCertificate ->
                        PersistentIdentity(key.toString(), value.certPath.encoded)
                    },
                    persistentEntityClass = PersistentIdentity::class.java
            )
        }

        fun createX500Map(cacheFactory: NamedCacheFactory): AppendOnlyPersistentMap<CordaX500Name, SecureHash, PersistentIdentityNames, String> {
            return AppendOnlyPersistentMap(
                    cacheFactory = cacheFactory,
                    name = "PersistentIdentityService_partyByName",
                    toPersistentEntityKey = { it.toString() },
                    fromPersistentEntity = { Pair(CordaX500Name.parse(it.name), SecureHash.parse(it.publicKeyHash)) },
                    toPersistentEntity = { key: CordaX500Name, value: SecureHash ->
                        PersistentIdentityNames(key.toString(), value.toString())
                    },
                    persistentEntityClass = PersistentIdentityNames::class.java
            )
        }

        private fun mapToKey(owningKey: PublicKey) = owningKey.hash
        private fun mapToKey(party: PartyAndCertificate) = mapToKey(party.owningKey)
    }

    @Entity
    @javax.persistence.Table(name = "${NODE_DATABASE_PREFIX}identities")
    class PersistentIdentity(
            @Id
            @Column(name = "pk_hash", length = MAX_HASH_HEX_SIZE, nullable = false)
            var publicKeyHash: String = "",

            @Lob
            @Column(name = "identity_value", nullable = false)
            var identity: ByteArray = EMPTY_BYTE_ARRAY
    )

    @Entity
    @javax.persistence.Table(name = "${NODE_DATABASE_PREFIX}named_identities")
    class PersistentIdentityNames(
            @Id
            @Column(name = "name", length = 128, nullable = false)
            var name: String = "",

            @Column(name = "pk_hash", length = MAX_HASH_HEX_SIZE, nullable = true)
            var publicKeyHash: String? = ""
    )

    private lateinit var _caCertStore: CertStore
    override val caCertStore: CertStore get() = _caCertStore

    private lateinit var _trustRoot: X509Certificate
    override val trustRoot: X509Certificate get() = _trustRoot

    private lateinit var _trustAnchor: TrustAnchor
    override val trustAnchor: TrustAnchor get() = _trustAnchor

    // CordaPersistence is not a c'tor parameter to work around the cyclic dependency
    var database: CordaPersistence? = null

    lateinit var ourNames: Set<CordaX500Name>

    private val keyToParties = createPKMap(cacheFactory)
    private val principalToParties = createX500Map(cacheFactory)

    fun start(trustRoot: X509Certificate, caCertificates: List<X509Certificate> = emptyList()) {
        _trustRoot = trustRoot
        _trustAnchor = TrustAnchor(trustRoot, null)
        _caCertStore = CertStore.getInstance("Collection", CollectionCertStoreParameters(caCertificates.toSet() + trustRoot))
    }

    fun loadIdentities(identities: Collection<PartyAndCertificate> = emptySet(), confidentialIdentities: Collection<PartyAndCertificate> = emptySet()) {
        identities.forEach {
            val key = mapToKey(it)
            keyToParties.addWithDuplicatesAllowed(key, it, false)
            principalToParties.addWithDuplicatesAllowed(it.name, key, false)
        }
        confidentialIdentities.forEach {
            principalToParties.addWithDuplicatesAllowed(it.name, mapToKey(it), false)
        }
        log.debug("Identities loaded")
    }

    @Throws(CertificateExpiredException::class, CertificateNotYetValidException::class, InvalidAlgorithmParameterException::class)
    override fun verifyAndRegisterIdentity(identity: PartyAndCertificate): PartyAndCertificate? {
        return verifyAndRegisterIdentity(identity, false)
    }

    @Throws(CertificateExpiredException::class, CertificateNotYetValidException::class, InvalidAlgorithmParameterException::class)
    override fun verifyAndRegisterIdentity(identity: PartyAndCertificate, isNewRandomIdentity: Boolean): PartyAndCertificate? {
        return onPersistentStore().transaction {
            verifyAndRegisterIdentity(trustAnchor, identity, isNewRandomIdentity)
        }
    }

    override fun registerIdentity(identity: PartyAndCertificate, isNewRandomIdentity: Boolean): PartyAndCertificate? {
        val identityCertChain = identity.certPath.x509Certificates
        log.debug { "Registering identity $identity" }
        val key = mapToKey(identity)
        if (isNewRandomIdentity) {
            // Because this is supposed to be new and random, there's no way we have it in the database already, so skip the pessimistic check.
            keyToParties.set(key, identity)
        } else {
            keyToParties.addWithDuplicatesAllowed(key, identity)
        }
        // Always keep the first party we registered, as that's the well known identity
        principalToParties.addWithDuplicatesAllowed(identity.name, key, false)
        val parentId = mapToKey(identityCertChain[1].publicKey)
        return keyToParties[parentId]
    }

    override fun certificateFromKey(owningKey: PublicKey): PartyAndCertificate? = onPersistentStore().transaction { keyToParties[mapToKey(owningKey)] }

    private fun certificateFromCordaX500Name(name: CordaX500Name): PartyAndCertificate? {
        return onPersistentStore().transaction {
            val partyId = principalToParties[name]
            if (partyId != null) {
                keyToParties[partyId]
            } else null
        }
    }

    // We give the caller a copy of the data set to avoid any locking problems
    override fun getAllIdentities(): Iterable<PartyAndCertificate> = onPersistentStore().transaction { keyToParties.allPersisted().map { it.second }.asIterable() }

    override fun wellKnownPartyFromX500Name(name: CordaX500Name): Party? = certificateFromCordaX500Name(name)?.party

    override fun wellKnownPartyFromAnonymous(party: AbstractParty): Party? = onPersistentStore().transaction { super.wellKnownPartyFromAnonymous(party) }

    override fun partiesFromName(query: String, exactMatch: Boolean): Set<Party> {
        return onPersistentStore().transaction {
            val results = LinkedHashSet<Party>()
            for ((x500name, partyId) in principalToParties.allPersisted()) {
                partiesFromName(query, exactMatch, x500name, results, keyToParties[partyId]!!.party)
            }
            results
        }
    }

    @Throws(UnknownAnonymousPartyException::class)
    override fun assertOwnership(party: Party, anonymousParty: AnonymousParty) = onPersistentStore().transaction { super.assertOwnership(party, anonymousParty) }

    private fun onPersistentStore(): CordaPersistence {
        return database ?: throw IllegalStateException("Persistence store within identity service has not yet been been initialised or been already closed.")
    }

    // Allows us to cheaply eliminate keys we know belong to others by using the cache contents without triggering loading.
    fun stripCachedPeerKeys(keys: Iterable<PublicKey>): Iterable<PublicKey> {
        return keys.filter {
            val party = keyToParties.getIfCached(mapToKey(it))?.party?.name
            party == null || party in ourNames
        }
    }

    override fun close() {
        log.info("Closing PersistentIdentityService")
        database = null
    }
}