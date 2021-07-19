package net.corda.node.services.identity

import net.corda.core.crypto.Crypto
import net.corda.core.crypto.toStringShort
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.AnonymousParty
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.identity.x500Matches
import net.corda.core.internal.CertRole
import net.corda.core.internal.NamedCacheFactory
import net.corda.core.internal.hash
import net.corda.core.internal.toSet
import net.corda.core.node.NotaryInfo
import net.corda.core.node.services.UnknownAnonymousPartyException
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.utilities.MAX_HASH_HEX_SIZE
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.debug
import net.corda.node.internal.schemas.MemberInfoSchemaV1
import net.corda.node.services.api.IdentityServiceInternal
import net.corda.node.services.keys.BasicHSMKeyManagementService
import net.corda.node.services.network.NotaryUpdateListener
import net.corda.node.services.persistence.PublicKeyHashToExternalId
import net.corda.node.services.persistence.WritablePublicKeyToOwningIdentityCache
import net.corda.node.utilities.AppendOnlyPersistentMap
import net.corda.node.utilities.NonInvalidatingCache
import net.corda.nodeapi.internal.KeyOwningIdentity
import net.corda.nodeapi.internal.crypto.X509CertificateFactory
import net.corda.nodeapi.internal.crypto.X509Utilities
import net.corda.nodeapi.internal.crypto.x509Certificates
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.nodeapi.internal.persistence.NODE_DATABASE_PREFIX
import net.corda.nodeapi.internal.persistence.currentDBSession
import org.hibernate.Session
import org.hibernate.annotations.Type
import org.hibernate.internal.util.collections.ArrayHelper.EMPTY_BYTE_ARRAY
import java.security.InvalidAlgorithmParameterException
import java.security.PublicKey
import java.security.cert.CertPathValidatorException
import java.security.cert.CertStore
import java.security.cert.CertificateExpiredException
import java.security.cert.CertificateNotYetValidException
import java.security.cert.CollectionCertStoreParameters
import java.security.cert.TrustAnchor
import java.security.cert.X509Certificate
import java.util.*
import java.util.stream.Stream
import javax.annotation.concurrent.ThreadSafe
import javax.persistence.Column
import javax.persistence.Entity
import javax.persistence.Id
import kotlin.streams.toList

/**
 * An identity service that stores parties and their identities to a key value tables in the database. The entries are
 * cached for efficient lookup.
 */
@ThreadSafe
@Suppress("TooManyFunctions")
class PersistentIdentityService(cacheFactory: NamedCacheFactory) : SingletonSerializeAsToken(), IdentityServiceInternal, NotaryUpdateListener {

    companion object {
        private val log = contextLogger()

        private fun createKeyToPartyAndCertMap(cacheFactory: NamedCacheFactory): AppendOnlyPersistentMap<String, PartyAndCertificate,
                PersistentPublicKeyHashToCertificate, String> {
            return AppendOnlyPersistentMap(
                    cacheFactory = cacheFactory,
                    name = "PersistentIdentityService_keyToPartyAndCert",
                    toPersistentEntityKey = { it },
                    fromPersistentEntity = {
                        Pair(
                                it.publicKeyHash,
                                PartyAndCertificate(X509CertificateFactory().delegate.generateCertPath(it.identity.inputStream()))
                        )
                    },
                    toPersistentEntity = { key: String, value: PartyAndCertificate ->
                        PersistentPublicKeyHashToCertificate(key, value.certPath.encoded)
                    },
                    persistentEntityClass = PersistentPublicKeyHashToCertificate::class.java
            )
        }

        /**
         * [Party] with owning key replaced by its hash.
         */
        private data class PartyWithHash(val name: CordaX500Name, val owningKeyHash: String) {
            constructor(party: Party) : this(party.name, party.owningKey.toStringShort())
        }

        private fun createKeyToPartyMap(cacheFactory: NamedCacheFactory): AppendOnlyPersistentMap<String, PartyWithHash,
                PersistentPublicKeyHashToParty, String> {
            return AppendOnlyPersistentMap(
                    cacheFactory = cacheFactory,
                    name = "PersistentIdentityService_keyToParty",
                    toPersistentEntityKey = { it },
                    fromPersistentEntity = {
                        Pair(
                                it.publicKeyHash,
                                PartyWithHash(CordaX500Name.parse(it.name), it.owningKeyHash)
                        )
                    },
                    toPersistentEntity = { key: String, value: PartyWithHash ->
                        PersistentPublicKeyHashToParty(key, value.name.toString(), value.owningKeyHash)
                    },
                    persistentEntityClass = PersistentPublicKeyHashToParty::class.java)
        }

        private fun createNameToPartyMap(cacheFactory: NamedCacheFactory): NonInvalidatingCache<CordaX500Name, Optional<Party>> {
            return NonInvalidatingCache(
                    cacheFactory = cacheFactory,
                    name = "PersistentIdentityService_nameToParty",
                    loadFunction = {
                        val result = currentDBSession().find(MemberInfoSchemaV1.PersistentMemberInfo::class.java, it.toString())
                        Optional.ofNullable(result?.toParty())
                    }
            )
        }

        fun createHashToKeyMap(cacheFactory: NamedCacheFactory): AppendOnlyPersistentMap<String, PublicKey, PersistentHashToPublicKey,
                String> {
            return AppendOnlyPersistentMap(
                    cacheFactory = cacheFactory,
                    name = "PersistentIdentityService_hashToKey",
                    toPersistentEntityKey = { it },
                    fromPersistentEntity = {
                        Pair(
                                it.publicKeyHash,
                                Crypto.decodePublicKey(it.publicKey)
                        )
                    },
                    toPersistentEntity = { key: String, value: PublicKey ->
                        PersistentHashToPublicKey(key, value.encoded)
                    },
                    persistentEntityClass = PersistentHashToPublicKey::class.java)
        }

        private fun mapToKey(party: PartyAndCertificate) = party.owningKey.toStringShort()
    }

    @Entity
    @javax.persistence.Table(name = "${NODE_DATABASE_PREFIX}identities")
    class PersistentPublicKeyHashToCertificate(
            @Id
            @Column(name = "pk_hash", length = MAX_HASH_HEX_SIZE, nullable = false)
            var publicKeyHash: String = "",

            @Type(type = "corda-blob")
            @Column(name = "identity_value", nullable = false)
            var identity: ByteArray = EMPTY_BYTE_ARRAY
    )

    @Entity
    @javax.persistence.Table(name = "${NODE_DATABASE_PREFIX}identities_no_cert")
    class PersistentPublicKeyHashToParty(
            @Id
            @Column(name = "pk_hash", length = MAX_HASH_HEX_SIZE, nullable = false)
            var publicKeyHash: String = "",

            @Column(name = "name", length = 128, nullable = false)
            var name: String = "",

            @Column(name = "owning_pk_hash", length = MAX_HASH_HEX_SIZE, nullable = false)
            var owningKeyHash: String = ""
    )

    @Entity
    @javax.persistence.Table(name = "${NODE_DATABASE_PREFIX}hash_to_key")
    class PersistentHashToPublicKey(
            @Id
            @Column(name = "pk_hash", length = MAX_HASH_HEX_SIZE, nullable = false)
            var publicKeyHash: String = "",

            @Type(type = "corda-blob")
            @Column(name = "public_key", nullable = false)
            var publicKey: ByteArray = EMPTY_BYTE_ARRAY
    )

    private lateinit var _caCertStore: CertStore
    override val caCertStore: CertStore get() = _caCertStore

    private lateinit var _trustRoot: X509Certificate
    override val trustRoot: X509Certificate get() = _trustRoot

    private lateinit var _trustAnchor: TrustAnchor
    override val trustAnchor: TrustAnchor get() = _trustAnchor

    private lateinit var _trustAnchors: Set<TrustAnchor>
    override val trustAnchors: Set<TrustAnchor> get() = _trustAnchors

    private lateinit var ourParty: Party

    /** Stores notary identities obtained from the network parameters, for which we don't need to perform a database lookup. */
    @Volatile
    private var notaryIdentityCache = HashSet<Party>()

    // CordaPersistence is not a c'tor parameter to work around the cyclic dependency
    lateinit var database: CordaPersistence

    private lateinit var _pkToIdCache: WritablePublicKeyToOwningIdentityCache

    private val keyToPartyAndCert = createKeyToPartyAndCertMap(cacheFactory)
    private val keyToParty = createKeyToPartyMap(cacheFactory)
    private val nameToParty = createNameToPartyMap(cacheFactory)
    private val hashToKey = createHashToKeyMap(cacheFactory)

    fun start(
            trustRoots: Set<X509Certificate>,
            ourIdentity: PartyAndCertificate,
            notaryIdentities: List<Party> = emptyList(),
            pkToIdCache: WritablePublicKeyToOwningIdentityCache
    ) {
        _trustRoot = ourIdentity.certPath.certificates.last() as X509Certificate
        _trustAnchor = TrustAnchor(trustRoot, null)
        _trustAnchors = trustRoots.map { TrustAnchor(it, null) }.toSet()
        // Extract Node CA certificate from node identity certificate path
        val certificates = setOf(ourIdentity.certificate, ourIdentity.certPath.certificates[1], trustRoot)
        _caCertStore = CertStore.getInstance("Collection", CollectionCertStoreParameters(certificates))
        _pkToIdCache = pkToIdCache
        notaryIdentityCache.addAll(notaryIdentities)
        ourParty = ourIdentity.party
    }

    fun loadIdentities(identities: Collection<PartyAndCertificate>) {
        identities.forEach {
            val key = mapToKey(it)
            keyToPartyAndCert.addWithDuplicatesAllowed(key, it, false)
            keyToParty.addWithDuplicatesAllowed(it.owningKey.toStringShort(), PartyWithHash(it.party), false)
            nameToParty.asMap()[it.name] = Optional.of(it.party)
        }
        log.debug("Identities loaded")
    }

    @Throws(CertificateExpiredException::class, CertificateNotYetValidException::class, InvalidAlgorithmParameterException::class)
    override fun verifyAndRegisterIdentity(identity: PartyAndCertificate): PartyAndCertificate? {
        return verifyAndRegisterIdentity(trustAnchors, identity)
    }

    @Throws(CertificateExpiredException::class, CertificateNotYetValidException::class, InvalidAlgorithmParameterException::class)
    override fun verifyAndRegisterNewRandomIdentity(identity: PartyAndCertificate) {
        verifyAndRegisterIdentity(trustAnchors, identity, true)
    }

    @Throws(CertificateExpiredException::class, CertificateNotYetValidException::class, InvalidAlgorithmParameterException::class)
    private fun verifyAndRegisterIdentity(trustAnchors: Set<TrustAnchor>, identity: PartyAndCertificate, isNewRandomIdentity: Boolean = false):
            PartyAndCertificate? {
        // Validate the chain first, before we do anything clever with it
        val identityCertChain = identity.certPath.x509Certificates
        try {
            identity.verify(trustAnchors)
        } catch (e: CertPathValidatorException) {
            val roots = trustAnchors.map { it.trustedCert.subjectX500Principal }
            log.warn("Certificate validation failed for ${identity.name} against trusted roots $roots.")
            log.warn("Certificate path :")
            identityCertChain.reversed().forEachIndexed { index, certificate ->
                val space = (0 until index).joinToString("") { "   " }
                log.warn("$space${certificate.subjectX500Principal}")
            }
            throw e
        }
        // Ensure we record the first identity of the same name, first
        val wellKnownCert = identityCertChain.single { CertRole.extract(it)?.isWellKnown ?: false }
        if (wellKnownCert != identity.certificate && !isNewRandomIdentity) {
            val idx = identityCertChain.lastIndexOf(wellKnownCert)
            val firstPath = X509Utilities.buildCertPath(identityCertChain.slice(idx until identityCertChain.size))
            verifyAndRegisterIdentity(trustAnchors, PartyAndCertificate(firstPath))
        }
        return registerIdentity(identity, isNewRandomIdentity)
    }

    override fun registerIdentity(identity: PartyAndCertificate) {
        registerIdentity(identity, false)
    }

    private fun registerIdentity(identity: PartyAndCertificate, isNewRandomIdentity: Boolean): PartyAndCertificate? {
        log.debug { "Registering identity $identity" }
        val identityCertChain = identity.certPath.x509Certificates
        val key = mapToKey(identity)

        return database.transaction {
            if (isNewRandomIdentity) {
                // Because this is supposed to be new and random, there's no way we have it in the database already, so skip the this check
                keyToPartyAndCert[key] = identity
                // keyToParty is already registered via KMS freshKeyInternal()
            } else {
                keyToPartyAndCert.addWithDuplicatesAllowed(key, identity, false)
                keyToParty.addWithDuplicatesAllowed(identity.owningKey.toStringShort(), PartyWithHash(identity.party), false)
            }
            val parentId = identityCertChain[1].publicKey.toStringShort()
            keyToPartyAndCert[parentId]
        }
    }

    override fun invalidateCaches(name: CordaX500Name) {
        nameToParty.invalidate(name)
    }

    override fun certificateFromKey(owningKey: PublicKey): PartyAndCertificate? = database.transaction {
        keyToPartyAndCert[owningKey.toStringShort()]
    }

    override fun partyFromKey(key: PublicKey): Party? {
        return certificateFromKey(key)?.takeIf { CertRole.extract(it.certificate)?.isWellKnown == false }?.party ?:
        database.transaction {
            keyToParty[key.toStringShort()]
        }?.let { wellKnownPartyFromX500Name(it.name) }
    }

    // We give the caller a copy of the data set to avoid any locking problems
    override fun getAllIdentities(): Iterable<PartyAndCertificate> {
        return database.transaction {
            keyToPartyAndCert.allPersisted.use { it.map { it.second }.toList() }
        }
    }

    override fun wellKnownPartyFromX500Name(name: CordaX500Name): Party? = database.transaction {
        nameToParty[name]?.orElse(null)
    }

    override fun wellKnownPartyFromAnonymous(party: AbstractParty): Party? {
        return database.transaction {
            log.debug("Attempting to find wellKnownParty for: ${party.owningKey.toStringShort()}")
            if (party is Party) {
                val candidate = wellKnownPartyFromX500Name(party.name)
                if (candidate != null && candidate != party) {
                    // Party doesn't match existing well-known party: check that the key is registered, otherwise return null.
                    require(party.name == candidate.name) { "Candidate party $candidate does not match expected $party" }
                    keyToParty[party.owningKey.toStringShort()]?.let { candidate }
                } else {
                    // Party is a well-known party or well-known party doesn't exist: skip checks.
                    // If the notary is not in the network map cache, try getting it from the network parameters
                    // to prevent database conversion issues with vault updates (CORDA-2745).
                    candidate ?: party.takeIf { it in notaryIdentityCache }
                }
            } else {
                keyToParty[party.owningKey.toStringShort()]?.let {
                    // Resolved party can be stale due to key rotation: always convert it to the actual well-known party.
                    wellKnownPartyFromX500Name(it.name)
                }
            }
        }
    }

    private fun getAllParties(session: Session): Stream<Party> {
        val criteria = session.criteriaBuilder.createQuery(MemberInfoSchemaV1.PersistentMemberInfo::class.java)
        criteria.select(criteria.from(MemberInfoSchemaV1.PersistentMemberInfo::class.java))
        return session.createQuery(criteria).stream().map { it.toParty() }
    }

    override fun partiesFromName(query: String, exactMatch: Boolean): Set<Party> {
        return database.transaction {
            getAllParties(session).use { stream ->
                stream.filter { x500Matches(query, exactMatch, it.name) }.toSet()
            }
        }
    }

    @Throws(UnknownAnonymousPartyException::class)
    override fun assertOwnership(party: Party, anonymousParty: AnonymousParty) = database.transaction { super.assertOwnership(party,
            anonymousParty) }

    override fun registerKey(publicKey: PublicKey, party: Party, externalId: UUID?) {
        return database.transaction {
            val publicKeyHash = publicKey.toStringShort()
            // EVERY key should be mapped to a Party in the "keyToName" table. Therefore if there is already a record in that table for the
            // specified key then it's either our key which has been stored prior or another node's key which we have previously mapped.
            val existingEntryForKey = keyToParty[publicKeyHash]
            if (existingEntryForKey == null) {
                // Update the three tables as necessary. We definitely store the public key and map it to a party and we optionally update
                // the public key to external ID mapping table. This block will only ever be reached when registering keys generated on
                // other because when a node generates its own keys "registerKeyToParty" is automatically called by
                // KeyManagementService.freshKey.
                registerKeyToParty(publicKey, party)
                hashToKey[publicKeyHash] = publicKey
                if (externalId != null) {
                    registerKeyToExternalId(publicKey, externalId)
                }
            } else {
                log.info("An existing entry for $publicKeyHash already exists.")
                if (party.name != existingEntryForKey.name) {
                    throw IllegalStateException("The public publicKey $publicKeyHash is already assigned to a different party than the " +
                            "supplied party.")
                }
            }
        }
    }

    // Internal function used by the KMS to register a public key to a Corda Party.
    fun registerKeyToParty(publicKey: PublicKey, party: Party = ourParty) {
        return database.transaction {
            log.info("Linking: ${publicKey.hash} to ${party.name}")
            keyToParty[publicKey.toStringShort()] = PartyWithHash(party)
            if (party == ourParty) {
                _pkToIdCache[publicKey] = KeyOwningIdentity.UnmappedIdentity
            }
        }
    }

    // Internal function used by the KMS to register a public key to an external ID.
    fun registerKeyToExternalId(publicKey: PublicKey, externalId: UUID) {
        _pkToIdCache[publicKey] = KeyOwningIdentity.fromUUID(externalId)
    }

    override fun externalIdForPublicKey(publicKey: PublicKey): UUID? {
        return _pkToIdCache[publicKey].uuid
    }

    private fun publicKeysForExternalId(externalId: UUID, table: Class<*>): List<PublicKey> {
        return database.transaction {
            val query = session.createQuery(
                    """
                        select a.publicKey
                        from ${table.name} a, ${PublicKeyHashToExternalId::class.java.name} b
                        where b.externalId = :uuid
                        and b.publicKeyHash = a.publicKeyHash
                    """,
                    ByteArray::class.java
            )
            query.setParameter("uuid", externalId)
            query.resultList.map { Crypto.decodePublicKey(it) }
        }
    }

    override fun publicKeysForExternalId(externalId: UUID): Iterable<PublicKey> {
        // If the externalId was created by this node then we'll find the keys in the KMS, otherwise they'll be in the IdentityService.
        val keys = publicKeysForExternalId(externalId, BasicHSMKeyManagementService.PersistentKey::class.java)
        return if (keys.isEmpty()) {
            publicKeysForExternalId(externalId, PersistentHashToPublicKey::class.java)
        } else {
            keys
        }
    }

    override fun onNewNotaryList(notaries: List<NotaryInfo>) {
        notaryIdentityCache = HashSet(notaries.map { it.identity })
    }
}