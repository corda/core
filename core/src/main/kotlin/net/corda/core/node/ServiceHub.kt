package net.corda.core.node

import net.corda.core.DeleteForDJVM
import net.corda.core.DoNotImplement
import net.corda.core.contracts.*
import net.corda.core.cordapp.CordappContext
import net.corda.core.cordapp.CordappProvider
import net.corda.core.crypto.Crypto
import net.corda.core.crypto.SignableData
import net.corda.core.crypto.SignatureMetadata
import net.corda.core.crypto.TransactionSignature
import net.corda.core.flows.ContractUpgradeFlow
import net.corda.core.node.services.*
import net.corda.core.node.services.diagnostics.DiagnosticsService
import net.corda.core.serialization.SerializeAsToken
import net.corda.core.transactions.FilteredTransaction
import net.corda.core.transactions.LedgerTransaction
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.TransactionBuilder
import java.security.PublicKey
import java.sql.Connection
import java.time.Clock
import java.util.function.Consumer
import javax.persistence.EntityManager

/**
 * Subset of node services that are used for loading transactions from the wire into fully resolved, looked up
 * forms ready for verification.
 */
@DeleteForDJVM
@DoNotImplement
interface ServicesForResolution {
    /**
     * An identity service maintains a directory of parties by their associated distinguished name/public keys and thus
     * supports lookup of a party given its key, or name. The service also manages the certificates linking confidential
     * identities back to the well known identity (i.e. the identity in the network map) of a party.
     */
    val identityService: IdentityService

    /** Provides access to storage of arbitrary JAR files (which may contain only data, no code). */
    val attachments: AttachmentStorage

    /** Provides access to anything relating to cordapps including contract attachment resolution and app context */
    val cordappProvider: CordappProvider

    /** Provides access to historical network parameters that are used in transaction resolution. */
    val networkParametersService: NetworkParametersService

    /** Returns the network parameters the node is operating under. */
    val networkParameters: NetworkParameters

    /**
     * Given a [StateRef] loads the referenced transaction and looks up the specified output [ContractState].
     *
     * *WARNING* Do not use this method unless you really only want a single state - any batch loading should
     * go through [loadStates] as repeatedly calling [loadState] can lead to repeat deserialsiation work and
     * severe performance degradation.
     *
     * @throws TransactionResolutionException if [stateRef] points to a non-existent transaction.
     */
    @Throws(TransactionResolutionException::class)
    fun loadState(stateRef: StateRef): TransactionState<*>

    /**
     * Given a [Set] of [StateRef]'s loads the referenced transaction and looks up the specified output [ContractState].
     *
     * @throws TransactionResolutionException if [stateRef] points to a non-existent transaction.
     */
    // TODO: future implementation to use a Vault state ref -> contract state BLOB table and perform single query bulk load
    // as the existing transaction store will become encrypted at some point
    @Throws(TransactionResolutionException::class)
    fun loadStates(stateRefs: Set<StateRef>): Set<StateAndRef<ContractState>>

    /**
     * Returns the [Attachment] that defines the given [StateRef], which must be in the visible subset of the ledger.
     */
    @Throws(TransactionResolutionException::class, AttachmentResolutionException::class)
    fun loadContractAttachment(stateRef: StateRef): Attachment

    /**
     * Provides a callback for the Node to customise the [LedgerTransaction].
     */
    @JvmDefault
    fun specialise(ltx: LedgerTransaction): LedgerTransaction = ltx
}

/**
 * Controls whether the transaction is sent to the vault at all, and if so whether states have to be relevant
 * or not in order to be recorded. Used in [ServiceHub.recordTransactions]
 */
enum class StatesToRecord {
    /** The received transaction is not sent to the vault at all. This is used within transaction resolution. */
    NONE,
    /**
     * All states that can be seen in the transaction will be recorded by the vault, even if none of the identities
     * on this node are a participant or owner.
     */
    ALL_VISIBLE,
    /**
     * Only states that involve one of our public keys will be stored in the vault. This is the default. A public
     * key is involved (relevant) if it's in the [OwnableState.owner] field, or appears in the [ContractState.participants]
     * collection. This is usually equivalent to "can I change the contents of this state by signing a transaction".
     */
    ONLY_RELEVANT
}

/**
 * A service hub is the starting point for most operations you can do inside the node. You are provided with one
 * when a class annotated with [CordaService] is constructed, and you have access to one inside flows. Most RPCs
 * simply forward to the services found here after some access checking.
 *
 * The APIs are organised roughly by category, with a few very important top level APIs available on the ServiceHub
 * itself. Inside a flow, it's safe to keep a reference to services found here on the stack: checkpointing will do the
 * right thing (it won't try to serialise the internals of the service).
 *
 * In unit test environments, some of those services may be missing or mocked out.
 */
@DeleteForDJVM
interface ServiceHub : ServicesForResolution {
    // NOTE: Any services exposed to flows (public view) need to implement [SerializeAsToken] or similar to avoid
    // their internal state from being serialized in checkpoints.

    /**
     * The vault service lets you observe, soft lock and add notes to states that involve you or are relevant to your
     * node in some way. Additionally you may query and track states that correspond to various criteria.
     */
    val vaultService: VaultService

    /**
     * The key management service is responsible for storing and using private keys to sign things. An
     * implementation of this may, for example, call out to a hardware security module that enforces various
     * auditing and frequency-of-use requirements.
     *
     * You don't normally need to use this directly. If you have a [TransactionBuilder] and wish to sign it to
     * get a [SignedTransaction], look at [signInitialTransaction].
     */
    val keyManagementService: KeyManagementService
    /**
     * The [ContractUpgradeService] is responsible for securely upgrading contract state objects according to
     * a specified and mutually agreed (amongst participants) contract version.
     *
     * @see ContractUpgradeFlow to understand the workflow associated with contract upgrades.
     */
    val contractUpgradeService: ContractUpgradeService

    /**
     * A map of hash->tx where tx has been signature/contract validated and the states are known to be correct.
     * The signatures aren't technically needed after that point, but we keep them around so that we can relay
     * the transaction data to other nodes that need it.
     */
    val validatedTransactions: TransactionStorage

    /**
     * A network map contains lists of nodes on the network along with information about their identity keys, services
     * they provide and host names or IP addresses where they can be connected to. The cache wraps around a map fetched
     * from an authoritative service, and adds easy lookup of the data stored within it. Generally it would be initialised
     * with a specified network map service, which it fetches data from and then subscribes to updates of.
     */
    val networkMapCache: MembershipGroupCache

    /**
     * The [DiagnosticsService] provides diagnostic level information about the node, including the current version of the node, and the
     * CorDapps currently installed on the node. Note that this information should be used to provide diagnostics only, and no functional
     * decisions should be made based on this.
     */
    val diagnosticsService: DiagnosticsService

    /**
     * INTERNAL. DO NOT USE.
     * @suppress
     */
    val transactionVerifierService: TransactionVerifierService

    /**
     * A [Clock] representing the node's current time. This should be used in preference to directly accessing the
     * clock so the current time can be controlled during unit testing.
     */
    val clock: Clock

    /** The [NodeInfo] object corresponding to our own entry in the network map. */
    val myInfo: NodeInfo

    /** The [MemberInfo] object which corresponds to our entry in the Membership Group. **/
    val myMemberInfo: MemberInfo

    /**
     * Return the singleton instance of the given Corda service type. This is a class that is annotated with
     * [CordaService] and will have automatically been registered by the node.
     * @throws IllegalArgumentException If [type] is not annotated with [CordaService] or if the instance is not found.
     */
    fun <T : SerializeAsToken> cordaService(type: Class<T>): T

    /**
     * Stores the given [SignedTransaction]s in the local transaction storage and then sends them to the vault for
     * further processing if [notifyVault] is true. This is expected to be run within a database transaction.
     *
     * @param txs The transactions to record.
     * @param notifyVault indicate if the vault should be notified for the update.
     */
    fun recordTransactions(notifyVault: Boolean, txs: Iterable<SignedTransaction>) {
        recordTransactions(if (notifyVault) StatesToRecord.ONLY_RELEVANT else StatesToRecord.NONE, txs)
    }

    /**
     * Stores the given [SignedTransaction]s in the local transaction storage and then sends them to the vault for
     * further processing if [notifyVault] is true. This is expected to be run within a database transaction.
     */
    fun recordTransactions(notifyVault: Boolean, first: SignedTransaction, vararg remaining: SignedTransaction) {
        recordTransactions(notifyVault, listOf(first, *remaining))
    }

    /**
     * Stores the given [SignedTransaction]s in the local transaction storage and then sends them to the vault for
     * further processing if [statesToRecord] is not [StatesToRecord.NONE].
     * This is expected to be run within a database transaction.
     *
     * @param txs The transactions to record.
     * @param statesToRecord how the vault should treat the output states of the transaction.
     */
    fun recordTransactions(statesToRecord: StatesToRecord, txs: Iterable<SignedTransaction>)

    /**
     * Stores the given [SignedTransaction]s in the local transaction storage and then sends them to the vault for
     * further processing. This is expected to be run within a database transaction.
     */
    fun recordTransactions(first: SignedTransaction, vararg remaining: SignedTransaction) {
        recordTransactions(listOf(first, *remaining))
    }

    /**
     * Stores the given [SignedTransaction]s in the local transaction storage and then sends them to the vault for
     * further processing. This is expected to be run within a database transaction.
     */
    fun recordTransactions(txs: Iterable<SignedTransaction>) {
        recordTransactions(StatesToRecord.ONLY_RELEVANT, txs)
    }

    /**
     * Converts the given [StateRef] into a [StateAndRef] object.
     *
     * @throws TransactionResolutionException if [stateRef] points to a non-existent transaction.
     */
    @Throws(TransactionResolutionException::class)
    fun <T : ContractState> toStateAndRef(stateRef: StateRef): StateAndRef<T> {
        val stx = validatedTransactions.getTransaction(stateRef.txhash) ?: throw TransactionResolutionException(stateRef.txhash)
        return stx.resolveBaseTransaction(this).outRef(stateRef.index)
    }

    private val legalIdentityKey: PublicKey get() = this.myInfo.legalIdentitiesAndCerts.first().owningKey

    // Helper method to construct an initial partially signed transaction from a [TransactionBuilder].
    private fun signInitialTransaction(builder: TransactionBuilder, publicKey: PublicKey, signatureMetadata: SignatureMetadata): SignedTransaction {
        return builder.toSignedTransaction(keyManagementService, publicKey, signatureMetadata, this)
    }

    /**
     * Helper method to construct an initial partially signed transaction from a [TransactionBuilder]
     * using keys stored inside the node. Signature metadata is added automatically.
     * @param builder The [TransactionBuilder] to seal with the node's signature.
     * Any existing signatures on the builder will be preserved.
     * @param publicKey The [PublicKey] matched to the internal [java.security.PrivateKey] to use in signing this transaction.
     * If the passed in key is actually a CompositeKey the code searches for the first child key hosted within this node
     * to sign with.
     * @return Returns a SignedTransaction with the new node signature attached.
     */
    fun signInitialTransaction(builder: TransactionBuilder, publicKey: PublicKey) =
            signInitialTransaction(builder, publicKey, SignatureMetadata(myInfo.platformVersion, Crypto.findSignatureScheme(publicKey).schemeNumberID))

    /**
     * Helper method to construct an initial partially signed transaction from a TransactionBuilder
     * using the default identity key contained in the node. The legal identity key is used to sign.
     * @param builder The TransactionBuilder to seal with the node's signature.
     * Any existing signatures on the builder will be preserved.
     * @return Returns a SignedTransaction with the new node signature attached.
     */
    fun signInitialTransaction(builder: TransactionBuilder): SignedTransaction = signInitialTransaction(builder, legalIdentityKey)

    /**
     * Helper method to construct an initial partially signed transaction from a [TransactionBuilder]
     * using a set of keys all held in this node.
     * @param builder The [TransactionBuilder] to seal with the node's signature.
     * Any existing signatures on the builder will be preserved.
     * @param signingPubKeys A list of [PublicKey]s used to lookup the matching [java.security.PrivateKey] and sign.
     * @throws IllegalArgumentException is thrown if any keys are unavailable locally.
     * @return Returns a [SignedTransaction] with the new node signature attached.
     */
    fun signInitialTransaction(builder: TransactionBuilder, signingPubKeys: Iterable<PublicKey>): SignedTransaction {
        val it = signingPubKeys.iterator()
        var stx = signInitialTransaction(builder, it.next())
        while (it.hasNext()) {
            stx = addSignature(stx, it.next())
        }
        return stx
    }

    // Helper method to create an additional signature for an existing (partially) [SignedTransaction].
    private fun createSignature(signedTransaction: SignedTransaction, publicKey: PublicKey, signatureMetadata: SignatureMetadata): TransactionSignature {
        val signableData = SignableData(signedTransaction.id, signatureMetadata)
        return keyManagementService.sign(signableData, publicKey)
    }

    /**
     * Helper method to create an additional signature for an existing (partially) [SignedTransaction]. Additional
     * [SignatureMetadata], including the
     * platform version used during signing and the cryptographic signature scheme use, is added to the signature.
     * @param signedTransaction The [SignedTransaction] to which the signature will apply.
     * @param publicKey The [PublicKey] matching to a signing [java.security.PrivateKey] hosted in the node.
     * If the [PublicKey] is actually a [net.corda.core.crypto.CompositeKey] the first leaf key found locally will be used
     * for signing.
     * @return The [TransactionSignature] generated by signing with the internally held [java.security.PrivateKey].
     */
    fun createSignature(signedTransaction: SignedTransaction, publicKey: PublicKey) =
            createSignature(signedTransaction, publicKey, SignatureMetadata(myInfo.platformVersion, Crypto.findSignatureScheme(publicKey).schemeNumberID))

    /**
     * Helper method to create a signature for an existing (partially) [SignedTransaction]
     * using the default identity signing key of the node. The legal identity key is used to sign. Additional
     * [SignatureMetadata], including the
     * platform version used during signing and the cryptographic signature scheme use, is added to the signature.
     * @param signedTransaction The SignedTransaction to which the signature will apply.
     * @return the TransactionSignature generated by signing with the internally held identity PrivateKey.
     */
    fun createSignature(signedTransaction: SignedTransaction): TransactionSignature {
        return createSignature(signedTransaction, legalIdentityKey)
    }

    /**
     * Helper method to append an additional signature to an existing (partially) [SignedTransaction].
     * @param signedTransaction The [SignedTransaction] to which the signature will be added.
     * @param publicKey The [PublicKey] matching to a signing [java.security.PrivateKey] hosted in the node.
     * If the [PublicKey] is actually a [net.corda.core.crypto.CompositeKey] the first leaf key found locally will be used
     * for signing.
     * @return A new [SignedTransaction] with the addition of the new signature.
     */
    fun addSignature(signedTransaction: SignedTransaction, publicKey: PublicKey): SignedTransaction {
        return signedTransaction + createSignature(signedTransaction, publicKey)
    }

    /**
     * Helper method to append an additional signature for an existing (partially) [SignedTransaction]
     * using the default identity signing key of the node.
     * @param signedTransaction The [SignedTransaction] to which the signature will be added.
     * @return A new [SignedTransaction] with the addition of the new signature.
     */
    fun addSignature(signedTransaction: SignedTransaction): SignedTransaction = addSignature(signedTransaction, legalIdentityKey)

    // Helper method to create a signature for a FilteredTransaction.
    private fun createSignature(filteredTransaction: FilteredTransaction, publicKey: PublicKey, signatureMetadata: SignatureMetadata): TransactionSignature {
        val signableData = SignableData(filteredTransaction.id, signatureMetadata)
        return keyManagementService.sign(signableData, publicKey)
    }

    /**
     * Helper method to create a signature for a FilteredTransaction. Additional [SignatureMetadata], including the
     * platform version used during signing and the cryptographic signature scheme use, is added to the signature.
     * @param filteredTransaction the [FilteredTransaction] to which the signature will apply.
     * @param publicKey The [PublicKey] matching to a signing [java.security.PrivateKey] hosted in the node.
     * If the [PublicKey] is actually a [net.corda.core.crypto.CompositeKey] the first leaf key found locally will be used
     * for signing.
     * @return The [TransactionSignature] generated by signing with the internally held [java.security.PrivateKey].
     */
    fun createSignature(filteredTransaction: FilteredTransaction, publicKey: PublicKey) =
            createSignature(filteredTransaction, publicKey, SignatureMetadata(myInfo.platformVersion, Crypto.findSignatureScheme(publicKey).schemeNumberID))

    /**
     * Helper method to create a signature for a FilteredTransaction
     * using the default identity signing key of the node. The legal identity key is used to sign. Additional
     * [SignatureMetadata], including the platform version used during signing and the cryptographic signature scheme use,
     * is added to the signature.
     * @param filteredTransaction the FilteredTransaction to which the signature will apply.
     * @return the [TransactionSignature] generated by signing with the internally held identity [java.security.PrivateKey].
     */
    fun createSignature(filteredTransaction: FilteredTransaction): TransactionSignature {
        return createSignature(filteredTransaction, legalIdentityKey)
    }

    /**
     * Exposes a JDBC connection (session) object using the currently configured database.
     * Applications can use this to execute arbitrary SQL queries (native, direct, prepared, callable)
     * against its Node database tables (including custom contract tables defined by extending
     * [net.corda.core.schemas.QueryableState]).
     *
     * When used within a flow, this session automatically forms part of the enclosing flow transaction boundary,
     * and thus queryable data will include everything committed as of the last checkpoint.
     *
     * We want to make sure users have a restricted access to administrative functions, this function will return a [RestrictedConnection] instance.
     * The following methods are blocked:
     * - abort(executor: Executor?)
     * - clearWarnings()
     * - close()
     * - commit()
     * - setSavepoint()
     * - setSavepoint(name : String?)
     * - releaseSavepoint(savepoint: Savepoint?)
     * - rollback()
     * - rollback(savepoint: Savepoint?)
     * - setCatalog(catalog : String?)
     * - setTransactionIsolation(level: Int)
     * - setTypeMap(map: MutableMap<String, Class<*>>?)
     * - setHoldability(holdability: Int)
     * - setSchema(schema: String?)
     * - setNetworkTimeout(executor: Executor?, milliseconds: Int)
     * - setAutoCommit(autoCommit: Boolean)
     * - setReadOnly(readOnly: Boolean)
     *
     * @throws IllegalStateException if called outside of a transaction.
     * @return A [Connection]
     */
    fun jdbcSession(): Connection

    /**
     * Exposes the Java Persistence API (JPA) to flows via a restricted [EntityManager]. This method can be used to
     * persist and query entities which inherit from [MappedSchema]. This is particularly useful if off-ledger data
     * needs to be kept in conjunction with on-ledger state data.
     *
     * NOTE: Suspendable flow operations such as send, receive, subFlow and sleep, cannot be called within the lambda.
     *
     * @param block a lambda function with access to an [EntityManager].
     *
     * We want to make sure users have a restricted access to administrative functions.
     * The following methods are blocked:
     * - close()
     * - unwrap(cls: Class<T>?)
     * - getDelegate(): Any
     * - getMetamodel()
     * - joinTransaction()
     * - lock(entity: Any?, lockMode: LockModeType?)
     * - lock(entity: Any?, lockMode: LockModeType?, properties: MutableMap<String, Any>?)
     * - setProperty(propertyName: String?, value: Any?)
     *
     * getTransaction returns a [RestrictedEntityTransaction] to prevent unsafe manipulation of a flow's underlying
     * database transaction.
     * The following methods are blocked:
     * - begin()
     * - commit()
     * - rollback()
     */
    fun <T : Any?> withEntityManager(block: EntityManager.() -> T): T

    /**
     * Exposes the Java Persistence API (JPA) to flows via a restricted [EntityManager]. This method can be used to
     * persist and query entities which inherit from [MappedSchema]. This is particularly useful if off-ledger data
     * needs to be kept in conjunction with on-ledger state data.
     *
     * NOTE: Suspendable flow operations such as send, receive, subFlow and sleep, cannot be called within the lambda.
     *
     * @param block a lambda function with access to an [EntityManager].
     *
     * We want to make sure users have a restricted access to administrative functions.
     * The following methods are blocked:
     * - close()
     * - unwrap(cls: Class<T>?)
     * - getDelegate(): Any
     * - getMetamodel()
     * - joinTransaction()
     * - lock(entity: Any?, lockMode: LockModeType?)
     * - lock(entity: Any?, lockMode: LockModeType?, properties: MutableMap<String, Any>?)
     * - setProperty(propertyName: String?, value: Any?)
     *
     * getTransaction returns a [RestrictedEntityTransaction] to prevent unsafe manipulation of a flow's underlying
     * database transaction.
     * The following methods are blocked:
     * - begin()
     * - commit()
     * - rollback()
     */
    fun withEntityManager(block: Consumer<EntityManager>)

    /**
     * Allows the registration of a callback that may inform services when the app is shutting down.
     *
     * The intent is to allow the cleaning up of resources - e.g. releasing ports.
     *
     * You should not rely on this to clean up executing flows - that's what quasar is for.
     *
     * Please note that the shutdown handler is not guaranteed to be called. In production the node process may crash,
     * be killed by the operating system and other forms of fatal termination may occur that result in this code never
     * running. So you should use this functionality only for unit/integration testing or for code that can optimise
     * this shutdown e.g. by cleaning up things that would otherwise trigger a slow recovery process next time the
     * node starts.
     */
    fun registerUnloadHandler(runOnStop: () -> Unit)

    /**
     * See [CordappProvider.getAppContext]
     */
    fun getAppContext(): CordappContext = cordappProvider.getAppContext()
}
