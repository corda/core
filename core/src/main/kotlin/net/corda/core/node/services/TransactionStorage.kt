package net.corda.core.node.services

import net.corda.core.DeleteForDJVM
import net.corda.core.DoNotImplement
import net.corda.core.concurrent.CordaFuture
import net.corda.core.crypto.SecureHash
import net.corda.core.messaging.DataFeed
import net.corda.core.serialization.CordaSerializable
import net.corda.core.transactions.SignedTransaction
import rx.Observable
import java.time.Instant

/**
 * Thread-safe storage of transactions.
 */
@DeleteForDJVM
@DoNotImplement
interface TransactionStorage {
    /**
     * Return the transaction with the given [id], or null if no such transaction exists.
     */
    fun getTransaction(id: SecureHash): SignedTransaction?

    /**
     * Get a synchronous Observable of updates.  When observations are pushed to the Observer, the vault will already
     * incorporate the update.
     */
    val updates: Observable<SignedTransaction>

    /**
     * Returns all currently stored transactions and further fresh ones.
     */
    fun track(): DataFeed<List<SignedTransaction>, SignedTransaction>

    /**
     * Returns a future that completes with the transaction corresponding to [id] once it has been committed
     */
    fun trackTransaction(id: SecureHash): CordaFuture<SignedTransaction>

    @CordaSerializable
    data class Page(val transactions: List<RecordedTransaction>, val totalTransactionsAvailable: Long)

    @CordaSerializable
    data class RecordedTransaction(val signedTransaction: SignedTransaction, val timestamp: Instant, val verified: Boolean)

    class TransactionsQueryException(description: String, cause: Exception? = null) : Exception(description, cause) {
        constructor(description: String) : this(description, null)
    }
}