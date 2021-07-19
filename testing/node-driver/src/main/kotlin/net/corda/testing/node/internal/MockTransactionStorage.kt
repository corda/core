package net.corda.testing.node.internal

import net.corda.core.concurrent.CordaFuture
import net.corda.core.crypto.SecureHash
import net.corda.core.internal.concurrent.doneFuture
import net.corda.core.messaging.DataFeed
import net.corda.core.node.services.TransactionStorage
import net.corda.core.node.services.vault.PageSpecification
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.toFuture
import net.corda.core.transactions.SignedTransaction
import net.corda.node.services.api.WritableTransactionStorage
import net.corda.testing.node.MockServices
import rx.Observable
import rx.subjects.PublishSubject
import java.time.Instant
import java.util.*

/**
 * A class which provides an implementation of [WritableTransactionStorage] which is used in [MockServices]
 */
open class MockTransactionStorage : WritableTransactionStorage, SingletonSerializeAsToken() {
    override fun trackTransaction(id: SecureHash): CordaFuture<SignedTransaction> {
        return getTransaction(id)?.let { doneFuture(it) } ?: _updatesPublisher.filter { it.id == id }.toFuture()
    }

    override fun trackTransactionWithNoWarning(id: SecureHash): CordaFuture<SignedTransaction> {
        return trackTransaction(id)
    }

    override fun track(): DataFeed<List<SignedTransaction>, SignedTransaction> {
        return DataFeed(txns.values.mapNotNull { if (it.isVerified) it.stx else null }, _updatesPublisher)
    }

    override fun trackWithPagingSpec(paging: PageSpecification): DataFeed<TransactionStorage.Page, SignedTransaction> {
        return DataFeed(TransactionStorage.Page(txns.values.mapNotNull { TransactionStorage.RecordedTransaction(it.stx, Instant.now(),it.isVerified) }, 0), _updatesPublisher);
    }

    private val txns = HashMap<SecureHash, TxHolder>()

    private val _updatesPublisher = PublishSubject.create<SignedTransaction>()

    override val updates: Observable<SignedTransaction>
        get() = _updatesPublisher

    private fun notify(transaction: SignedTransaction): Boolean {
        _updatesPublisher.onNext(transaction)
        return true
    }

    override fun addTransaction(transaction: SignedTransaction): Boolean {
        val current = txns.putIfAbsent(transaction.id, TxHolder(transaction, isVerified = true))
        return if (current == null) {
            notify(transaction)
        } else if (!current.isVerified) {
            current.isVerified = true
            notify(transaction)
        } else {
            false
        }
    }

    override fun addUnverifiedTransaction(transaction: SignedTransaction) {
        txns.putIfAbsent(transaction.id, TxHolder(transaction, isVerified = false))
    }

    override fun getTransaction(id: SecureHash): SignedTransaction? = txns[id]?.let { if (it.isVerified) it.stx else null }

    override fun getTransactionInternal(id: SecureHash): Pair<SignedTransaction, Boolean>? = txns[id]?.let { Pair(it.stx, it.isVerified) }

    private class TxHolder(val stx: SignedTransaction, var isVerified: Boolean)
}