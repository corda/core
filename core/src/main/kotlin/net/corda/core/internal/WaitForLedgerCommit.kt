package net.corda.core.internal

import net.corda.core.DeleteForDJVM
import net.corda.core.concurrent.CordaFuture
import net.corda.core.crypto.SecureHash
import net.corda.core.internal.concurrent.asCordaFuture
import net.corda.core.node.ServiceHub
import net.corda.core.transactions.SignedTransaction

/**
 * Track a transaction hash and notify the state machine once the corresponding transaction has committed.
 */
@DeleteForDJVM
class WaitForLedgerCommit(val hash: SecureHash, private val services: ServiceHub): FlowAsyncOperation<SignedTransaction> {

    override val collectErrorsFromSessions: Boolean
        get() = true

    override fun execute(deduplicationId: String): CordaFuture<SignedTransaction> {
        return services.validatedTransactions.trackTransaction(hash).toCompletableFuture().asCordaFuture()
    }

    override fun toString() = "WaitForLedgerCommit(hash=$hash)"
}