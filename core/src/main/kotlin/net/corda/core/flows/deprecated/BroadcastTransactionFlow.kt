package net.corda.core.flows.deprecated

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.InitiatingFlow
import net.corda.core.identity.Party
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.NonEmptySet

/**
 * Notify the specified parties about a transaction. The remote peers will download this transaction and its
 * dependency graph, verifying them all. The flow returns when all peers have acknowledged the transactions
 * as valid. Normally you wouldn't use this directly, it would be called via [FinalityFlow].
 *
 * @param notarisedTransaction transaction which has been notarised (if needed) and is ready to notify nodes about.
 * @param participants a list of participants involved in the transaction.
 * @return a list of participants who were successfully notified of the transaction.
 */
@InitiatingFlow
@Deprecated("Use net.corda.core.flows.BroadcastTransactionFlow instead")
class BroadcastTransactionFlow(val notarisedTransaction: SignedTransaction,
                               val participants: NonEmptySet<Party>) : FlowLogic<Unit>() {
    @Suspendable
    override fun call() {
        // TODO: Messaging layer should handle this broadcast for us
        participants.filter { !serviceHub.myInfo.isLegalIdentity(it) }.forEach { participant ->
            // SendTransactionFlow allows otherParty to access our data to resolve the transaction.
            subFlow(SendTransactionFlow(participant, notarisedTransaction))
        }
    }
}
