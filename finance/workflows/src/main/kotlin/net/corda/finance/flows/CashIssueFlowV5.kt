package net.corda.finance.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.contracts.Amount
import net.corda.core.flows.StartableByRPC
import net.corda.core.identity.Party
import net.corda.core.serialization.CordaSerializable
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.ProgressTracker
import net.corda.finance.contracts.asset.Cash
import net.corda.finance.flows.AbstractCashFlow.Companion.FINALISING_TX
import net.corda.finance.flows.AbstractCashFlow.Companion.GENERATING_TX
import net.corda.finance.flows.AbstractCashFlow.Companion.SIGNING_TX
import net.corda.finance.issuedBy
import java.util.*

/**
 * Initiates a flow that self-issues cash (which should then be sent to recipient(s) using a payment transaction).
 *
 * We issue cash only to ourselves so that all KYC/AML checks on payments are enforced consistently, rather than risk
 * checks for issuance and payments differing. Outside of test scenarios it would be extremely unusual to issue cash
 * and immediately transfer it, so impact of this limitation is considered minimal.
 *
 * @param amount the amount of currency to issue.
 * @param issuerBankPartyRef a reference to put on the issued currency.
 * @param notary the notary to set on the output states.
 */
@StartableByRPC
class CashIssueFlowV5(val amount: Amount<Currency>,
                      val issuerBankPartyRef: OpaqueBytes,
                      val notary: Party? = null) : AbstractCashFlow<AbstractCashFlow.Result>(tracker()) {

    @Suspendable
    override fun call(): AbstractCashFlow.Result {
        progressTracker.currentStep = GENERATING_TX
        val builder = TransactionBuilder(notary)
        builder.addSuperState()
        val signers = Cash().generateIssue(builder, amount.issuedBy(ourIdentity.ref(issuerBankPartyRef)), ourIdentity, notary
                ?: serviceHub.networkMapCache.notaryIdentities.first())
        progressTracker.currentStep = SIGNING_TX
        val tx = serviceHub.signInitialTransaction(builder, signers)
        progressTracker.currentStep = FINALISING_TX
        // There is no one to send the tx to as we're the only participants
        val notarised = finaliseTx(tx, emptySet(), "Unable to notarise issue")
        return Result(notarised, ourIdentity)
    }
}
