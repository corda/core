package net.corda.finance.workflows

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.contracts.*
import net.corda.core.crypto.SecureHash.Companion.SHA3_256
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.Party
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.node.ServiceHub
import net.corda.core.transactions.TransactionBuilder
import net.corda.finance.contracts.CommercialPaper
import net.corda.finance.workflows.asset.CashUtils
import java.time.Instant
import java.util.*

object CommercialPaperUtils {
    /**
     * Returns a transaction that issues commercial paper, owned by the issuing parties key. Does not update
     * an existing transaction because you aren't able to issue multiple pieces of CP in a single transaction
     * at the moment: this restriction is not fundamental and may be lifted later.
     */
    @JvmStatic
    fun generateIssue(issuance: PartyAndReference, faceValue: Amount<Issued<Currency>>, maturityDate: Instant, notary: Party): TransactionBuilder {
        val state = CommercialPaper.State(issuance, issuance.party, faceValue, maturityDate)
        return TransactionBuilder(notary = notary).setHashAlgorithm(SHA3_256).withItems(
                StateAndContract(state, CommercialPaper.CP_PROGRAM_ID),
                Command(CommercialPaper.Commands.Issue(), issuance.party.owningKey)
        )
    }

    /**
     * Updates the given partial transaction with an input/output/command to reassign ownership of the paper.
     */
    @JvmStatic
    fun generateMove(tx: TransactionBuilder, paper: StateAndRef<CommercialPaper.State>, newOwner: AbstractParty) {
        tx.addInputState(paper)
        tx.addOutputState(paper.state.data.withOwner(newOwner), CommercialPaper.CP_PROGRAM_ID)
        tx.addCommand(CommercialPaper.Commands.Move(), paper.state.data.owner.owningKey)
    }

    /**
     * Intended to be called by the issuer of some commercial paper, when an owner has notified us that they wish
     * to redeem the paper. We must therefore send enough money to the key that owns the paper to satisfy the face
     * value, and then ensure the paper is removed from the ledger.
     *
     * @throws InsufficientBalanceException if the vault doesn't contain enough money to pay the redeemer.
     */
    @Throws(InsufficientBalanceException::class)
    @JvmStatic
    @Suspendable
    fun generateRedeem(tx: TransactionBuilder, paper: StateAndRef<CommercialPaper.State>, services: ServiceHub, ourIdentity: PartyAndCertificate) {
        // Add the cash movement using the states in our vault.
        CashUtils.generateSpend(services, tx, paper.state.data.faceValue.withoutIssuer(), ourIdentity, paper.state.data.owner)
        tx.addInputState(paper)
        tx.addCommand(CommercialPaper.Commands.Redeem(), paper.state.data.owner.owningKey)
    }
}
