package net.corda.traderdemo.flow

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.contracts.Amount
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.InitiatingFlow
import net.corda.core.flows.StartableByRPC
import net.corda.core.identity.Party
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.ProgressTracker
import net.corda.finance.contracts.CommercialPaper
import net.corda.finance.flows.TwoPartyTradeFlow
import java.util.*

@InitiatingFlow
@StartableByRPC
class SellerFlow(private val otherParty: Party,
                 private val amount: Amount<Currency>,
                 override val progressTracker: ProgressTracker) : FlowLogic<SignedTransaction>() {
    constructor(otherParty: Party, amount: Amount<Currency>) : this(otherParty, amount, tracker())

    companion object {
        val PROSPECTUS_HASH = SecureHash.create("decd098666b9657314870e192ced0c3519c2c9d395507a238338f8d003929de9")

        object SELF_ISSUING : ProgressTracker.Step("Got session ID back, issuing and timestamping some commercial paper")

        object TRADING : ProgressTracker.Step("Starting the trade flow") {
            override fun childProgressTracker(): ProgressTracker = TwoPartyTradeFlow.Seller.tracker()
        }

        // We vend a progress tracker that already knows there's going to be a TwoPartyTradingFlow involved at some
        // point: by setting up the tracker in advance, the user can see what's coming in more detail, instead of being
        // surprised when it appears as a new set of tasks below the current one.
        fun tracker() = ProgressTracker(SELF_ISSUING, TRADING)
    }

    @Suspendable
    override fun call(): SignedTransaction {
        progressTracker.currentStep = SELF_ISSUING

        val cpOwner = serviceHub.keyManagementService.freshKeyAndCert(ourIdentityAndCert, false)
        val commercialPaper = serviceHub.vaultService.queryBy(CommercialPaper.State::class.java)
                .states.firstOrNull() ?: throw IllegalStateException("No commercial paper found. Please check if you issued the papers first, follow the README for instructions.")

        progressTracker.currentStep = TRADING

        // Send the offered amount.
        val session = initiateFlow(otherParty)
        session.send(amount)
        val seller = TwoPartyTradeFlow.Seller(
                session,
                commercialPaper,
                amount,
                cpOwner,
                progressTracker.getChildProgressTracker(TRADING)!!)
        return subFlow(seller)
    }
}
