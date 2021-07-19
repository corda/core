package net.corda.finance.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.confidential.SwapIdentitiesFlow
import net.corda.core.contracts.Amount
import net.corda.core.contracts.InsufficientBalanceException
import net.corda.core.crypto.SecureHash.Companion.SHA3_256
import net.corda.core.flows.*
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.Party
import net.corda.core.serialization.CordaSerializable
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.ProgressTracker
import net.corda.core.utilities.unwrap
import net.corda.finance.flows.AbstractCashFlow.Companion.FINALISING_TX
import net.corda.finance.flows.AbstractCashFlow.Companion.GENERATING_ID
import net.corda.finance.flows.AbstractCashFlow.Companion.GENERATING_TX
import net.corda.finance.flows.AbstractCashFlow.Companion.SIGNING_TX
import net.corda.finance.workflows.asset.CashUtils
import java.util.*

/**
 * Initiates a flow that sends cash to a recipient.
 *
 * @param amount the amount of a currency to pay to the recipient.
 * @param recipient the party to pay the currency to.
 * @param issuerConstraint if specified, the payment will be made using only cash issued by the given parties.
 * @param anonymous whether to anonymous the recipient party. Should be true for normal usage, but may be false
 * @param notary if not specified, the first notary of the network map is selected
 * for testing purposes.
 */
@StartableByRPC
@InitiatingFlow
open class CashPaymentFlow(
        val amount: Amount<Currency>,
        val recipient: Party,
        val anonymous: Boolean,
        progressTracker: ProgressTracker,
        val issuerConstraint: Set<Party> = emptySet(),
        val notary: Party? = null
) : AbstractCashFlow<AbstractCashFlow.Result>(progressTracker) {
    /** A straightforward constructor that constructs spends using cash states of any issuer. */
    constructor(amount: Amount<Currency>, recipient: Party) : this(amount, recipient, true, tracker())

    /** A straightforward constructor that constructs spends using cash states of any issuer. */
    constructor(amount: Amount<Currency>, recipient: Party, anonymous: Boolean) : this(amount, recipient, anonymous, tracker())

    constructor(amount: Amount<Currency>, recipient: Party, anonymous: Boolean, notary: Party) : this(amount, recipient, anonymous, tracker(), notary = notary)

    constructor(request: PaymentRequest) : this(request.amount, request.recipient, request.anonymous, tracker(), request.issuerConstraint, request.notary)

    @Suspendable
    override fun call(): AbstractCashFlow.Result {
        progressTracker.currentStep = GENERATING_ID
        val recipientSession = initiateFlow(recipient)
        recipientSession.send(anonymous)
        val anonymousRecipient: AbstractParty = if (anonymous) {
            subFlow(SwapIdentitiesFlow(recipientSession))[recipient]!!
        } else {
            recipient
        }
        progressTracker.currentStep = GENERATING_TX
        val builder = TransactionBuilder(notary = notary ?: serviceHub.networkMapCache.notaryIdentities.first()).setHashAlgorithm(SHA3_256)
        logger.info("Generating spend for: ${builder.lockId}")
        // TODO: Have some way of restricting this to states the caller controls
        val (spendTX, keysForSigning) = try {
            CashUtils.generateSpend(
                    serviceHub,
                    builder,
                    amount,
                    ourIdentityAndCert,
                    anonymousRecipient,
                    issuerConstraint
            )
        } catch (e: InsufficientBalanceException) {
            throw CashException("Insufficient cash for spend: ${e.message}", e)
        }

        progressTracker.currentStep = SIGNING_TX
        logger.info("Signing transaction for: ${spendTX.lockId}")
        val tx = serviceHub.signInitialTransaction(spendTX, keysForSigning)

        progressTracker.currentStep = FINALISING_TX
        logger.info("Finalising transaction for: ${tx.id}")
        val sessionsForFinality = if (serviceHub.myInfo.isLegalIdentity(recipient)) emptyList() else listOf(recipientSession)
        val notarised = finaliseTx(tx, sessionsForFinality, "Unable to notarise spend")
        logger.info("Finalised transaction for: ${notarised.id}")
        return Result(notarised, anonymousRecipient)
    }

    @CordaSerializable
    class PaymentRequest(amount: Amount<Currency>,
                         val recipient: Party,
                         val anonymous: Boolean,
                         val issuerConstraint: Set<Party> = emptySet(),
                         val notary: Party? = null) : AbstractRequest(amount)
}

@InitiatedBy(CashPaymentFlow::class)
class CashPaymentReceiverFlow(private val otherSide: FlowSession) : FlowLogic<Unit>() {
    @Suspendable
    override fun call() {
        val anonymous = otherSide.receive<Boolean>().unwrap { it }
        if (anonymous) {
            subFlow(SwapIdentitiesFlow(otherSide))
        }
        // Not ideal that we have to do this check, but we must as FinalityFlow does not send locally
        if (!serviceHub.myInfo.isLegalIdentity(otherSide.counterparty)) {
            subFlow(ReceiveFinalityFlow(otherSide))
        }
    }
}
