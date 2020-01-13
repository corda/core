package net.corda.finance.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.confidential.SwapIdentitiesFlow
import net.corda.core.contracts.Amount
import net.corda.core.contracts.InsufficientBalanceException
import net.corda.core.flows.*
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.Party
import net.corda.core.serialization.CordaSerializable
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.ProgressTracker
import net.corda.core.utilities.unwrap
import net.corda.finance.contracts.asset.PartyAndAmount
import net.corda.finance.flows.AbstractCashFlow.Companion.FINALISING_TX
import net.corda.finance.flows.AbstractCashFlow.Companion.GENERATING_ID
import net.corda.finance.flows.AbstractCashFlow.Companion.GENERATING_TX
import net.corda.finance.flows.AbstractCashFlow.Companion.SIGNING_TX
import net.corda.finance.workflows.asset.CashUtils
import java.util.*

@StartableByRPC
@InitiatingFlow
open class VersionAwareCashPaymentFlow(
        val amount: Amount<Currency>,
        val recipients: List<Party>
) : AbstractCashFlow<AbstractCashFlow.Result>(tracker()) {

    @Suspendable
    override fun call(): AbstractCashFlow.Result {
        progressTracker.currentStep = GENERATING_ID
        progressTracker.currentStep = GENERATING_TX
        val builder = TransactionBuilder(serviceHub.networkMapCache.notaryIdentities.first())
        val minimumCommonVersion = getMinimumCommonVersion(recipients)
        when (minimumCommonVersion) {
            in 1..4 -> {
                CashUtils.generateSpend(
                        serviceHub,
                        builder,
                        recipients.map { PartyAndAmount(it, amount) },
                        ourIdentityAndCert,
                        maxVersion = 4)
            }
            in 5..6 -> {
                CashUtils.generateSpend(
                        serviceHub,
                        builder,
                        recipients.map { PartyAndAmount(it, amount) },
                        ourIdentityAndCert,
                        maxVersion = 5
                )
                builder.addSuperState()
            }
            else -> {
                throw IllegalStateException("Unhandled platformVersion: $minimumCommonVersion")
            }
        }
        logger.info("Generating spend for: ${builder.lockId}")
        // TODO: Have some way of restricting this to states the caller controls
        progressTracker.currentStep = SIGNING_TX
        val tx = serviceHub.signInitialTransaction(builder)
        progressTracker.currentStep = FINALISING_TX
        logger.info("Finalising transaction for: ${tx.id}")
        val sessionsForFinality = recipients.filter { ourIdentity != it }.map { initiateFlow(it) }
        val notarised = finaliseTx(tx, sessionsForFinality, "Unable to notarise spend")
        logger.info("Finalised transaction for: ${notarised.id}")
        return Result(notarised, null)
    }

    @CordaSerializable
    class PaymentRequest(amount: Amount<Currency>,
                         val recipient: Party,
                         val anonymous: Boolean,
                         val issuerConstraint: Set<Party> = emptySet(),
                         val notary: Party? = null) : AbstractRequest(amount)
}

@InitiatedBy(VersionAwareCashPaymentFlow::class)
class VersionAwareCashPaymentFlowResponder(private val otherSide: FlowSession) : FlowLogic<Unit>() {
    @Suspendable
    override fun call() {
        if (!serviceHub.myInfo.isLegalIdentity(otherSide.counterparty)) {
            subFlow(ReceiveFinalityFlow(otherSide))
        }
    }
}
