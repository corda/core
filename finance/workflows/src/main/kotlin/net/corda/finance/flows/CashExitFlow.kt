package net.corda.finance.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.contracts.Amount
import net.corda.core.contracts.InsufficientBalanceException
import net.corda.core.crypto.SecureHash.Companion.SHA3_256
import net.corda.core.flows.*
import net.corda.core.node.services.queryBy
import net.corda.core.node.services.vault.DEFAULT_PAGE_NUM
import net.corda.core.node.services.vault.PageSpecification
import net.corda.core.node.services.vault.QueryCriteria.VaultQueryCriteria
import net.corda.core.serialization.CordaSerializable
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.ProgressTracker
import net.corda.finance.contracts.asset.Cash
import net.corda.finance.workflows.asset.selection.AbstractCashSelection
import net.corda.finance.flows.AbstractCashFlow.Companion.FINALISING_TX
import net.corda.finance.flows.AbstractCashFlow.Companion.GENERATING_TX
import net.corda.finance.flows.AbstractCashFlow.Companion.SIGNING_TX
import net.corda.finance.issuedBy
import java.util.*

/**
 * Initiates a flow that produces an cash exit transaction.
 *
 * @param amount the amount of a currency to remove from the ledger.
 * @param issuerRef the reference on the issued currency. Added to the node's legal identity to determine the
 * issuer.
 */
@InitiatingFlow
@StartableByRPC
class CashExitFlow(private val amount: Amount<Currency>,
                   private val issuerRef: OpaqueBytes,
                   progressTracker: ProgressTracker) : AbstractCashFlow<AbstractCashFlow.Result>(progressTracker) {
    constructor(amount: Amount<Currency>, issuerRef: OpaqueBytes) : this(amount, issuerRef, tracker())
    constructor(request: ExitRequest) : this(request.amount, request.issuerRef, tracker())

    companion object {
        fun tracker() = ProgressTracker(GENERATING_TX, SIGNING_TX, FINALISING_TX)
    }

    /**
     * @return the signed transaction, and a mapping of parties to new anonymous identities generated
     * (for this flow this map is always empty).
     */
    @Suspendable
    @Throws(CashException::class)
    override fun call(): AbstractCashFlow.Result {
        progressTracker.currentStep = GENERATING_TX
        val builder = TransactionBuilder(notary = null).setHashAlgorithm(SHA3_256)
        val issuer = ourIdentity.ref(issuerRef)
        val exitStates = AbstractCashSelection
                .getInstance { serviceHub.jdbcSession().metaData }
                .unconsumedCashStatesForSpending(serviceHub, amount, setOf(issuer.party), builder.notary, builder.lockId, setOf(issuer.reference))
        val signers = try {
            val changeOwner = exitStates.asSequence().map { it.state.data.owner }.toSet().firstOrNull() ?: throw InsufficientBalanceException(amount)
            Cash().generateExit(
                    builder,
                    amount.issuedBy(issuer),
                    exitStates,
                    changeOwner)
        } catch (e: InsufficientBalanceException) {
            throw CashException("Exiting more cash than exists", e)
        }

        // Work out who the owners of the burnt states were (specify page size so we don't silently drop any if > DEFAULT_PAGE_SIZE)
        val inputStates = serviceHub.vaultService.queryBy<Cash.State>(VaultQueryCriteria(stateRefs = builder.inputStates()),
                PageSpecification(pageNumber = DEFAULT_PAGE_NUM, pageSize = builder.inputStates().size)).states

        val participantSessions = inputStates
                .asSequence()
                .mapNotNull { serviceHub.identityService.wellKnownPartyFromAnonymous(it.state.data.owner) }
                .filterNot(serviceHub.myInfo::isLegalIdentity)
                .distinct()
                .map(::initiateFlow)
                .toList()
        // Sign transaction
        progressTracker.currentStep = SIGNING_TX
        val tx = serviceHub.signInitialTransaction(builder, signers)

        // Commit the transaction
        progressTracker.currentStep = FINALISING_TX
        val notarised = finaliseTx(tx, participantSessions, "Unable to notarise exit")
        return Result(notarised, null)
    }

    @CordaSerializable
    class ExitRequest(amount: Amount<Currency>, val issuerRef: OpaqueBytes) : AbstractRequest(amount)
}

@InitiatedBy(CashExitFlow::class)
class CashExitResponderFlow(private val otherSide: FlowSession) : FlowLogic<Unit>() {
    @Suspendable
    override fun call() {
        subFlow(ReceiveFinalityFlow(otherSide))
    }
}
