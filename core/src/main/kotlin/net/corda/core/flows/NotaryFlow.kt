package net.corda.core.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.CordaInternal
import net.corda.core.DoNotImplement
import net.corda.core.contracts.StateRef
import net.corda.core.contracts.TimeWindow
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.TransactionSignature
import net.corda.core.identity.Party
import net.corda.core.internal.BackpressureAwareTimedFlow
import net.corda.core.internal.FetchDataFlow
import net.corda.core.internal.NetworkParametersStorage
import net.corda.core.internal.notary.generateSignature
import net.corda.core.internal.notary.validateSignatures
import net.corda.core.internal.pushToLoggingContext
import net.corda.core.transactions.*
import net.corda.core.utilities.ProgressTracker
import net.corda.core.utilities.UntrustworthyData
import net.corda.core.utilities.unwrap
import java.util.function.Predicate

class NotaryFlow {
    /**
     * A flow to be used by a party for obtaining signature(s) from a [NotaryService] ascertaining the transaction
     * time-window is correct and none of its inputs have been used in another completed transaction.
     *
     * In case of a single-node or Raft notary, the flow will return a single signature. For the BFT notary multiple
     * signatures will be returned – one from each replica that accepted the input state commit.
     *
     * The transaction to be notarised, [stx], should be fully verified before calling this flow.
     *
     * @throws NotaryException in case the any of the inputs to the transaction have been consumed
     *                         by another transaction or the time-window is invalid or
     *                         the parameters used for this transaction are no longer in force in the network.
     */
    @DoNotImplement
    @InitiatingFlow
    open class Client(
            private val stx: SignedTransaction,
            override val progressTracker: ProgressTracker,
            /**
             * Set to *true* if the [stx] has already been verified for signature and contract validity,
             * to prevent re-verification.
             */
            private val skipVerification: Boolean = false
    ) : BackpressureAwareTimedFlow<List<TransactionSignature>>() {
        @JvmOverloads
        constructor(stx: SignedTransaction, skipVerification: Boolean = false) : this(stx, tracker(), skipVerification)
        constructor(stx: SignedTransaction, progressTracker: ProgressTracker): this(stx, progressTracker, false)

        companion object {
            object REQUESTING : ProgressTracker.Step("Requesting signature by Notary service")
            object VALIDATING : ProgressTracker.Step("Validating response from Notary service")

            fun tracker() = ProgressTracker(REQUESTING, VALIDATING)
        }

        override val isTimeoutEnabled: Boolean
            @CordaInternal
            get() {
                val notaryParty = stx.notary ?: throw IllegalStateException("Transaction does not specify a Notary")
                return serviceHub.networkMapCache.getNodesByLegalIdentityKey(notaryParty.owningKey).size > 1
            }

        @Suspendable
        @Throws(NotaryException::class)
        override fun call(): List<TransactionSignature> {
            stx.pushToLoggingContext()
            val notaryParty = checkTransaction()
            logger.info("Sending transaction to notary: ${notaryParty.name}.")
            progressTracker.currentStep = REQUESTING
            val response = notarise(notaryParty)
            logger.info("Notary responded (${notaryParty.name}).")
            progressTracker.currentStep = VALIDATING
            return validateResponse(response, notaryParty)
        }

        /**
         * Checks that the transaction specifies a valid notary, and verifies that it contains all required signatures
         * apart from the notary's.
         */
        protected fun checkTransaction(): Party {
            val notaryParty = stx.notary ?: throw IllegalStateException("Transaction does not specify a Notary")
            check(serviceHub.networkMapCache.isNotary(notaryParty)) { "$notaryParty is not a notary on the network" }
            check(serviceHub.loadStates(stx.inputs.toSet() + stx.references.toSet()).all { it.state.notary == notaryParty }) {
                "Input states and reference input states must have the same Notary"
            }

            if (!skipVerification) {
                // TODO= [CORDA-3267] Remove duplicate signature verification
                stx.resolveTransactionWithSignatures(serviceHub).verifySignaturesExcept(notaryParty.owningKey)
                stx.verify(serviceHub, false)
            }
            return notaryParty
        }

        /** Notarises the transaction with the [notaryParty], obtains the notary's signature(s). */
        @Throws(NotaryException::class)
        @Suspendable
        protected fun notarise(notaryParty: Party): UntrustworthyData<NotarisationResponse> {
            val session = initiateFlow(notaryParty)
            val requestSignature = generateRequestSignature()
            return if (isValidating(notaryParty)) {
                sendAndReceiveValidating(session, requestSignature)
            } else {
                sendAndReceiveNonValidating(notaryParty, session, requestSignature)
            }
        }

        private fun isValidating(notaryParty: Party): Boolean {
            val onTheCurrentWhitelist = serviceHub.networkMapCache.isNotary(notaryParty)
            return if (!onTheCurrentWhitelist) {
                /*
                    Note that the only scenario where it's acceptable to use a notary not in the current network parameter whitelist is
                    when performing a notary change transaction after a network merge – the old notary won't be on the whitelist of the new network,
                    and can't be used for regular transactions.
                */
                check(stx.coreTransaction is NotaryChangeWireTransaction) {
                    "Notary $notaryParty is not on the network parameter whitelist. A non-whitelisted notary can only be used for notary change transactions"
                }
                val historicNotary = (serviceHub.networkParametersService as NetworkParametersStorage).getHistoricNotary(notaryParty)
                        ?: throw IllegalStateException("The notary party $notaryParty specified by transaction ${stx.id}, is not recognised as a current or historic notary.")
                historicNotary.validating
            } else serviceHub.networkMapCache.isValidatingNotary(notaryParty)
        }

        @Suspendable
        private fun sendAndReceiveValidating(session: FlowSession, signature: NotarisationRequestSignature): UntrustworthyData<NotarisationResponse> {
            val payload = NotarisationPayload(stx, signature)
            subFlow(NotarySendTransactionFlow(session, payload))
            return receiveResultOrTiming(session)
        }

        @Suspendable
        private fun sendAndReceiveNonValidating(notaryParty: Party, session: FlowSession, signature: NotarisationRequestSignature): UntrustworthyData<NotarisationResponse> {
            val ctx = stx.coreTransaction
            val tx = when (ctx) {
                is ContractUpgradeWireTransaction -> ctx.buildFilteredTransaction()
                is WireTransaction -> ctx.buildFilteredTransaction(Predicate {
                    it is StateRef || it is ReferenceStateRef || it is TimeWindow || it == notaryParty || it is NetworkParametersHash
                })
                else -> ctx
            }
            session.send(NotarisationPayload(tx, signature))
            return receiveResultOrTiming(session)
        }

        /** Checks that the notary's signature(s) is/are valid. */
        protected fun validateResponse(response: UntrustworthyData<NotarisationResponse>, notaryParty: Party): List<TransactionSignature> {
            return response.unwrap {
                it.validateSignatures(stx.id, notaryParty)
                it.signatures
            }
        }

        /**
         * The [NotarySendTransactionFlow] flow is similar to [SendTransactionFlow], but uses [NotarisationPayload] as the
         * initial message, and retries message delivery.
         */
        private class NotarySendTransactionFlow(otherSide: FlowSession, payload: NotarisationPayload) : DataVendingFlow(otherSide, payload) {
            @Suspendable
            override fun sendPayloadAndReceiveDataRequest(otherSideSession: FlowSession, payload: Any): UntrustworthyData<FetchDataFlow.Request> {
                return otherSideSession.sendAndReceiveWithRetry(payload)
            }
        }

        /**
         * Ensure that transaction ID instances are not referenced in the serialized form in case several input states are outputs of the
         * same transaction.
         */
        private fun generateRequestSignature(): NotarisationRequestSignature {
            // TODO: This is not required any more once our AMQP serialization supports turning off object referencing.
            val notarisationRequest = NotarisationRequest(stx.inputs.map { it.copy(txhash = SecureHash.create(it.txhash.toString())) }, stx.id)
            return notarisationRequest.generateSignature(serviceHub)
        }
    }
}
