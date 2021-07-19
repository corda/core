package net.corda.confidential

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.contracts.ContractState
import net.corda.core.contracts.TransactionResolutionException
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.FlowSession
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.transactions.WireTransaction
import net.corda.core.utilities.ProgressTracker
import net.corda.core.utilities.unwrap

object IdentitySyncFlow {
    /**
     * Flow for ensuring that our counterparties in a transaction have the full certificate paths for *our*
     * confidential identities used in states present in the transaction. This is intended for use as a sub-flow of
     * another flow, typically between transaction assembly and signing. An example of where this is useful is where
     * a recipient of a state wants to know that it is being paid by the correct party, and the owner of the state is a
     * confidential identity of that party. This flow would send a copy of the confidential identity path to the
     * recipient, enabling them to verify that identity.
     */
    // TODO: Can this be triggered automatically from [SendTransactionFlow]?
    class Send @JvmOverloads constructor(val otherSideSessions: Set<FlowSession>,
               val tx: WireTransaction,
               override val progressTracker: ProgressTracker = tracker()) : FlowLogic<Unit>() {
        constructor(otherSide: FlowSession, tx: WireTransaction) : this(setOf(otherSide), tx)

        companion object {
            object SYNCING_IDENTITIES : ProgressTracker.Step("Syncing identities")

            fun tracker() = ProgressTracker(SYNCING_IDENTITIES)
        }

        @Suspendable
        override fun call() {
            progressTracker.currentStep = SYNCING_IDENTITIES
            val identityCertificates: Map<AbstractParty, PartyAndCertificate?> = extractOurConfidentialIdentities()

            otherSideSessions.forEach { otherSideSession ->
                val requestedIdentities: List<AbstractParty> = otherSideSession.sendAndReceive<List<AbstractParty>>(identityCertificates.keys.toList()).unwrap { req ->
                    require(req.all { it in identityCertificates.keys }) { "${otherSideSession.counterparty} requested a confidential identity not part of transaction: ${tx.id}" }
                    req
                }
                val sendIdentities: List<PartyAndCertificate?> = requestedIdentities.map {
                    val identityCertificate = identityCertificates[it]
                    if (identityCertificate != null)
                        identityCertificate
                    else
                        throw IllegalStateException("Counterparty requested a confidential identity for which we do not have the certificate path: ${tx.id}")
                }
                otherSideSession.send(sendIdentities)
            }
        }

        private fun extractOurConfidentialIdentities(): Map<AbstractParty, PartyAndCertificate?> {
            val inputStates: List<ContractState> = (tx.inputs.toSet()).mapNotNull {
                try {
                    serviceHub.loadState(it).data
                }
                catch (e: TransactionResolutionException) {
                    null
                }
            }
            val states: List<ContractState> = inputStates + tx.outputs.map { it.data }
            val identities: Set<AbstractParty> = states.flatMap(ContractState::participants).toSet()
            // Filter participants down to the set of those not in the network map (are not well known)
            val confidentialIdentities = identities
                    .filter { serviceHub.networkMapCache.getPartyByKey(it.owningKey) == null }
                    .toList()
            return confidentialIdentities
                    .map {
                        @Suppress("DEPRECATION")
                        Pair(it, serviceHub.identityService.certificateFromKey(it.owningKey))
                    }
                    // Filter down to confidential identities of our well known identity
                    // TODO: Consider if this too restrictive - we perhaps should be checking the name on the signing certificate in the certificate path instead
                    .filter { it.second?.name == ourIdentity.name }
                    .toMap()
        }
    }

    /**
     * Handle an offer to provide proof of identity (in the form of certificate paths) for confidential identities which
     * we do not yet know about.
     */
    class Receive(val otherSideSession: FlowSession) : FlowLogic<Unit>() {
        companion object {
            object RECEIVING_IDENTITIES : ProgressTracker.Step("Receiving confidential identities")
            object RECEIVING_CERTIFICATES : ProgressTracker.Step("Receiving certificates for unknown identities")
        }

        override val progressTracker: ProgressTracker = ProgressTracker(RECEIVING_IDENTITIES, RECEIVING_CERTIFICATES)

        @Suspendable
        override fun call() {
            progressTracker.currentStep = RECEIVING_IDENTITIES
            val allIdentities = otherSideSession.receive<List<AbstractParty>>().unwrap { it }
            val unknownIdentities = allIdentities.filter { serviceHub.identityService.wellKnownPartyFromAnonymous(it) == null }
            progressTracker.currentStep = RECEIVING_CERTIFICATES
            val missingIdentities = otherSideSession.sendAndReceive<List<PartyAndCertificate>>(unknownIdentities)

            // Batch verify the identities we've received, so we know they're all correct before we start storing them in
            // the identity service
            missingIdentities.unwrap { identities ->
                identities.forEach { it.verify(serviceHub.identityService.trustAnchor) }
                identities
            }.forEach { identity ->
                // Store the received confidential identities in the identity service so we have a record of which well known identity they map to.
                serviceHub.identityService.verifyAndRegisterIdentity(identity)
            }
        }
    }
}
