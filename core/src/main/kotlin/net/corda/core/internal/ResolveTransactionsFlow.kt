package net.corda.core.internal

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.DeleteForDJVM
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.FlowSession
import net.corda.core.node.StatesToRecord
import net.corda.core.transactions.ContractUpgradeWireTransaction
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.WireTransaction
import net.corda.core.utilities.debug
import net.corda.core.utilities.trace

/**
 * Resolves transactions for the specified [txHashes] along with their full history (dependency graph) from [otherSide].
 * Each retrieved transaction is validated and inserted into the local transaction storage.
 */
@DeleteForDJVM
class ResolveTransactionsFlow private constructor(
        val initialTx: SignedTransaction?,
        val txHashes: Set<SecureHash>,
        val otherSide: FlowSession,
        val statesToRecord: StatesToRecord
) : FlowLogic<Unit>() {

    constructor(txHashes: Set<SecureHash>, otherSide: FlowSession, statesToRecord: StatesToRecord = StatesToRecord.NONE)
            : this(null, txHashes, otherSide, statesToRecord)

    /**
     * Resolves and validates the dependencies of the specified [SignedTransaction]. Fetches the attachments, but does
     * *not* validate or store the [SignedTransaction] itself.
     *
     * @return a list of verified [SignedTransaction] objects, in a depth-first order.
     */
    constructor(transaction: SignedTransaction, otherSide: FlowSession, statesToRecord: StatesToRecord = StatesToRecord.NONE)
            : this(transaction, transaction.dependencies, otherSide, statesToRecord)

    private var fetchNetParamsFromCounterpart = false

    @Suppress("MagicNumber")
    @Suspendable
    override fun call() {
        // TODO This error should actually cause the flow to be sent to the flow hospital to be retried
        val counterpartyPlatformVersion = checkNotNull(serviceHub.networkMapCache.getMemberByParty(otherSide.counterparty)?.platformVersion) {
            "Couldn't retrieve party's ${otherSide.counterparty} platform version from NetworkMapCache"
        }

        // Fetch missing parameters flow was added in version 4. This check is needed so we don't end up with node V4 sending parameters
        // request to node V3 that doesn't know about this protocol.
        fetchNetParamsFromCounterpart = counterpartyPlatformVersion >= PlatformVersionSwitches.FETCH_MISSING_NETWORK_PARAMETERS
        val batchMode = counterpartyPlatformVersion >= PlatformVersionSwitches.BATCH_DOWNLOAD_COUNTERPARTY_BACKCHAIN
        logger.debug { "ResolveTransactionsFlow.call(): Otherside Platform Version = '$counterpartyPlatformVersion': Batch mode = $batchMode" }

        if (initialTx != null) {
            fetchMissingAttachments(initialTx)
            fetchMissingNetworkParameters(initialTx)
        }

        val resolver = (serviceHub as ServiceHubCoreInternal).createTransactionsResolver(this)
        resolver.downloadDependencies(batchMode)

        logger.trace { "ResolveTransactionsFlow: Sending END." }
        otherSide.send(FetchDataFlow.Request.End) // Finish fetching data.

        // If transaction resolution is performed for a transaction where some states are relevant, then those should be
        // recorded if this has not already occurred.
        val usedStatesToRecord = if (statesToRecord == StatesToRecord.NONE) StatesToRecord.ONLY_RELEVANT else statesToRecord
        resolver.recordDependencies(usedStatesToRecord)
    }

    /**
     * Fetches the set of attachments required to verify the given transaction. If these are not already present, they will be fetched from
     * a remote peer.
     *
     * @param transaction The transaction to fetch attachments for
     * @return True if any attachments were fetched from a remote peer, false otherwise
     */
    // TODO: This could be done in parallel with other fetches for extra speed.
    @Suspendable
    fun fetchMissingAttachments(transaction: SignedTransaction): Boolean {
        val tx = transaction.coreTransaction
        val attachmentIds = when (tx) {
            is WireTransaction -> tx.attachments.toSet()
            is ContractUpgradeWireTransaction -> setOf(tx.legacyContractAttachmentId, tx.upgradedContractAttachmentId)
            else -> return false
        }
        val downloads = subFlow(FetchAttachmentsFlow(attachmentIds, otherSide)).downloaded
        return (downloads.isNotEmpty())
    }

    /**
     * Fetches the network parameters under which the given transaction was created. Note that if the transaction was created pre-V4, or if
     * the counterparty does not understand that network parameters may need to be fetched, no parameters will be requested.
     *
     * @param transaction The transaction to fetch the network parameters for, if the parameters are not already present
     * @return True if the network parameters were fetched from a remote peer, false otherwise
     */
    // TODO This can also be done in parallel. See comment to [fetchMissingAttachments] above.
    @Suspendable
    fun fetchMissingNetworkParameters(transaction: SignedTransaction): Boolean {
        return if (fetchNetParamsFromCounterpart) {
            transaction.networkParametersHash?.let {
                val downloads = subFlow(FetchNetworkParametersFlow(setOf(it), otherSide)).downloaded
                downloads.isNotEmpty()
            } ?: false
        } else {
            false
        }
    }
}
