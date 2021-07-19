package net.corda.notary.experimental.bftsmart

import co.paralleluniverse.fibers.Suspendable
import com.google.common.util.concurrent.SettableFuture
import net.corda.core.contracts.StateRef
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.SignedData
import net.corda.core.flows.*
import net.corda.core.identity.Party
import net.corda.core.internal.notary.NotaryInternalException
import net.corda.core.internal.notary.NotaryService
import net.corda.core.internal.notary.verifySignature
import net.corda.core.schemas.PersistentStateRef
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.transactions.CoreTransaction
import net.corda.core.transactions.FilteredTransaction
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.debug
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.unwrap
import net.corda.node.services.api.ServiceHubInternal
import net.corda.node.services.transactions.PersistentUniquenessProvider
import net.corda.node.utilities.AppendOnlyPersistentMap
import net.corda.nodeapi.internal.persistence.NODE_DATABASE_PREFIX
import java.security.PublicKey
import javax.persistence.Column
import javax.persistence.Entity
import javax.persistence.Id
import javax.persistence.Table
import kotlin.concurrent.thread

/**
 * A non-validating notary service operated by a group of parties that don't necessarily trust each other.
 *
 * A transaction is notarised when the consensus is reached by the cluster on its uniqueness, and time-window validity.
 */
class BFTSmartNotaryService(
        override val services: ServiceHubInternal,
        override val notaryIdentityKey: PublicKey
) : NotaryService() {
    companion object {
        private val log = contextLogger()
        @JvmStatic
        val serializationFilter
            get() = { clazz: Class<*> ->
                clazz.name.let {
                    it.startsWith("bftsmart.")
                            || it.startsWith("java.security.")
                            || it.startsWith("java.util.")
                            || it.startsWith("java.lang.")
                            || it.startsWith("java.net.")
                }
            }
    }

    private val notaryConfig = services.configuration.notary
            ?: throw IllegalArgumentException("Failed to register ${BFTSmartNotaryService::class.java}: notary configuration not present")

    private val bftSMaRtConfig = notaryConfig.bftSMaRt
            ?: throw IllegalArgumentException("Failed to register ${BFTSmartNotaryService::class.java}: BFT-Smart configuration not present")

    private val cluster: BFTSmart.Cluster = makeBFTCluster(notaryIdentityKey, bftSMaRtConfig)

    private fun makeBFTCluster(
            @Suppress("UNUSED_PARAMETER") notaryKey: PublicKey,
            @Suppress("UNUSED_PARAMETER") bftSMaRtConfig: BFTSmartConfig
    ): BFTSmart.Cluster {
        return object : BFTSmart.Cluster {
            override fun waitUntilAllReplicasHaveInitialized() {
                log.warn("A BFT replica may still be initializing, in which case the upcoming consensus change may cause it to spin.")
            }
        }
    }

    private val client: BFTSmart.Client
    private val replicaHolder = SettableFuture.create<Replica>()

    init {
        client = BFTSmartConfigInternal(bftSMaRtConfig.clusterAddresses, bftSMaRtConfig.debug, bftSMaRtConfig.exposeRaces)
                .use {
            val replicaId = bftSMaRtConfig.replicaId
            val configHandle = it.handle()
            // Replica startup must be in parallel with other replicas, otherwise the constructor may not return:
            thread(name = "BFT SMaRt replica $replicaId init", isDaemon = true) {
                configHandle.use {
                    val replica = Replica(it, replicaId, { createMap() }, services, notaryIdentityKey)
                    replicaHolder.set(replica)
                    log.info("BFT SMaRt replica $replicaId is running.")
                }
            }
                    BFTSmart.Client(it, replicaId, cluster, this)
        }
    }

    fun waitUntilReplicaHasInitialized() {
        log.debug { "Waiting for replica ${bftSMaRtConfig.replicaId} to initialize." }
        replicaHolder.getOrThrow() // It's enough to wait for the ServiceReplica constructor to return.
    }

    fun commitTransaction(payload: NotarisationPayload, otherSide: Party) = client.commitTransaction(payload, otherSide)

    override fun createServiceFlow(otherPartySession: FlowSession): FlowLogic<Void?> = ServiceFlow(otherPartySession, this)

    private class ServiceFlow(val otherSideSession: FlowSession, val service: BFTSmartNotaryService) : FlowLogic<Void?>() {
        @Suspendable
        override fun call(): Void? {
            val payload = otherSideSession.receive<NotarisationPayload>().unwrap { it }
            val response = commit(payload)
            otherSideSession.send(response)
            return null
        }

        private fun commit(payload: NotarisationPayload): NotarisationResponse {
            val response = service.commitTransaction(payload, otherSideSession.counterparty)
            when (response) {
                is BFTSmart.ClusterResponse.Error -> {
                    // TODO: here we assume that all error will be the same, but there might be invalid onces from mailicious nodes
                    val responseError = response.errors.first().verified()
                    throw NotaryException(responseError, payload.coreTransaction.id)
                }
                is BFTSmart.ClusterResponse.Signatures -> {
                    log.debug("All input states of transaction ${payload.coreTransaction.id} have been committed")
                    return NotarisationResponse(response.txSignatures)
                }
            }
        }
    }

    @Suppress("MagicNumber") // database column length
    @Entity
    @Table(name = "${NODE_DATABASE_PREFIX}bft_committed_txs")
    class CommittedTransaction(
            @Id
            @Column(name = "transaction_id", nullable = false, length = 80)
            val transactionId: String
    )

    @Entity
    @Table(name = "${NODE_DATABASE_PREFIX}bft_committed_states")
    class CommittedState(id: PersistentStateRef, consumingTxHash: String) : PersistentUniquenessProvider.BaseComittedState(id, consumingTxHash)

    private fun createMap(): AppendOnlyPersistentMap<StateRef, SecureHash, CommittedState, PersistentStateRef> {
        return AppendOnlyPersistentMap(
                cacheFactory = services.cacheFactory,
                name = "BFTNonValidatingNotaryService_transactions",
                toPersistentEntityKey = { PersistentStateRef(it.txhash.toString(), it.index) },
                fromPersistentEntity = {
                    //TODO null check will become obsolete after making DB/JPA columns not nullable
                    val txId = it.id.txId
                    val index = it.id.index
                    Pair(
                            StateRef(txhash = SecureHash.create(txId), index = index),
                            SecureHash.create(it.consumingTxHash)
                    )
                },
                toPersistentEntity = { (txHash, index): StateRef, id: SecureHash ->
                    CommittedState(
                            id = PersistentStateRef(txHash.toString(), index),
                            consumingTxHash = id.toString()
                    )
                },
                persistentEntityClass = CommittedState::class.java
        )
    }

    private class Replica(config: BFTSmartConfigInternal,
                          replicaId: Int,
                          createMap: () -> AppendOnlyPersistentMap<StateRef, SecureHash, CommittedState, PersistentStateRef>,
                          services: ServiceHubInternal,
                          notaryIdentityKey: PublicKey) : BFTSmart.Replica(config, replicaId, createMap, services, notaryIdentityKey) {

        override fun executeCommand(command: ByteArray): ByteArray {
            val commitRequest = command.deserialize<BFTSmart.CommitRequest>()
            verifyRequest(commitRequest)
            val response = verifyAndCommitTx(commitRequest.payload.coreTransaction, commitRequest.callerIdentity, commitRequest.payload.requestSignature)
            return response.serialize().bytes
        }

        private fun verifyAndCommitTx(transaction: CoreTransaction, callerIdentity: Party, requestSignature: NotarisationRequestSignature): BFTSmart.ReplicaResponse {
            return try {
                val id = transaction.id
                val inputs = transaction.inputs
                val references = transaction.references
                val notary = transaction.notary
                val timeWindow = (transaction as? FilteredTransaction)?.timeWindow
                @Suppress("DEPRECATION")
                if (notary !in services.myInfo.legalIdentities) throw NotaryInternalException(NotaryError.WrongNotary)
                commitInputStates(inputs, id, callerIdentity.name, requestSignature, timeWindow, references)
                log.debug { "Inputs committed successfully, signing $id" }
                BFTSmart.ReplicaResponse.Signature(sign(id))
            } catch (e: NotaryInternalException) {
                log.debug { "Error processing transaction: ${e.error}" }
                val serializedError = e.error.serialize()
                val errorSignature = sign(serializedError.bytes)
                val signedError = SignedData(serializedError, errorSignature)
                BFTSmart.ReplicaResponse.Error(signedError)
            }
        }

        private fun verifyRequest(commitRequest: BFTSmart.CommitRequest) {
            val transaction = commitRequest.payload.coreTransaction
            val notarisationRequest = NotarisationRequest(transaction.inputs, transaction.id)
            notarisationRequest.verifySignature(commitRequest.payload.requestSignature, commitRequest.callerIdentity)
        }
    }

    override fun start() {
    }

    override fun stop() {
        replicaHolder.getOrThrow().dispose()
        client.dispose()
    }
}
