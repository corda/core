package net.corda.coretests.internal

import net.corda.core.contracts.TransactionVerificationException
import net.corda.core.crypto.Crypto
import net.corda.core.crypto.SignableData
import net.corda.core.crypto.SignatureMetadata
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.NetworkParametersStorage
import net.corda.core.internal.createComponentGroups
import net.corda.core.node.NetworkParameters
import net.corda.core.node.NotaryInfo
import net.corda.core.node.ServiceHub
import net.corda.core.serialization.serialize
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.transactions.WireTransaction
import net.corda.core.utilities.getOrThrow
import net.corda.nodeapi.internal.createDevNetworkMapCa
import net.corda.nodeapi.internal.crypto.CertificateAndKeyPair
import net.corda.testing.common.internal.testNetworkParameters
import net.corda.testing.contracts.DummyContract
import net.corda.testing.core.singleIdentity
import net.corda.testing.node.MockNetwork
import net.corda.testing.node.MockNetworkParameters
import net.corda.testing.node.StartedMockNode
import net.corda.testing.node.internal.DUMMY_CONTRACTS_CORDAPP
import net.corda.testing.node.internal.cordappForClasses
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.After
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals

class NetworkParametersResolutionTest {
    private lateinit var defaultParams: NetworkParameters
    private lateinit var params2: NetworkParameters
    private lateinit var params3: NetworkParameters
    private val certKeyPair: CertificateAndKeyPair = createDevNetworkMapCa()
    private lateinit var mockNet: MockNetwork
    private lateinit var notaryNode: StartedMockNode
    private lateinit var megaCorpNode: StartedMockNode
    private lateinit var miniCorpNode: StartedMockNode
    private lateinit var megaCorpParty: Party
    private lateinit var miniCorpParty: Party
    private lateinit var notaryParty: Party

    @Before
    fun setup() {
        mockNet = MockNetwork(MockNetworkParameters(
                cordappsForAllNodes = listOf(DUMMY_CONTRACTS_CORDAPP, cordappForClasses(ResolveTransactionsFlowTest.TestFlow::class.java, ResolveTransactionsFlowTest.TestResponseFlow::class.java))))
        notaryNode = mockNet.defaultNotaryNode
        megaCorpNode = mockNet.createPartyNode(CordaX500Name("MegaCorp", "London", "GB"))
        miniCorpNode = mockNet.createPartyNode(CordaX500Name("MiniCorp", "London", "GB"))
        notaryParty = mockNet.defaultNotaryIdentity
        megaCorpParty = megaCorpNode.info.singleIdentity()
        miniCorpParty = miniCorpNode.info.singleIdentity()
        defaultParams = miniCorpNode.services.networkParameters
        params2 = testNetworkParameters(epoch = 2, minimumPlatformVersion = 3, notaries = listOf((NotaryInfo(notaryParty, true))))
        params3 = testNetworkParameters(epoch = 3, minimumPlatformVersion = 4, notaries = listOf((NotaryInfo(notaryParty, true))))
    }

    @After
    fun tearDown() {
        mockNet.stopNodes()
    }

    // This function is resolving and signing WireTransaction with special parameters.
    private fun TransactionBuilder.toSignedTransactionWithParameters(parameters: NetworkParameters?, services: ServiceHub): SignedTransaction {
        val wtx = toWireTransaction(services)
        val wtxWithHash = WireTransaction(
                createComponentGroups(
                        wtx.inputs,
                        wtx.outputs,
                        wtx.commands,
                        wtx.attachments,
                        wtx.notary,
                        wtx.timeWindow,
                        wtx.references,
                        parameters?.serialize()?.hash),
                wtx.privacySalt,
                0
        )
        val publicKey = services.myInfo.singleIdentity().owningKey
        val signatureMetadata = SignatureMetadata(services.myInfo.platformVersion, Crypto.findSignatureScheme(publicKey).schemeNumberID)
        val signableData = SignableData(wtxWithHash.id, signatureMetadata)
        val sig = services.keyManagementService.sign(signableData, publicKey)
        return SignedTransaction(wtxWithHash, listOf(sig))
    }

    // Similar to ResolveTransactionsFlowTest but creates transactions with given network parameters
    // First transaction in pair is dependency of the second one.
    private fun makeTransactions(parameters1: NetworkParameters?, parameters2: NetworkParameters?): Pair<SignedTransaction, SignedTransaction> {
        // Make a chain of custody of dummy states and insert into node A.
        val dummy1: SignedTransaction = DummyContract.generateInitial(0, notaryParty, megaCorpParty.ref(1)).let {
            val ptx = it.toSignedTransactionWithParameters(parameters1, megaCorpNode.services)
            notaryNode.services.addSignature(ptx, notaryParty.owningKey)
        }
        val dummy2: SignedTransaction = DummyContract.move(dummy1.tx.outRef(0), miniCorpParty).let {
            val ptx = it.toSignedTransactionWithParameters(parameters2, megaCorpNode.services)
            notaryNode.services.addSignature(ptx, notaryParty.owningKey)
        }
        megaCorpNode.transaction {
            megaCorpNode.services.recordTransactions(dummy1, dummy2)
            // Record parameters too.
            with(megaCorpNode.services.networkParametersService as NetworkParametersStorage) {
                parameters1?.let { saveParameters(certKeyPair.sign(it)) }
                parameters2?.let { saveParameters(certKeyPair.sign(it)) }
            }
        }
        return Pair(dummy1, dummy2)
    }

    @Test
    fun `parameters all null`() {
        val (stx1, stx2) = makeTransactions(null, null)
        assertThat(stx1.networkParametersHash).isNull()
        assertThat(stx2.networkParametersHash).isNull()
        val p = ResolveTransactionsFlowTest.TestFlow(setOf(stx2.id), megaCorpParty)
        val future = miniCorpNode.startFlow(p)
        mockNet.runNetwork()
        future.getOrThrow()
        miniCorpNode.transaction {
            assertEquals(stx1, miniCorpNode.services.validatedTransactions.getTransaction(stx1.id))
            assertEquals(stx2, miniCorpNode.services.validatedTransactions.getTransaction(stx2.id))
        }
    }

    @Test
    fun `transaction chain out of order parameters`() {
        val hash2 = params2.serialize().hash
        val hash3 = params3.serialize().hash
        val (stx1, stx2) = makeTransactions(params3, params2)
        assertThat(stx1.networkParametersHash).isEqualTo(hash3)
        assertThat(stx2.networkParametersHash).isEqualTo(hash2)
        miniCorpNode.transaction {
            assertThat(miniCorpNode.services.networkParametersService.lookup(hash2)).isNull()
            assertThat(miniCorpNode.services.networkParametersService.lookup(hash3)).isNull()
        }
        val p = ResolveTransactionsFlowTest.TestFlow(setOf(stx2.id), megaCorpParty)
        val future = miniCorpNode.startFlow(p)
        mockNet.runNetwork()
        assertThatExceptionOfType(TransactionVerificationException.TransactionNetworkParameterOrderingException::class.java).isThrownBy {
            future.getOrThrow()
        }.withMessageContaining("The network parameters epoch (${params2.epoch}) of this transaction " +
                "is older than the epoch (${params3.epoch}) of input state: ${stx2.inputs.first()}")
        miniCorpNode.transaction {
            assertThat(miniCorpNode.services.validatedTransactions.getTransaction(stx1.id)).isNull()
            assertThat(miniCorpNode.services.validatedTransactions.getTransaction(stx2.id)).isNull()
            // Even though the resolution failed, we should still have downloaded the parameters to the storage.
        }
    }

    @Test
    fun `request parameters that are not in the storage`() {
        val hash1 = defaultParams.serialize().hash
        val hash2 = params2.serialize().hash
        // Create two transactions on megaCorpNode
        val (stx1, stx2) = makeTransactions(defaultParams, params2)
        assertThat(stx1.networkParametersHash).isEqualTo(hash1)
        assertThat(stx2.networkParametersHash).isEqualTo(hash2)
        miniCorpNode.transaction {
            assertThat(miniCorpNode.services.networkParametersService.lookup(hash1)).isNotNull()
            assertThat(miniCorpNode.services.networkParametersService.lookup(hash2)).isNull()
        }
        // miniCorpNode resolves the stx2 from megaCorpParty
        val p = ResolveTransactionsFlowTest.TestFlow(setOf(stx2.id), megaCorpParty)
        val future = miniCorpNode.startFlow(p)
        mockNet.runNetwork()
        future.getOrThrow()
        miniCorpNode.transaction {
            // Check that parameters were downloaded to the storage.
            assertThat(miniCorpNode.services.networkParametersService.lookup(hash1)).isEqualTo(defaultParams)
            assertThat(miniCorpNode.services.networkParametersService.lookup(hash2)).isEqualTo(params2)
        }
    }

    @Test
    fun `transaction chain out of order parameters with default`() {
        val hash3 = params3.serialize().hash
        // stx1 with epoch 3 -> stx2 with default epoch, which is 1
        val (stx1, stx2) = makeTransactions(params3, null)
        assertThat(stx2.networkParametersHash).isNull()
        assertThat(stx1.networkParametersHash).isEqualTo(hash3)
        val p = ResolveTransactionsFlowTest.TestFlow(setOf(stx2.id), megaCorpParty)
        val future = miniCorpNode.startFlow(p)
        mockNet.runNetwork()
        assertThatExceptionOfType(TransactionVerificationException.TransactionNetworkParameterOrderingException::class.java).isThrownBy {
            future.getOrThrow()
        }.withMessageContaining("The network parameters epoch (${defaultParams.epoch}) of this transaction " +
                "is older than the epoch (${params3.epoch}) of input state: ${stx2.inputs.first()}")
        miniCorpNode.transaction {
            assertThat(miniCorpNode.services.validatedTransactions.getTransaction(stx1.id)).isNull()
            assertThat(miniCorpNode.services.validatedTransactions.getTransaction(stx2.id)).isNull()
            assertThat(miniCorpNode.services.networkParametersService.lookup(hash3)).isEqualTo(params3)
        }
    }

    @Test
    fun `incorrect triangle of transactions`() {
        // stx1 with epoch 2, stx2 with epoch 1, stx3 with epoch 3
        // stx1 -> stx2, stx1 -> stx3, stx2 -> stx3
        val stx1 = makeTransactions(params2, null).first

        val stx2 = DummyContract.move(stx1.tx.outRef(0), miniCorpParty).let { builder ->
            val ptx = builder.toSignedTransactionWithParameters(defaultParams, megaCorpNode.services)
            notaryNode.services.addSignature(ptx, notaryParty.owningKey)
        }

        val stx3 = DummyContract.move(listOf(stx1.tx.outRef(0), stx2.tx.outRef(0)), miniCorpParty).let { builder ->
            val ptx = builder.toSignedTransactionWithParameters(params3, megaCorpNode.services)
            notaryNode.services.addSignature(ptx, notaryParty.owningKey)
        }

        megaCorpNode.transaction {
            megaCorpNode.services.recordTransactions(stx2, stx3)
            (megaCorpNode.services.networkParametersService as NetworkParametersStorage).saveParameters(certKeyPair.sign(defaultParams))
            (megaCorpNode.services.networkParametersService as NetworkParametersStorage).saveParameters(certKeyPair.sign(params3))
        }

        val p = ResolveTransactionsFlowTest.TestFlow(setOf(stx3.id), megaCorpParty)
        val future = miniCorpNode.startFlow(p)
        mockNet.runNetwork()
        assertThatExceptionOfType(TransactionVerificationException.TransactionNetworkParameterOrderingException::class.java).isThrownBy {
            future.getOrThrow()
        }.withMessageContaining("The network parameters epoch (${defaultParams.epoch}) of this transaction " +
                "is older than the epoch (${params2.epoch}) of input state: ${stx2.inputs.first()}")
    }
}