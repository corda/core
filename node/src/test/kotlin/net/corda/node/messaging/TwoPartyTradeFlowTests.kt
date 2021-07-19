package net.corda.node.messaging

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.concurrent.CordaFuture
import net.corda.core.contracts.*
import net.corda.core.crypto.*
import net.corda.core.flows.*
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.AnonymousParty
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.FlowStateMachine
import net.corda.core.internal.concurrent.map
import net.corda.core.internal.rootCause
import net.corda.core.messaging.DataFeed
import net.corda.core.messaging.StateMachineTransactionMapping
import net.corda.core.node.services.TransactionStorage
import net.corda.core.node.services.Vault
import net.corda.core.node.services.vault.PageSpecification
import net.corda.core.serialization.CordaSerializable
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.toFuture
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.transactions.WireTransaction
import net.corda.core.utilities.days
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.toNonEmptySet
import net.corda.core.utilities.unwrap
import net.corda.finance.DOLLARS
import net.corda.finance.`issued by`
import net.corda.finance.contracts.CommercialPaper
import net.corda.finance.contracts.asset.CASH
import net.corda.finance.contracts.asset.Cash
import net.corda.finance.flows.TwoPartyTradeFlow.Buyer
import net.corda.finance.flows.TwoPartyTradeFlow.Seller
import net.corda.node.services.api.WritableTransactionStorage
import net.corda.node.services.persistence.DBTransactionStorage
import net.corda.node.services.persistence.checkpoints
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.testing.core.*
import net.corda.testing.dsl.LedgerDSL
import net.corda.testing.dsl.TestLedgerDSLInterpreter
import net.corda.testing.dsl.TestTransactionDSLInterpreter
import net.corda.testing.internal.LogHelper
import net.corda.testing.internal.TEST_TX_TIME
import net.corda.testing.internal.vault.VaultFiller
import net.corda.testing.node.internal.*
import net.corda.testing.node.ledger
import org.assertj.core.api.Assertions.assertThat
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import rx.Observable
import java.io.ByteArrayOutputStream
import java.util.*
import java.util.jar.JarOutputStream
import java.util.zip.ZipEntry
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * In this example, Alice wishes to sell her commercial paper to Bob in return for $1,000,000 and they wish to do
 * it on the ledger atomically. Therefore they must work together to build a transaction.
 *
 * We assume that Alice and Bob already found each other via some market, and have agreed the details already.
 */
// TODO These tests need serious cleanup.
@RunWith(Parameterized::class)
class TwoPartyTradeFlowTests(private val anonymous: Boolean) {
    companion object {
        @JvmStatic
        @Parameterized.Parameters(name = "Anonymous = {0}")
        fun data(): Collection<Boolean> = listOf(true, false)

        private val dummyNotary = TestIdentity(DUMMY_NOTARY_NAME, 20)
    }

    private lateinit var mockNet: InternalMockNetwork

    @Before
    fun before() {
        LogHelper.setLevel("platform.trade", "core.contract.TransactionGroup", "recordingmap")
    }

    @After
    fun after() {
        if (::mockNet.isInitialized) {
            mockNet.stopNodes()
        }
        LogHelper.reset("platform.trade", "core.contract.TransactionGroup", "recordingmap")
    }

    @Test(timeout=300_000)
	fun `trade cash for commercial paper`() {
        // We run this in parallel threads to help catch any race conditions that may exist. The other tests
        // we run in the unit test thread exclusively to speed things up, ensure deterministic results and
        // allow interruption half way through.
        mockNet = InternalMockNetwork(cordappsForAllNodes = listOf(FINANCE_CONTRACTS_CORDAPP), threadPerNode = true)
        val notaryNode = mockNet.defaultNotaryNode
        val notary = mockNet.defaultNotaryIdentity
        notaryNode.services.ledger(notary) {
            val aliceNode = mockNet.createPartyNode(ALICE_NAME)
            val bobNode = mockNet.createPartyNode(BOB_NAME)
            val bankNode = mockNet.createPartyNode(BOC_NAME)
            val alice = aliceNode.info.singleIdentity()
            val bank = bankNode.info.singleIdentity()
            val bob = bobNode.info.singleIdentity()
            val cashIssuer = bank.ref(1)
            val cpIssuer = bank.ref(1, 2, 3)

            aliceNode.internals.disableDBCloseOnStop()
            bobNode.internals.disableDBCloseOnStop()

            bobNode.database.transaction {
                VaultFiller(bobNode.services, dummyNotary, notary, ::Random).fillWithSomeTestCash(2000.DOLLARS, bankNode.services, 3, 10, cashIssuer)
            }

            val alicesFakePaper = aliceNode.database.transaction {
                fillUpForSeller(false, cpIssuer, alice,
                        1200.DOLLARS `issued by` bank.ref(0), null, notary).second
            }

            insertFakeTransactions(alicesFakePaper, aliceNode, alice, notaryNode, bankNode)

            val (bobStateMachine, aliceResult) = runBuyerAndSeller(notary, bob, aliceNode, bobNode,
                    "alice's paper".outputStateAndRef())

            // TODO: Verify that the result was inserted into the transaction database.
            // assertEquals(bobResult.get(), aliceNode.storage.validatedTransactions[aliceResult.get().id])
            assertEquals(aliceResult.getOrThrow(), bobStateMachine.getOrThrow().resultFuture.getOrThrow())

            aliceNode.dispose()
            bobNode.dispose()

            aliceNode.database.transaction {
                assertThat(aliceNode.internals.checkpointStorage.checkpoints()).isEmpty()
            }
            aliceNode.internals.manuallyCloseDB()
            bobNode.database.transaction {
                assertThat(bobNode.internals.checkpointStorage.checkpoints()).isEmpty()
            }
            bobNode.internals.manuallyCloseDB()
        }
    }

    @Test(expected = InsufficientBalanceException::class, timeout=300_000)
    fun `trade cash for commercial paper fails using soft locking`() {
        mockNet = InternalMockNetwork(cordappsForAllNodes = listOf(FINANCE_CONTRACTS_CORDAPP), threadPerNode = true)
        val notaryNode = mockNet.defaultNotaryNode
        notaryNode.services.ledger(notaryNode.info.singleIdentity()) {
            val aliceNode = mockNet.createPartyNode(ALICE_NAME)
            val bobNode = mockNet.createPartyNode(BOB_NAME)
            val bankNode = mockNet.createPartyNode(BOC_NAME)
            val alice = aliceNode.info.singleIdentity()
            val bank = bankNode.info.singleIdentity()
            val bob = bobNode.info.singleIdentity()
            val issuer = bank.ref(1)
            val notary = mockNet.defaultNotaryIdentity

            aliceNode.internals.disableDBCloseOnStop()
            bobNode.internals.disableDBCloseOnStop()

            val cashStates = bobNode.database.transaction {
                VaultFiller(bobNode.services, dummyNotary, notary, ::Random).fillWithSomeTestCash(2000.DOLLARS, bankNode.services, 3, issuer)
            }

            val alicesFakePaper = aliceNode.database.transaction {
                fillUpForSeller(false, issuer, alice,
                        1200.DOLLARS `issued by` bank.ref(0), null, notary).second
            }

            insertFakeTransactions(alicesFakePaper, aliceNode, alice, notaryNode, bankNode)

            val cashLockId = UUID.randomUUID()
            bobNode.database.transaction {
                // lock the cash states with an arbitrary lockId (to prevent the Buyer flow from claiming the states)
                val refs = cashStates.states.map { it.ref }
                if (refs.isNotEmpty()) {
                    bobNode.services.vaultService.softLockReserve(cashLockId, refs.toNonEmptySet())
                }
            }

            val (bobStateMachine, aliceResult) = runBuyerAndSeller(notary, bob, aliceNode, bobNode,
                    "alice's paper".outputStateAndRef())

            assertEquals(aliceResult.getOrThrow(), bobStateMachine.getOrThrow().resultFuture.getOrThrow())

            aliceNode.dispose()
            bobNode.dispose()

            aliceNode.database.transaction {
                assertThat(aliceNode.internals.checkpointStorage.checkpoints()).isEmpty()
            }
            aliceNode.internals.manuallyCloseDB()
            bobNode.database.transaction {
                assertThat(bobNode.internals.checkpointStorage.checkpoints()).isEmpty()
            }
            bobNode.internals.manuallyCloseDB()
        }
    }

    @Test(timeout=300_000)
	fun `shutdown and restore`() {
        mockNet = InternalMockNetwork(cordappsForAllNodes = listOf(FINANCE_CONTRACTS_CORDAPP))
        val notaryNode = mockNet.defaultNotaryNode
        val notary = mockNet.defaultNotaryIdentity
        notaryNode.services.ledger(notary) {
            val aliceNode = mockNet.createPartyNode(ALICE_NAME)
            var bobNode = mockNet.createPartyNode(BOB_NAME)
            val bankNode = mockNet.createPartyNode(BOC_NAME)
            aliceNode.internals.disableDBCloseOnStop()
            bobNode.internals.disableDBCloseOnStop()

            val bobAddr = bobNode.network.myAddress
            mockNet.runNetwork() // Clear network map registration messages

            val alice = aliceNode.info.singleIdentity()
            val bank = bankNode.info.singleIdentity()
            val bob = bobNode.info.singleIdentity()
            val issuer = bank.ref(1, 2, 3)

            bobNode.database.transaction {
                VaultFiller(bobNode.services, dummyNotary, notary, ::Random).fillWithSomeTestCash(2000.DOLLARS, bankNode.services, 3, 10, issuer)
            }
            val alicesFakePaper = aliceNode.database.transaction {
                fillUpForSeller(false, issuer, alice,
                        1200.DOLLARS `issued by` bank.ref(0), null, notary).second
            }
            insertFakeTransactions(alicesFakePaper, aliceNode, alice, notaryNode, bankNode)
            val aliceFuture = runBuyerAndSeller(notary, bob, aliceNode, bobNode, "alice's paper".outputStateAndRef()).sellerResult

            // Everything is on this thread so we can now step through the flow one step at a time.
            // Seller Alice already sent a message to Buyer Bob. Pump once:
            bobNode.pumpReceive()

            // Bob sends a couple of queries for the dependencies back to Alice. Alice reponds.
            aliceNode.pumpReceive()
            bobNode.pumpReceive()
            aliceNode.pumpReceive()
            bobNode.pumpReceive()
            aliceNode.pumpReceive()
            bobNode.pumpReceive()

            // OK, now Bob has sent the partial transaction back to Alice and is waiting for Alice's signature.
            bobNode.database.transaction {
                assertThat(bobNode.internals.checkpointStorage.checkpoints()).hasSize(1)
            }

            val storage = bobNode.services.validatedTransactions
            val bobTransactionsBeforeCrash = bobNode.database.transaction {
                (storage as DBTransactionStorage).transactions
            }
            assertThat(bobTransactionsBeforeCrash).isNotEmpty

            // .. and let's imagine that Bob's computer has a power cut. He now has nothing now beyond what was on disk.
            bobNode.dispose()

            // Alice doesn't know that and carries on: she wants to know about the cash transactions he's trying to use.
            // She will wait around until Bob comes back.
            assertThat(aliceNode.pumpReceive()).isNotNull()

            // FIXME: Knowledge of confidential identities is lost on node shutdown, so Bob's node now refuses to sign the
            //        transaction because it has no idea who the parties are.

            // ... bring the node back up ... the act of constructing the SMM will re-register the message handlers
            // that Bob was waiting on before the reboot occurred.
            bobNode = mockNet.createNode(InternalMockNodeParameters(bobAddr.id, BOB_NAME))
            // Find the future representing the result of this state machine again.
            val bobFuture = bobNode.smm.findStateMachines(BuyerAcceptor::class.java).single().second

            // And off we go again.
            mockNet.runNetwork()

            // Bob is now finished and has the same transaction as Alice.
            assertThat(bobFuture.getOrThrow()).isEqualTo(aliceFuture.getOrThrow())

            assertThat(bobNode.smm.findStateMachines(Buyer::class.java)).isEmpty()
            bobNode.database.transaction {
                assertThat(bobNode.internals.checkpointStorage.checkpoints()).isEmpty()
            }
            aliceNode.database.transaction {
                assertThat(aliceNode.internals.checkpointStorage.checkpoints()).isEmpty()
            }

            bobNode.database.transaction {
                val restoredBobTransactions = bobTransactionsBeforeCrash.filter {
                    bobNode.services.validatedTransactions.getTransaction(it.id) != null
                }
                assertThat(restoredBobTransactions).containsAll(bobTransactionsBeforeCrash)
            }

            aliceNode.internals.manuallyCloseDB()
            bobNode.internals.manuallyCloseDB()
        }
    }

    // Creates a mock node with an overridden storage service that uses a RecordingMap, that lets us test the order
    // of gets and puts.
    private fun makeNodeWithTracking(name: CordaX500Name): TestStartedNode {
        // Create a node in the mock network ...
        return mockNet.createNode(InternalMockNodeParameters(legalName = name), nodeFactory = { args ->
            object : InternalMockNetwork.MockNode(args) {
                // That constructs a recording tx storage
                override fun makeTransactionStorage(transactionCacheSizeBytes: Long): WritableTransactionStorage {
                    return RecordingTransactionStorage(database, super.makeTransactionStorage(transactionCacheSizeBytes))
                }
            }
        })
    }

    @Test(timeout=300_000)
	fun `check dependencies of sale asset are resolved`() {
        mockNet = InternalMockNetwork(cordappsForAllNodes = listOf(FINANCE_CONTRACTS_CORDAPP))
        val notaryNode = mockNet.defaultNotaryNode
        val aliceNode = makeNodeWithTracking(ALICE_NAME)
        val bobNode = makeNodeWithTracking(BOB_NAME)
        val bankNode = makeNodeWithTracking(BOC_NAME)
        mockNet.runNetwork()
        val notary = mockNet.defaultNotaryIdentity
        val alice = aliceNode.info.singleIdentity()
        val bob = bobNode.info.singleIdentity()
        val bank = bankNode.info.singleIdentity()
        val issuer = bank.ref(1, 2, 3)
        aliceNode.services.ledger(notary) {
            // Insert a prospectus type attachment into the commercial paper transaction.
            val stream = ByteArrayOutputStream()
            JarOutputStream(stream).use {
                it.putNextEntry(ZipEntry("Prospectus.txt"))
                it.write("Our commercial paper is top notch stuff".toByteArray())
                it.closeEntry()
            }
            val attachmentID = aliceNode.database.transaction {
                attachment(stream.toByteArray().inputStream())
            }

            val (_, bobsFakeCash, bobsSignedTxns) = bobNode.database.transaction {
                fillUpForBuyerAndInsertFakeTransactions(false, issuer, AnonymousParty(bob.owningKey), notary, bobNode, bob, notaryNode, bankNode)
            }
            val alicesFakePaper = aliceNode.database.transaction {
                fillUpForSeller(false, issuer, alice,
                        1200.DOLLARS `issued by` bank.ref(0), attachmentID, notary).second
            }
            val alicesSignedTxns = insertFakeTransactions(alicesFakePaper, aliceNode, alice, notaryNode, bankNode)

            mockNet.runNetwork() // Clear network map registration messages

            runBuyerAndSeller(notary, bob, aliceNode, bobNode, "alice's paper".outputStateAndRef())

            mockNet.runNetwork()

            run {
                val records = (bobNode.services.validatedTransactions as RecordingTransactionStorage).records
                // Check Bobs's database accesses as Bob's cash transactions are downloaded by Alice.
                records.expectEvents(isStrict = false) {
                    sequence(
                            // Buyer Bob is told about Alice's commercial paper, but doesn't know it ..
                            expect(TxRecord.Get(alicesFakePaper[0].id)),
                            // He asks and gets the tx, validates it, sees it's a self issue with no dependencies, stores.
                            expect(TxRecord.Add(alicesSignedTxns.values.first())),
                            // Alice gets Bob's proposed transaction and doesn't know his two cash states. She asks, Bob answers.
                            expect(TxRecord.Get(bobsFakeCash[1].id)),
                            expect(TxRecord.Get(bobsFakeCash[2].id)),
                            // Alice notices that Bob's cash txns depend on a third tx she also doesn't know. She asks, Bob answers.
                            expect(TxRecord.Get(bobsFakeCash[0].id))
                    )
                }

                // Bob has downloaded the attachment.
                bobNode.database.transaction {
                    bobNode.services.attachments.openAttachment(attachmentID)!!.openAsJAR().use {
                        it.nextJarEntry
                        val contents = it.reader().readText()
                        assertTrue(contents.contains("Our commercial paper is top notch stuff"))
                    }
                }
            }

            // And from Alice's perspective ...
            run {
                val records = (aliceNode.services.validatedTransactions as RecordingTransactionStorage).records
                records.expectEvents(isStrict = false) {
                    sequence(
                            // Seller Alice sends her seller info to Bob, who wants to check the asset for sale.
                            // He requests, Alice looks up in her DB to send the tx to Bob
                            expect(TxRecord.Get(alicesFakePaper[0].id)),
                            // Seller Alice gets a proposed tx which depends on Bob's two cash txns and her own tx.
                            expect(TxRecord.Get(bobsFakeCash[1].id)),
                            expect(TxRecord.Get(bobsFakeCash[2].id)),
                            expect(TxRecord.Get(alicesFakePaper[0].id)),
                            // Alice notices that Bob's cash txns depend on a third tx she also doesn't know.
                            expect(TxRecord.Get(bobsFakeCash[0].id)),
                            // Bob answers with the transactions that are now all verifiable, as Alice bottomed out.
                            // Bob's transactions are valid, so she commits to the database
                            //expect(TxRecord.Add(bobsSignedTxns[bobsFakeCash[0].id]!!)), //TODO investigate missing event after introduction of signature constraints non-downgrade rule
                            expect(TxRecord.Get(bobsFakeCash[0].id)), // Verify
                            expect(TxRecord.Add(bobsSignedTxns[bobsFakeCash[2].id]!!)),
                            expect(TxRecord.Get(bobsFakeCash[0].id)), // Verify
                            expect(TxRecord.Add(bobsSignedTxns[bobsFakeCash[1].id]!!)),
                            // Now she verifies the transaction is contract-valid (not signature valid) which means
                            // looking up the states again.
                            expect(TxRecord.Get(bobsFakeCash[1].id)),
                            expect(TxRecord.Get(bobsFakeCash[2].id)),
                            expect(TxRecord.Get(alicesFakePaper[0].id)),
                            // Alice needs to look up the input states to find out which Notary they point to
                            expect(TxRecord.Get(bobsFakeCash[1].id)),
                            expect(TxRecord.Get(bobsFakeCash[2].id)),
                            expect(TxRecord.Get(alicesFakePaper[0].id))
                    )
                }
            }
        }
    }

    @Test(timeout=300_000)
	fun `track works`() {
        mockNet = InternalMockNetwork(cordappsForAllNodes = listOf(FINANCE_CONTRACTS_CORDAPP))
        val notaryNode = mockNet.defaultNotaryNode
        val aliceNode = makeNodeWithTracking(ALICE_NAME)
        val bobNode = makeNodeWithTracking(BOB_NAME)
        val bankNode = makeNodeWithTracking(BOC_NAME)

        val notary = mockNet.defaultNotaryIdentity
        val alice: Party = aliceNode.info.singleIdentity()
        val bank: Party = bankNode.info.singleIdentity()
        val bob = bobNode.info.singleIdentity()
        val issuer = bank.ref(1, 2, 3)
        aliceNode.services.ledger(notary) {
            // Insert a prospectus type attachment into the commercial paper transaction.
            val stream = ByteArrayOutputStream()
            JarOutputStream(stream).use {
                it.putNextEntry(ZipEntry("Prospectus.txt"))
                it.write("Our commercial paper is top notch stuff".toByteArray())
                it.closeEntry()
            }
            val attachmentID = aliceNode.database.transaction {
                attachment(stream.toByteArray().inputStream())
            }

            val bobsKey = bobNode.services.keyManagementService.keys.single()
            val bobsFakeCash = bobNode.database.transaction {
                fillUpForBuyerAndInsertFakeTransactions(false, issuer, AnonymousParty(bobsKey), notary, bobNode, bob, notaryNode, bankNode)
            }.second

            val alicesFakePaper = aliceNode.database.transaction {
                fillUpForSeller(false, issuer, alice,
                        1200.DOLLARS `issued by` bank.ref(0), attachmentID, notary).second
            }

            insertFakeTransactions(alicesFakePaper, aliceNode, alice, notaryNode, bankNode)

            val aliceTxStream = aliceNode.services.validatedTransactions.track().updates
            val aliceTxMappings = with(aliceNode) {
                database.transaction { services.stateMachineRecordedTransactionMapping.track().updates }
            }
            val aliceSmId = runBuyerAndSeller(notary, bob, aliceNode, bobNode,
                    "alice's paper".outputStateAndRef()).sellerId

            mockNet.runNetwork()

            // We need to declare this here, if we do it inside [expectEvents] kotlin throws an internal compiler error(!).
            val aliceTxExpectations = sequence(
                    expect { tx: SignedTransaction ->
                        require(tx.id == bobsFakeCash[0].id)
                    },
                    expect { tx: SignedTransaction ->
                        require(tx.id == bobsFakeCash[2].id)
                    },
                    expect { tx: SignedTransaction ->
                        require(tx.id == bobsFakeCash[1].id)
                    }
            )
            aliceTxStream.expectEvents { aliceTxExpectations }
            val aliceMappingExpectations = sequence(
                    expect<StateMachineTransactionMapping> { (stateMachineRunId, transactionId) ->
                        require(stateMachineRunId == aliceSmId)
                        require(transactionId == bobsFakeCash[0].id)
                    },
                    expect<StateMachineTransactionMapping> { (stateMachineRunId, transactionId) ->
                        require(stateMachineRunId == aliceSmId)
                        require(transactionId == bobsFakeCash[2].id)
                    },
                    expect { (stateMachineRunId, transactionId) ->
                        require(stateMachineRunId == aliceSmId)
                        require(transactionId == bobsFakeCash[1].id)
                    }
            )
            aliceTxMappings.expectEvents { aliceMappingExpectations }
        }
    }

    @Test(timeout=300_000)
    fun `trackWithPagingSpec works`() {
        mockNet = InternalMockNetwork(cordappsForAllNodes = listOf(FINANCE_CONTRACTS_CORDAPP))
        val notaryNode = mockNet.defaultNotaryNode
        val aliceNode = makeNodeWithTracking(ALICE_NAME)
        val bobNode = makeNodeWithTracking(BOB_NAME)
        val bankNode = makeNodeWithTracking(BOC_NAME)

        val notary = mockNet.defaultNotaryIdentity
        val alice: Party = aliceNode.info.singleIdentity()
        val bank: Party = bankNode.info.singleIdentity()
        val bob = bobNode.info.singleIdentity()
        val issuer = bank.ref(1, 2, 3)
        aliceNode.services.ledger(notary) {
            // Insert a prospectus type attachment into the commercial paper transaction.
            val stream = ByteArrayOutputStream()
            JarOutputStream(stream).use {
                it.putNextEntry(ZipEntry("Prospectus.txt"))
                it.write("Our commercial paper is top notch stuff".toByteArray())
                it.closeEntry()
            }
            val attachmentID = aliceNode.database.transaction {
                attachment(stream.toByteArray().inputStream())
            }

            val bobsKey = bobNode.services.keyManagementService.keys.single()
            val bobsFakeCash = bobNode.database.transaction {
                fillUpForBuyerAndInsertFakeTransactions(false, issuer, AnonymousParty(bobsKey), notary, bobNode, bob, notaryNode, bankNode)
            }.second

            val alicesFakePaper = aliceNode.database.transaction {
                fillUpForSeller(false, issuer, alice,
                        1200.DOLLARS `issued by` bank.ref(0), attachmentID, notary).second
            }

            insertFakeTransactions(alicesFakePaper, aliceNode, alice, notaryNode, bankNode)

            val aliceTxStream = aliceNode.services.validatedTransactions.trackWithPagingSpec(PageSpecification(1, 200)).updates
            val aliceTxMappings = with(aliceNode) {
                database.transaction { services.stateMachineRecordedTransactionMapping.track().updates }
            }
            val aliceSmId = runBuyerAndSeller(notary, bob, aliceNode, bobNode,
                    "alice's paper".outputStateAndRef()).sellerId

            mockNet.runNetwork()

            // test paged tx snapshot for alice
            val aliceTxPagedSnapshot = aliceNode.services.validatedTransactions.trackWithPagingSpec(PageSpecification(2,2)).snapshot
            assertEquals(aliceTxPagedSnapshot.totalTransactionsAvailable,5)
            assertEquals(aliceTxPagedSnapshot.transactions.size, 2)
            assertEquals(aliceTxPagedSnapshot.transactions[0].signedTransaction.id, bobsFakeCash[2].id)
            assertEquals(aliceTxPagedSnapshot.transactions[1].signedTransaction.id, bobsFakeCash[0].id)
            // We need to declare this here, if we do it inside [expectEvents] kotlin throws an internal compiler error(!).
            val aliceTxExpectations = sequence(
                    expect { tx: SignedTransaction ->
                        require(tx.id == bobsFakeCash[0].id)
                    },
                    expect { tx: SignedTransaction ->
                        require(tx.id == bobsFakeCash[2].id)
                    },
                    expect { tx: SignedTransaction ->
                        require(tx.id == bobsFakeCash[1].id)
                    }
            )
            aliceTxStream.expectEvents { aliceTxExpectations }
            val aliceMappingExpectations = sequence(
                    expect<StateMachineTransactionMapping> { (stateMachineRunId, transactionId) ->
                        require(stateMachineRunId == aliceSmId)
                        require(transactionId == bobsFakeCash[0].id)
                    },
                    expect<StateMachineTransactionMapping> { (stateMachineRunId, transactionId) ->
                        require(stateMachineRunId == aliceSmId)
                        require(transactionId == bobsFakeCash[2].id)
                    },
                    expect { (stateMachineRunId, transactionId) ->
                        require(stateMachineRunId == aliceSmId)
                        require(transactionId == bobsFakeCash[1].id)
                    }
            )
            aliceTxMappings.expectEvents { aliceMappingExpectations }
        }
    }

    @Test(timeout=300_000)
	fun `dependency with error on buyer side`() {
        mockNet = InternalMockNetwork(cordappsForAllNodes = listOf(FINANCE_CONTRACTS_CORDAPP))
        mockNet.defaultNotaryNode.services.ledger(mockNet.defaultNotaryIdentity) {
            runWithError(true, false, "at least one cash input")
        }
    }

    @Test(timeout=300_000)
	fun `dependency with error on seller side`() {
        mockNet = InternalMockNetwork(cordappsForAllNodes = listOf(FINANCE_CONTRACTS_CORDAPP))
        mockNet.defaultNotaryNode.services.ledger(mockNet.defaultNotaryIdentity) {
            runWithError(false, true, "Issuances have a time-window")
        }
    }

    private data class RunResult(
            // The buyer is not created immediately, only when the seller starts running
            val buyer: CordaFuture<FlowStateMachine<*>>,
            val sellerResult: CordaFuture<SignedTransaction>,
            val sellerId: StateMachineRunId
    )

    private fun runBuyerAndSeller(notary: Party,
                                  buyer: Party,
                                  sellerNode: TestStartedNode,
                                  buyerNode: TestStartedNode,
                                  assetToSell: StateAndRef<OwnableState>): RunResult {
        val buyerFlows: Observable<out FlowLogic<*>> = buyerNode.registerInitiatedFlow(BuyerAcceptor::class.java)
        val firstBuyerFiber = buyerFlows.toFuture().map { it.stateMachine }
        val seller = SellerInitiator(buyer, notary, assetToSell, 1000.DOLLARS, anonymous)
        val sellerResult = sellerNode.services.startFlow(seller).resultFuture
        return RunResult(firstBuyerFiber, sellerResult, seller.stateMachine.id)
    }

    @InitiatingFlow
    class SellerInitiator(private val buyer: Party,
                          private val notary: Party,
                          private val assetToSell: StateAndRef<OwnableState>,
                          private val price: Amount<Currency>,
                          private val anonymous: Boolean) : FlowLogic<SignedTransaction>() {
        @Suspendable
        override fun call(): SignedTransaction {
            val myPartyAndCert = if (anonymous) {
                serviceHub.keyManagementService.freshKeyAndCert(ourIdentityAndCert, false)
            } else {
                ourIdentityAndCert
            }
            val buyerSession = initiateFlow(buyer)
            buyerSession.send(TestTx(notary, price, anonymous))
            return subFlow(Seller(
                    buyerSession,
                    assetToSell,
                    price,
                    myPartyAndCert))
        }
    }

    @InitiatedBy(SellerInitiator::class)
    class BuyerAcceptor(private val sellerSession: FlowSession) : FlowLogic<SignedTransaction>() {
        @Suspendable
        override fun call(): SignedTransaction {
            val (notary, price, anonymous) = sellerSession.receive<TestTx>().unwrap {
                require(serviceHub.networkMapCache.isNotary(it.notaryIdentity)) { "${it.notaryIdentity} is not a notary" }
                it
            }
            return subFlow(Buyer(sellerSession, notary, price, CommercialPaper.State::class.java, anonymous))
        }
    }

    @CordaSerializable
    data class TestTx(val notaryIdentity: Party, val price: Amount<Currency>, val anonymous: Boolean)

    private fun LedgerDSL<TestTransactionDSLInterpreter, TestLedgerDSLInterpreter>.runWithError(
            bobError: Boolean,
            aliceError: Boolean,
            expectedMessageSubstring: String
    ) {
        val notaryNode = mockNet.defaultNotaryNode
        val aliceNode = mockNet.createPartyNode(ALICE_NAME)
        val bobNode = mockNet.createPartyNode(BOB_NAME)
        val bankNode = mockNet.createPartyNode(BOC_NAME)

        val notary = mockNet.defaultNotaryIdentity
        val alice = aliceNode.info.singleIdentity()
        val bob = bobNode.info.singleIdentity()
        val bank = bankNode.info.singleIdentity()
        val issuer = bank.ref(1, 2, 3)

        bobNode.database.transaction {
            fillUpForBuyerAndInsertFakeTransactions(bobError, issuer, bob, notary, bobNode, bob, notaryNode, bankNode).second
        }
        val alicesFakePaper = aliceNode.database.transaction {
            fillUpForSeller(aliceError, issuer, alice, 1200.DOLLARS `issued by` issuer, null, notary).second
        }

        insertFakeTransactions(alicesFakePaper, aliceNode, alice, notaryNode, bankNode)

        val (bobStateMachine, aliceResult) = runBuyerAndSeller(notary, bob, aliceNode, bobNode, "alice's paper".outputStateAndRef())

        mockNet.runNetwork()

        val e = assertFailsWith<TransactionVerificationException> {
            if (bobError)
                aliceResult.getOrThrow()
            else
                bobStateMachine.getOrThrow().resultFuture.getOrThrow()
        }
        val underlyingMessage = e.rootCause.message!!
        if (expectedMessageSubstring !in underlyingMessage) {
            assertEquals(expectedMessageSubstring, underlyingMessage)
        }
    }

    private fun insertFakeTransactions(
            wtxToSign: List<WireTransaction>,
            node: TestStartedNode,
            identity: Party,
            notaryNode: TestStartedNode,
            vararg extraSigningNodes: TestStartedNode): Map<SecureHash, SignedTransaction> {
        val notaryParty = mockNet.defaultNotaryIdentity
        val signed = wtxToSign.map {
            val id = it.id
            val sigs = mutableListOf<TransactionSignature>()
            val nodeKey = identity.owningKey
            sigs += node.services.keyManagementService.sign(
                    SignableData(id, SignatureMetadata(1, Crypto.findSignatureScheme(nodeKey).schemeNumberID)),
                    nodeKey
            )
            sigs += notaryNode.services.keyManagementService.sign(
                    SignableData(id, SignatureMetadata(1, Crypto.findSignatureScheme(notaryParty.owningKey).schemeNumberID)),
                    notaryParty.owningKey
            )
            extraSigningNodes.forEach { currentNode ->
                val currentIdentity = currentNode.info.singleIdentity()
                sigs += currentNode.services.keyManagementService.sign(
                        SignableData(id, SignatureMetadata(
                                1,
                                Crypto.findSignatureScheme(currentIdentity.owningKey).schemeNumberID)),
                        currentIdentity.owningKey)
            }
            SignedTransaction(it, sigs)
        }
        return node.database.transaction {
            node.services.recordTransactions(signed)
            val validatedTransactions = node.services.validatedTransactions
            if (validatedTransactions is RecordingTransactionStorage) {
                validatedTransactions.records.clear()
            }
            signed.associateBy { it.id }
        }
    }

    private fun LedgerDSL<TestTransactionDSLInterpreter, TestLedgerDSLInterpreter>.fillUpForBuyerAndInsertFakeTransactions(
            withError: Boolean,
            issuer: PartyAndReference,
            owner: AbstractParty,
            notary: Party,
            node: TestStartedNode,
            identity: Party,
            notaryNode: TestStartedNode,
            vararg extraSigningNodes: TestStartedNode
            ): Triple<Vault<ContractState>, List<WireTransaction>, Map<SecureHash,SignedTransaction>> {
        val interimOwner = issuer.party
        // Bob (Buyer) has some cash he got from the Bank of Elbonia, Alice (Seller) has some commercial paper she
        // wants to sell to Bob.
        val eb1 = transaction(transactionBuilder = TransactionBuilder(notary = notary)) {
            // Issued money to itself.
            output(Cash.PROGRAM_ID, "elbonian money 1", notary = notary, contractState = 800.DOLLARS.CASH issuedBy issuer ownedBy interimOwner)
            output(Cash.PROGRAM_ID, "elbonian money 2", notary = notary, contractState = 1000.DOLLARS.CASH issuedBy issuer ownedBy interimOwner)
            if (!withError) {
                command(issuer.party.owningKey, Cash.Commands.Issue())
            } else {
                // Put a broken command on so at least a signature is created
                command(issuer.party.owningKey, Cash.Commands.Move())
            }
            timeWindow(TEST_TX_TIME)
            if (withError) {
                this.fails()
            } else {
                this.verifies()
            }
        }
        val eb1Txns = insertFakeTransactions(listOf(eb1), node, identity, notaryNode, *extraSigningNodes)

        // Bob gets some cash onto the ledger from BoE
        val bc1 = transaction(transactionBuilder = TransactionBuilder(notary = notary)) {
            input("elbonian money 1")
            output(Cash.PROGRAM_ID, "bob cash 1", notary = notary, contractState = 800.DOLLARS.CASH issuedBy issuer ownedBy owner)
            command(interimOwner.owningKey, Cash.Commands.Move())
            this.verifies()
        }
        val eb2Txns = insertFakeTransactions(listOf(bc1), node, identity, notaryNode, *extraSigningNodes)

        val bc2 = transaction(transactionBuilder = TransactionBuilder(notary = notary)) {
            input("elbonian money 2")
            output(Cash.PROGRAM_ID, "bob cash 2", notary = notary, contractState = 300.DOLLARS.CASH issuedBy issuer ownedBy owner)
            output(Cash.PROGRAM_ID, notary = notary, contractState = 700.DOLLARS.CASH issuedBy issuer ownedBy interimOwner)   // Change output.
            command(interimOwner.owningKey, Cash.Commands.Move())
            this.verifies()
        }
        val eb3Txns = insertFakeTransactions(listOf(bc2), node, identity, notaryNode, *extraSigningNodes)

        val vault = Vault<ContractState>(listOf("bob cash 1".outputStateAndRef(), "bob cash 2".outputStateAndRef()))
        return Triple(vault, listOf(eb1, bc1, bc2), eb1Txns + eb2Txns + eb3Txns)
    }

    private fun LedgerDSL<TestTransactionDSLInterpreter, TestLedgerDSLInterpreter>.fillUpForSeller(
            withError: Boolean,
            issuer: PartyAndReference,
            owner: AbstractParty,
            amount: Amount<Issued<Currency>>,
            attachmentID: SecureHash?,
            notary: Party): Pair<Vault<ContractState>, List<WireTransaction>> {
        val ap = transaction(transactionBuilder = TransactionBuilder(notary = notary)) {
            output(CommercialPaper.CP_PROGRAM_ID, "alice's paper", notary = notary,
                    contractState = CommercialPaper.State(issuer, owner, amount, TEST_TX_TIME + 7.days))
            command(issuer.party.owningKey, CommercialPaper.Commands.Issue())
            if (!withError)
                timeWindow(time = TEST_TX_TIME)
            if (attachmentID != null)
                attachment(attachmentID)
            if (withError) {
                this.fails()
            } else {
                this.verifies()
            }
        }

        val vault = Vault<ContractState>(listOf("alice's paper".outputStateAndRef()))
        return Pair(vault, listOf(ap))
    }

    class RecordingTransactionStorage(
            private val database: CordaPersistence,
            private val delegate: WritableTransactionStorage
    ) : WritableTransactionStorage, SingletonSerializeAsToken() {
        override fun trackTransaction(id: SecureHash): CordaFuture<SignedTransaction> {
            return database.transaction {
                delegate.trackTransaction(id)
            }
        }

        override fun trackTransactionWithNoWarning(id: SecureHash): CordaFuture<SignedTransaction> {
            return database.transaction {
                delegate.trackTransactionWithNoWarning(id)
            }
        }

        override fun track(): DataFeed<List<SignedTransaction>, SignedTransaction> {
            return database.transaction {
                delegate.track()
            }
        }

        override fun trackWithPagingSpec(paging: PageSpecification): DataFeed<TransactionStorage.Page, SignedTransaction> {
            return database.transaction {
                delegate.trackWithPagingSpec(paging)
            }
        }

        val records: MutableList<TxRecord> = Collections.synchronizedList(ArrayList<TxRecord>())
        override val updates: Observable<SignedTransaction>
            get() = delegate.updates

        override fun addTransaction(transaction: SignedTransaction): Boolean {
            database.transaction {
                records.add(TxRecord.Add(transaction))
                delegate.addTransaction(transaction)
            }
            return true
        }

        override fun addUnverifiedTransaction(transaction: SignedTransaction) {
            database.transaction {
                delegate.addUnverifiedTransaction(transaction)
            }
        }

        override fun getTransaction(id: SecureHash): SignedTransaction? {
            return database.transaction {
                records.add(TxRecord.Get(id))
                delegate.getTransaction(id)
            }
        }

        override fun getTransactionInternal(id: SecureHash): Pair<SignedTransaction, Boolean>? {
            return database.transaction {
                delegate.getTransactionInternal(id)
            }
        }
    }

    interface TxRecord {
        data class Add(val transaction: SignedTransaction) : TxRecord
        data class Get(val id: SecureHash) : TxRecord
    }
}
