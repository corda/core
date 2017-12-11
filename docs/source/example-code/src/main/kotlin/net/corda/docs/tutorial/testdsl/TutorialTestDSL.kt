package net.corda.docs.tutorial.testdsl

import net.corda.core.utilities.days
import net.corda.finance.DOLLARS
import net.corda.finance.`issued by`
import net.corda.finance.contracts.CP_PROGRAM_ID
import net.corda.finance.contracts.CommercialPaper
import net.corda.finance.contracts.ICommercialPaperState
import net.corda.finance.contracts.asset.CASH
import net.corda.finance.contracts.asset.Cash
import net.corda.testing.*
import net.corda.testing.node.MockServices
import net.corda.testing.node.makeTestIdentityService
import org.junit.Rule
import org.junit.Test

class CommercialPaperTest {
    @Rule
    @JvmField
    val testSerialization = SerializationEnvironmentRule()
    private val ledgerServices = MockServices(makeTestIdentityService(listOf(MEGA_CORP_IDENTITY, MINI_CORP_IDENTITY, DUMMY_CASH_ISSUER_IDENTITY, DUMMY_NOTARY_IDENTITY)), MEGA_CORP.name)
    // DOCSTART 1
    fun getPaper(): ICommercialPaperState = CommercialPaper.State(
            issuance = MEGA_CORP.ref(123),
            owner = MEGA_CORP,
            faceValue = 1000.DOLLARS `issued by` MEGA_CORP.ref(123),
            maturityDate = TEST_TX_TIME + 7.days
    )
    // DOCEND 1

    // DOCSTART 2
    @Test
    fun simpleCP() {
        val inState = getPaper()
        ledgerServices.ledger(DUMMY_NOTARY) {
            transaction {
                attachments(CP_PROGRAM_ID)
                input(CP_PROGRAM_ID, inState)
                verifies()
            }
        }
    }
    // DOCEND 2

    // DOCSTART 3
    @Test
    fun simpleCPMove() {
        val inState = getPaper()
        ledgerServices.ledger(DUMMY_NOTARY) {
            transaction {
                input(CP_PROGRAM_ID, inState)
                command(MEGA_CORP_PUBKEY, CommercialPaper.Commands.Move())
                attachments(CP_PROGRAM_ID)
                verifies()
            }
        }
    }
    // DOCEND 3

    // DOCSTART 4
    @Test
    fun simpleCPMoveFails() {
        val inState = getPaper()
        ledgerServices.ledger(DUMMY_NOTARY) {
            transaction {
                input(CP_PROGRAM_ID, inState)
                command(MEGA_CORP_PUBKEY, CommercialPaper.Commands.Move())
                attachments(CP_PROGRAM_ID)
                `fails with`("the state is propagated")
            }
        }
    }
    // DOCEND 4

    // DOCSTART 5
    @Test
    fun simpleCPMoveSuccess() {
        val inState = getPaper()
        ledgerServices.ledger(DUMMY_NOTARY) {
            transaction {
                input(CP_PROGRAM_ID, inState)
                command(MEGA_CORP_PUBKEY, CommercialPaper.Commands.Move())
                attachments(CP_PROGRAM_ID)
                `fails with`("the state is propagated")
                output(CP_PROGRAM_ID, "alice's paper", inState.withOwner(ALICE))
                verifies()
            }
        }
    }
    // DOCEND 5

    // DOCSTART 6
    @Test
    fun `simple issuance with tweak`() {
        ledgerServices.ledger(DUMMY_NOTARY) {
            transaction {
                output(CP_PROGRAM_ID, "paper", getPaper()) // Some CP is issued onto the ledger by MegaCorp.
                attachments(CP_PROGRAM_ID)
                tweak {
                    // The wrong pubkey.
                    command(BIG_CORP_PUBKEY, CommercialPaper.Commands.Issue())
                    timeWindow(TEST_TX_TIME)
                    `fails with`("output states are issued by a command signer")
                }
                command(MEGA_CORP_PUBKEY, CommercialPaper.Commands.Issue())
                timeWindow(TEST_TX_TIME)
                verifies()
            }
        }
    }
    // DOCEND 6

    // DOCSTART 7
    @Test
    fun `simple issuance with tweak and top level transaction`() {
        ledgerServices.transaction(DUMMY_NOTARY) {
            output(CP_PROGRAM_ID, "paper", getPaper()) // Some CP is issued onto the ledger by MegaCorp.
            attachments(CP_PROGRAM_ID)
            tweak {
                // The wrong pubkey.
                command(BIG_CORP_PUBKEY, CommercialPaper.Commands.Issue())
                timeWindow(TEST_TX_TIME)
                `fails with`("output states are issued by a command signer")
            }
            command(MEGA_CORP_PUBKEY, CommercialPaper.Commands.Issue())
            timeWindow(TEST_TX_TIME)
            verifies()
        }
    }
    // DOCEND 7

    // DOCSTART 8
    @Test
    fun `chain commercial paper`() {
        val issuer = MEGA_CORP.ref(123)
        ledgerServices.ledger(DUMMY_NOTARY) {
            unverifiedTransaction {
                attachments(Cash.PROGRAM_ID)
                output(Cash.PROGRAM_ID, "alice's $900", 900.DOLLARS.CASH issuedBy issuer ownedBy ALICE)
            }

            // Some CP is issued onto the ledger by MegaCorp.
            transaction("Issuance") {
                output(CP_PROGRAM_ID, "paper", getPaper())
                command(MEGA_CORP_PUBKEY, CommercialPaper.Commands.Issue())
                attachments(CP_PROGRAM_ID)
                timeWindow(TEST_TX_TIME)
                verifies()
            }


            transaction("Trade") {
                input("paper")
                input("alice's $900")
                output(Cash.PROGRAM_ID, "borrowed $900", 900.DOLLARS.CASH issuedBy issuer ownedBy MEGA_CORP)
                output(CP_PROGRAM_ID, "alice's paper", "paper".output<ICommercialPaperState>().withOwner(ALICE))
                command(ALICE_PUBKEY, Cash.Commands.Move())
                command(MEGA_CORP_PUBKEY, CommercialPaper.Commands.Move())
                verifies()
            }
        }
    }
    // DOCEND 8

    // DOCSTART 9
    @Test
    fun `chain commercial paper double spend`() {
        val issuer = MEGA_CORP.ref(123)
        ledgerServices.ledger(DUMMY_NOTARY) {
            unverifiedTransaction {
                attachments(Cash.PROGRAM_ID)
                output(Cash.PROGRAM_ID, "alice's $900", 900.DOLLARS.CASH issuedBy issuer ownedBy ALICE)
            }

            // Some CP is issued onto the ledger by MegaCorp.
            transaction("Issuance") {
                output(CP_PROGRAM_ID, "paper", getPaper())
                command(MEGA_CORP_PUBKEY, CommercialPaper.Commands.Issue())
                attachments(CP_PROGRAM_ID)
                timeWindow(TEST_TX_TIME)
                verifies()
            }

            transaction("Trade") {
                input("paper")
                input("alice's $900")
                output(Cash.PROGRAM_ID, "borrowed $900", 900.DOLLARS.CASH issuedBy issuer ownedBy MEGA_CORP)
                output(CP_PROGRAM_ID, "alice's paper", "paper".output<ICommercialPaperState>().withOwner(ALICE))
                command(ALICE_PUBKEY, Cash.Commands.Move())
                command(MEGA_CORP_PUBKEY, CommercialPaper.Commands.Move())
                verifies()
            }

            transaction {
                input("paper")
                // We moved a paper to another pubkey.
                output(CP_PROGRAM_ID, "bob's paper", "paper".output<ICommercialPaperState>().withOwner(BOB))
                command(MEGA_CORP_PUBKEY, CommercialPaper.Commands.Move())
                verifies()
            }

            fails()
        }
    }
    // DOCEND 9

    // DOCSTART 10
    @Test
    fun `chain commercial tweak`() {
        val issuer = MEGA_CORP.ref(123)
        ledgerServices.ledger(DUMMY_NOTARY) {
            unverifiedTransaction {
                attachments(Cash.PROGRAM_ID)
                output(Cash.PROGRAM_ID, "alice's $900", 900.DOLLARS.CASH issuedBy issuer ownedBy ALICE)
            }

            // Some CP is issued onto the ledger by MegaCorp.
            transaction("Issuance") {
                output(CP_PROGRAM_ID, "paper", getPaper())
                command(MEGA_CORP_PUBKEY, CommercialPaper.Commands.Issue())
                attachments(CP_PROGRAM_ID)
                timeWindow(TEST_TX_TIME)
                verifies()
            }

            transaction("Trade") {
                input("paper")
                input("alice's $900")
                output(Cash.PROGRAM_ID, "borrowed $900", 900.DOLLARS.CASH issuedBy issuer ownedBy MEGA_CORP)
                output(CP_PROGRAM_ID, "alice's paper", "paper".output<ICommercialPaperState>().withOwner(ALICE))
                command(ALICE_PUBKEY, Cash.Commands.Move())
                command(MEGA_CORP_PUBKEY, CommercialPaper.Commands.Move())
                verifies()
            }

            tweak {
                transaction {
                    input("paper")
                    // We moved a paper to another pubkey.
                    output(CP_PROGRAM_ID, "bob's paper", "paper".output<ICommercialPaperState>().withOwner(BOB))
                    command(MEGA_CORP_PUBKEY, CommercialPaper.Commands.Move())
                    verifies()
                }
                fails()
            }

            verifies()
        }
    }
    // DOCEND 10
}
