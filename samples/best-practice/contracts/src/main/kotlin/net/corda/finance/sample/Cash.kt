package net.corda.finance.sample

import net.corda.core.contracts.*
import net.corda.core.crypto.toStringShort
import net.corda.core.identity.AbstractParty
import net.corda.core.internal.Emoji
import net.corda.core.schemas.MappedSchema
import net.corda.core.schemas.PersistentState
import net.corda.core.schemas.QueryableState
import net.corda.core.transactions.LedgerTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.finance.contracts.utils.sumCash
import net.corda.finance.schemas.CashSchemaV1
import java.util.*

/**
 * Copy of the finance Cash contract rewritten to use [CommandWithMetadata].
 */
class Cash : Contract {

    /**
     * Nothing to see here. Same as the original version.
     *
     * A state representing a cash claim against some party.
     */
    @BelongsToContract(Cash::class)
    data class State(
            override val amount: Amount<Issued<Currency>>,

            /** There must be a MoveCommand signed by this key to claim the amount. */
            override val owner: AbstractParty
    ) : FungibleAsset<Currency>, QueryableState {
        constructor(deposit: PartyAndReference, amount: Amount<Currency>, owner: AbstractParty)
                : this(Amount(amount.quantity, Issued(deposit, amount.token)), owner)

        override val exitKeys = setOf(owner.owningKey, amount.token.issuer.party.owningKey)
        override val participants = listOf(owner)

        override fun withNewOwnerAndAmount(newAmount: Amount<Issued<Currency>>, newOwner: AbstractParty): FungibleAsset<Currency> = copy(amount = amount.copy(newAmount.quantity), owner = newOwner)

        override fun toString() = "${Emoji.bagOfCash}Cash($amount at ${amount.token.issuer} owned by $owner)"

        override fun withNewOwner(newOwner: AbstractParty) = TODO()

        /** Object Relational Mapping support. */
        override fun generateMappedObject(schema: MappedSchema): PersistentState {
            return when (schema) {
                is CashSchemaV1 -> CashSchemaV1.PersistentCashState(
                        owner = this.owner,
                        pennies = this.amount.quantity,
                        currency = this.amount.token.product.currencyCode,
                        issuerPartyHash = this.amount.token.issuer.party.owningKey.toStringShort(),
                        issuerRef = this.amount.token.issuer.reference.bytes
                )
                /** Additional schema mappings would be added here (eg. CashSchemaV2, CashSchemaV3, ...) */
                else -> throw IllegalArgumentException("Unrecognised schema $schema")
            }
        }

        override fun supportedSchemas(): Iterable<MappedSchema> = listOf(CashSchemaV1)
    }

    interface Commands : CommandWithMetadata {
        /**
         * [paysFor] points to the index of the Command that this Move is meant to pay.
         * It is mandatory to set if the other Contract is structured similarly. See [CommercialPaper.RedeemTransition]
         */
        class Move(override val inputs: List<Int>, override val outputs: List<Int>, val paysFor: Int?) : Commands

        class Issue(override val outputs: List<Int>) : Commands {
            override val inputs: List<Int> get() = emptyList()
        }

        class Exit(override val inputs: List<Int>) : Commands {
            override val outputs: List<Int> get() = emptyList()
        }
    }

    data class MoveTransition(override val command: Command<Commands.Move>, val inputs: List<StateAndRef<State>>, val outputs: List<State>) : Transition<Commands.Move>() {
        constructor(cmd: Command<Commands.Move>, tx: LedgerTransaction) : this(cmd, cmd.value.inputs.map { tx.inputs[it] as StateAndRef<State> }, cmd.value.outputs.map { tx.outputStates[it] as State })

        override fun verify(tx: LedgerTransaction) {
            requireThat {
                "there must be input states" using (inputs.isNotEmpty())
                "there must be output states" using (outputs.isNotEmpty())
                "transition the same token" using ((inputs.map { it.state.data.amount.token } + outputs.map { it.amount.token }).toSet().size == 1)
                "there are no zero sized outputs" using (outputs.none { it.amount.quantity == 0L })
                "the owning key is a signer" using (command.signers.containsAll(inputs.map { it.state.data.owner.owningKey }))
                "input value must equal output value" using (inputs.map { it.state.data }.sumCash() == outputs.sumCash())
            }
        }

        val movedAmount get() = outputs.sumCash()
    }

    data class IssueTransition(override val command: Command<Commands.Issue>, val outputs: List<State>) : Transition<Commands.Issue>() {
        override fun verify(tx: LedgerTransaction) {
            requireThat {
                "at least one issued state" using (outputs.isNotEmpty())
                for (output in outputs) {
                    "output states are issued by a command signer" using (output.amount.token.issuer.party.owningKey in command.signers)
                }
                "there are no zero sized outputs" using (outputs.none { it.amount.quantity == 0L })
                "all outputs have the same deposit reference and currency" using (outputs.map { it.amount.token }.toSet().size == 1)
            }
        }

        fun issuedAmount() = outputs.sumCash()
    }

    data class ExitTransition(override val command: Command<Commands.Exit>, val inputs: List<StateAndRef<State>>) : Transition<Commands.Exit>() {
        override fun verify(tx: LedgerTransaction) {
            val exitKeys = inputs.flatMap { it.state.data.exitKeys }.toSet()
            requireThat {
                "there must be input states" using (inputs.isNotEmpty())
                "the exit keys are signers" using (command.signers.containsAll(exitKeys))
            }
        }

        fun exitedAmount() = inputs.map { it.state.data }.sumCash()
    }

    /**
     * Splits a transaction into individual transitions.
     * This reconstructs the transitions only from the Command meta-data.
     * If the command does not hold valid information for the transition, it will fail at this step.
     *
     * TODO - make this nicer.
     */
    private fun LedgerTransaction.extractTransitions(commands: List<Command<Commands>>) = commands.map { cmd ->
        val command = cmd.value
        when (command) {
            is Commands.Move -> MoveTransition(cmd as Command<Commands.Move>, this)
            is Commands.Issue -> IssueTransition(cmd as Command<Commands.Issue>, command.outputs.map { this.outputStates[it] as State })
            is Commands.Exit -> ExitTransition(cmd as Command<Commands.Exit>, command.inputs.map { this.inputs[it] as StateAndRef<State> })
            else -> throw java.lang.IllegalArgumentException("unknown command $cmd")
        }
    }

    override fun verify(tx: LedgerTransaction) {
        val cashCommands = tx.commandsOfType<Commands>()

        // Identify the transitions and verify them independently.
        val transitions = tx.extractTransitions(cashCommands)
        for (transition in transitions) {
            transition.verify(tx)
        }

        // Ensure there are no free floating Cash states and that the same state is not used in multiple transitions.
        tx.checkNoFreeFloatingStates<State, Commands>(cashCommands.map { it.value })

        // Additional cross transition overlap check.
        val payments = tx.commandsOfType<Commands.Move>().mapNotNull { it.value.paysFor }
        requireThat {
            "didn't pay for multiple items with the same money" using (payments.noDuplicates())
        }
    }
}

/**
 * Utilities to build unambiguous transactions.
 * They add metadata to the commands that will be used by the verification logic to build transitions.
 */
fun TransactionBuilder.issueCash(outputs: List<Cash.State>): Int = this.let {
    val idxs = addOutputStatesIdx(outputs)
    addCommand(Cash.Commands.Issue(idxs), outputs.map { it.owner.owningKey })
    commands().size - 1
}

/**
 * [paysFor] represents the index of the Command that this Cash move pays for
 */
fun TransactionBuilder.moveCash(inputs: List<StateAndRef<Cash.State>>, outputs: List<Cash.State>, paysFor: Int?): Int = this.let {
    val inIdxs = addInputStatesIdx(inputs)
    val outIdxs = addOutputStatesIdx(outputs)
    addCommand(Cash.Commands.Move(inIdxs, outIdxs, paysFor), outputs.map { it.owner.owningKey })
    commands().size - 1
}

fun TransactionBuilder.redeemCash(inputs: List<StateAndRef<Cash.State>>): Int = this.let {
    val idxs = addInputStatesIdx(inputs)
    addCommand(Cash.Commands.Exit(idxs), inputs.map { it.state.data.owner.owningKey })
    commands().size - 1
}

/**
 * USAGE
 */
fun exampleBuildCPRedeem(toRedeem: StateAndRef<CommercialPaper.State>, cashToPay: StateAndRef<Cash.State>, outputCash: Cash.State): TransactionBuilder = TransactionBuilder().apply {
    // First redeem the CP.
    val redeemCmdId = redeemCP(listOf(toRedeem))

    // The cash moves points to the CP redeem transition.
    moveCash(listOf(cashToPay), listOf(outputCash), paysFor = redeemCmdId)
}
