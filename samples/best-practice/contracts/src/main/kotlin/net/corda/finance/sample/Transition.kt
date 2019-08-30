package net.corda.finance.sample

import net.corda.core.contracts.*
import net.corda.core.internal.castIfPossible
import net.corda.core.transactions.LedgerTransaction
import net.corda.core.transactions.TransactionBuilder

/**
 * Will be used to verify transitions independently.
 */
interface CommandWithMetadata : CommandData {
    val inputs: List<Int>
    val outputs: List<Int>
}

/**
 * A Transition is an "instance" of a [CommandWithMetadata] in a [LedgerTransaction].
 */
abstract class Transition<C : CommandWithMetadata> {
    abstract val command: Command<C>

    /**
     * The equivalent of the [Contract.verify] method which applies to the related subset of states.
     */
    abstract fun verify(tx: LedgerTransaction)
}

/**
 * Standard check that should be performed by each contract using [CommandWithMetadata].
 */
inline fun <reified S : ContractState, C : CommandWithMetadata> LedgerTransaction.checkNoFreeFloatingStates(commands: List<C>) {
    val allInputs = this.inputIdxsOfType<S>()
    val allOutputs = this.outputIdxsOfType<S>()

    val allReferredInputs = commands.flatMap { it.inputs }
    val allReferredOutputs = commands.flatMap { it.outputs }

    requireThat {
        "input states not referenced by any command" using (allReferredInputs same allInputs)
        "output states not referenced by any command" using (allReferredOutputs same allOutputs)
        "input state referred by multiple transitions" using (allReferredInputs.noDuplicates())
        "output state referred by multiple transitions" using (allReferredOutputs.noDuplicates())
    }
}

// Utilities
fun <T : ContractState> LedgerTransaction.inputIdxsOfType(clazz: Class<T>): List<Int> = inputs.mapIndexedNotNull { idx, it ->
    clazz.castIfPossible(it.state.data)?.let { idx }
}

inline fun <reified T : ContractState> LedgerTransaction.inputIdxsOfType(): List<Int> = inputIdxsOfType(T::class.java)

fun <T : ContractState> LedgerTransaction.outputIdxsOfType(clazz: Class<T>): List<Int> = outputs.mapIndexedNotNull { idx, it ->
    clazz.castIfPossible(it.data)?.let { idx }
}

inline fun <reified T : ContractState> LedgerTransaction.outputIdxsOfType(): List<Int> = outputIdxsOfType(T::class.java)

fun Collection<*>.noDuplicates() = this.toSet().size == this.size

infix fun <T : Comparable<T>> Collection<T>.same(other: Collection<T>) = this.sorted() == other.sorted()

fun TransactionBuilder.addInputStateIdx(input: StateAndRef<*>): Int = this.addInputState(input).let { inputStates().size - 1 }
fun TransactionBuilder.addInputStatesIdx(inputs: List<StateAndRef<*>>): List<Int> = inputs.map { input -> addInputStateIdx(input) }

fun <C : ContractState> TransactionBuilder.addOutputStateIdx(output: C): Int = this.addOutputState(output).let { outputStates().size - 1 }
fun <C : ContractState> TransactionBuilder.addOutputStatesIdx(outputs: List<C>): List<Int> = outputs.map { output -> addOutputStateIdx(output) }