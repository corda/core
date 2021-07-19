package net.corda.core.transactions

import net.corda.core.CordaException
import net.corda.core.KeepForDJVM
import net.corda.core.contracts.*
import net.corda.core.contracts.ComponentGroupEnum.*
import net.corda.core.crypto.*
import net.corda.core.identity.Party
import net.corda.core.internal.deserialiseCommands
import net.corda.core.internal.deserialiseComponentGroup
import net.corda.core.serialization.CordaSerializable
import net.corda.core.serialization.SerializedBytes
import net.corda.core.serialization.deserialize
import net.corda.core.utilities.OpaqueBytes
import java.security.PublicKey
import java.util.function.Predicate

/**
 * Implemented by [WireTransaction] and [FilteredTransaction]. A TraversableTransaction allows you to iterate
 * over the flattened components of the underlying transaction structure, taking into account that some
 * may be missing in the case of this representing a "torn" transaction. Please see the user guide section
 * "Transaction tear-offs" to learn more about this feature.
 */
abstract class TraversableTransaction(open val componentGroups: List<ComponentGroup>) : CoreTransaction() {
    /** Hashes of the ZIP/JAR files that are needed to interpret the contents of this wire transaction. */
    val attachments: List<SecureHash> = deserialiseComponentGroup(componentGroups, SecureHash::class, ATTACHMENTS_GROUP)

    /** Pointers to the input states on the ledger, identified by (tx identity hash, output index). */
    override val inputs: List<StateRef> = deserialiseComponentGroup(componentGroups, StateRef::class, INPUTS_GROUP)

    /** Pointers to reference states, identified by (tx identity hash, output index). */
    override val references: List<StateRef> = deserialiseComponentGroup(componentGroups, StateRef::class, REFERENCES_GROUP)

    override val outputs: List<TransactionState<ContractState>> = deserialiseComponentGroup(componentGroups, TransactionState::class, OUTPUTS_GROUP)

    /** Ordered list of ([CommandData], [PublicKey]) pairs that instruct the contracts what to do. */
    val commands: List<Command<*>> = deserialiseCommands(componentGroups)

    override val notary: Party? = let {
        val notaries: List<Party> = deserialiseComponentGroup(componentGroups, Party::class, NOTARY_GROUP)
        check(notaries.size <= 1) { "Invalid Transaction. More than 1 notary party detected." }
        notaries.firstOrNull()
    }

    val timeWindow: TimeWindow? = let {
        val timeWindows: List<TimeWindow> = deserialiseComponentGroup(componentGroups, TimeWindow::class, TIMEWINDOW_GROUP)
        check(timeWindows.size <= 1) { "Invalid Transaction. More than 1 time-window detected." }
        timeWindows.firstOrNull()
    }

    override val networkParametersHash: SecureHash? = let {
        val parametersHashes = deserialiseComponentGroup(componentGroups, SecureHash::class, PARAMETERS_GROUP)
        check(parametersHashes.size <= 1) { "Invalid Transaction. More than 1 network parameters hash detected." }
        parametersHashes.firstOrNull()
    }

    /**
     * Returns a list of all the component groups that are present in the transaction, excluding the privacySalt,
     * in the following order (which is the same with the order in [ComponentGroupEnum]:
     * - list of each input that is present
     * - list of each output that is present
     * - list of each command that is present
     * - list of each attachment that is present
     * - The notary [Party], if present (list with one element)
     * - The time-window of the transaction, if present (list with one element)
     * - list of each reference input that is present
     * - network parameters hash if present
     */
    val availableComponentGroups: List<List<Any>>
        get() {
            val result = mutableListOf(inputs, outputs, commands, attachments, references)
            notary?.let { result += listOf(it) }
            timeWindow?.let { result += listOf(it) }
            networkParametersHash?.let { result += listOf(it) }
            return result
        }
}

/**
 * Class representing merkleized filtered transaction.
 * @param id Merkle tree root hash.
 * @param filteredComponentGroups list of transaction components groups remained after filters are applied to [WireTransaction].
 * @param groupHashes the roots of the transaction component groups.
 */
@KeepForDJVM
@CordaSerializable
class FilteredTransaction internal constructor(
        override val id: SecureHash,
        val filteredComponentGroups: List<FilteredComponentGroup>,
        val groupHashes: List<SecureHash>
) : TraversableTransaction(filteredComponentGroups) {

    companion object {
        /**
         * Construction of filtered transaction with partial Merkle tree.
         * @param wtx WireTransaction to be filtered.
         * @param filtering filtering over the whole WireTransaction.
         */
        @JvmStatic
        fun buildFilteredTransaction(wtx: WireTransaction, filtering: Predicate<Any>): FilteredTransaction {
            val filteredComponentGroups = filterWithFun(wtx, filtering)
            return FilteredTransaction(wtx.id, filteredComponentGroups, wtx.groupHashes)
        }

        /**
         * Construction of partial transaction from [WireTransaction] based on filtering.
         * Note that list of nonces to be sent is updated on the fly, based on the index of the filtered tx component.
         * @param filtering filtering over the whole WireTransaction.
         * @return a list of [FilteredComponentGroup] used in PartialMerkleTree calculation and verification.
         */
        private fun filterWithFun(wtx: WireTransaction, filtering: Predicate<Any>): List<FilteredComponentGroup> {
            val filteredSerialisedComponents: MutableMap<Int, MutableList<OpaqueBytes>> = hashMapOf()
            val filteredComponentNonces: MutableMap<Int, MutableList<SecureHash>> = hashMapOf()
            val filteredComponentHashes: MutableMap<Int, MutableList<SecureHash>> = hashMapOf() // Required for partial Merkle tree generation.
            var signersIncluded = false

            fun <T : Any> filter(t: T, componentGroupIndex: Int, internalIndex: Int) {
                if (!filtering.test(t)) return

                val group = filteredSerialisedComponents[componentGroupIndex]
                // Because the filter passed, we know there is a match. We also use first Vs single as the init function
                // of WireTransaction ensures there are no duplicated groups.
                val serialisedComponent = wtx.componentGroups.first { it.groupIndex == componentGroupIndex }.components[internalIndex]
                if (group == null) {
                    // As all of the helper Map structures, like availableComponentNonces, availableComponentHashes
                    // and groupsMerkleRoots, are computed lazily via componentGroups.forEach, there should always be
                    // a match on Map.get ensuring it will never return null.
                    filteredSerialisedComponents[componentGroupIndex] = mutableListOf(serialisedComponent)
                    filteredComponentNonces[componentGroupIndex] = mutableListOf(wtx.availableComponentNonces[componentGroupIndex]!![internalIndex])
                    filteredComponentHashes[componentGroupIndex] = mutableListOf(wtx.availableComponentHashes[componentGroupIndex]!![internalIndex])
                } else {
                    group.add(serialisedComponent)
                    // If the group[componentGroupIndex] existed, then we guarantee that
                    // filteredComponentNonces[componentGroupIndex] and filteredComponentHashes[componentGroupIndex] are not null.
                    filteredComponentNonces[componentGroupIndex]!!.add(wtx.availableComponentNonces[componentGroupIndex]!![internalIndex])
                    filteredComponentHashes[componentGroupIndex]!!.add(wtx.availableComponentHashes[componentGroupIndex]!![internalIndex])
                }
                // If at least one command is visible, then all command-signers should be visible as well.
                // This is required for visibility purposes, see FilteredTransaction.checkAllCommandsVisible() for more details.
                if (componentGroupIndex == COMMANDS_GROUP.ordinal && !signersIncluded) {
                    signersIncluded = true
                    val signersGroupIndex = SIGNERS_GROUP.ordinal
                    // There exist commands, thus the signers group is not empty.
                    val signersGroupComponents = wtx.componentGroups.first { it.groupIndex == signersGroupIndex }
                    filteredSerialisedComponents[signersGroupIndex] = signersGroupComponents.components.toMutableList()
                    filteredComponentNonces[signersGroupIndex] = wtx.availableComponentNonces[signersGroupIndex]!!.toMutableList()
                    filteredComponentHashes[signersGroupIndex] = wtx.availableComponentHashes[signersGroupIndex]!!.toMutableList()
                }
            }

            fun updateFilteredComponents() {
                wtx.inputs.forEachIndexed { internalIndex, it -> filter(it, INPUTS_GROUP.ordinal, internalIndex) }
                wtx.outputs.forEachIndexed { internalIndex, it -> filter(it, OUTPUTS_GROUP.ordinal, internalIndex) }
                wtx.commands.forEachIndexed { internalIndex, it -> filter(it, COMMANDS_GROUP.ordinal, internalIndex) }
                wtx.attachments.forEachIndexed { internalIndex, it -> filter(it, ATTACHMENTS_GROUP.ordinal, internalIndex) }
                if (wtx.notary != null) filter(wtx.notary, NOTARY_GROUP.ordinal, 0)
                if (wtx.timeWindow != null) filter(wtx.timeWindow, TIMEWINDOW_GROUP.ordinal, 0)
                // Note that because [inputs] and [references] share the same type [StateRef], we use a wrapper for references [ReferenceStateRef],
                // when filtering. Thus, to filter-in all [references] based on type, one should use the wrapper type [ReferenceStateRef] and not [StateRef].
                // Similar situation is for network parameters hash and attachments, one should use wrapper [NetworkParametersHash] and not [SecureHash].
                wtx.references.forEachIndexed { internalIndex, it -> filter(ReferenceStateRef(it), REFERENCES_GROUP.ordinal, internalIndex) }
                wtx.networkParametersHash?.let { filter(NetworkParametersHash(it), PARAMETERS_GROUP.ordinal, 0) }
                // It is highlighted that because there is no a signers property in TraversableTransaction,
                // one cannot specifically filter them in or out.
                // The above is very important to ensure someone won't filter out the signers component group if at least one
                // command is included in a FilteredTransaction.

                // It's sometimes possible that when we receive a WireTransaction for which there is a new or more unknown component groups,
                // we decide to filter and attach this field to a FilteredTransaction.
                // An example would be to redact certain contract state types, but otherwise leave a transaction alone,
                // including the unknown new components.
                wtx.componentGroups
                        .filter { it.groupIndex >= values().size }
                        .forEach { componentGroup -> componentGroup.components.forEachIndexed { internalIndex, component -> filter(component, componentGroup.groupIndex, internalIndex) } }
            }

            fun createPartialMerkleTree(componentGroupIndex: Int): PartialMerkleTree {
                return PartialMerkleTree.build(
                        MerkleTree.getMerkleTree(wtx.availableComponentHashes[componentGroupIndex]!!),
                        filteredComponentHashes[componentGroupIndex]!!
                )
            }

            fun createFilteredComponentGroups(): List<FilteredComponentGroup> {
                updateFilteredComponents()
                val filteredComponentGroups: MutableList<FilteredComponentGroup> = mutableListOf()
                filteredSerialisedComponents.forEach { (groupIndex, value) ->
                    filteredComponentGroups.add(FilteredComponentGroup(groupIndex, value, filteredComponentNonces[groupIndex]!!, createPartialMerkleTree(groupIndex)))
                }
                return filteredComponentGroups
            }

            return createFilteredComponentGroups()
        }
    }

    /**
     * Runs verification of partial Merkle branch against [id].
     * Note that empty filtered transactions (with no component groups) are accepted as well,
     * e.g. for Timestamp Authorities to blindly sign or any other similar case in the future
     * that requires a blind signature over a transaction's [id].
     * @throws FilteredTransactionVerificationException if verification fails.
     */
    @Throws(FilteredTransactionVerificationException::class)
    fun verify() {
        verificationCheck(groupHashes.isNotEmpty()) { "At least one component group hash is required" }
        // Verify the top level Merkle tree (group hashes are its leaves, including allOnesHash for empty list or null
        // components in WireTransaction).
        verificationCheck(MerkleTree.getMerkleTree(groupHashes).hash == id) {
            "Top level Merkle tree cannot be verified against transaction's id"
        }

        // For completely blind verification (no components are included).
        if (filteredComponentGroups.isEmpty()) return

        // Compute partial Merkle roots for each filtered component and verify each of the partial Merkle trees.
        filteredComponentGroups.forEach { (groupIndex, components, nonces, groupPartialTree) ->
            verificationCheck(groupIndex < groupHashes.size) { "There is no matching component group hash for group $groupIndex" }
            val groupMerkleRoot = groupHashes[groupIndex]
            verificationCheck(groupMerkleRoot == PartialMerkleTree.rootAndUsedHashes(groupPartialTree.root, mutableListOf())) {
                "Partial Merkle tree root and advertised full Merkle tree root for component group $groupIndex do not match"
            }
            verificationCheck(groupPartialTree.verify(groupMerkleRoot, components.mapIndexed { index, component -> componentHash(nonces[index], component) })) {
                "Visible components in group $groupIndex cannot be verified against their partial Merkle tree"
            }
        }
    }

    /**
     * Function that checks the whole filtered structure.
     * Force type checking on a structure that we obtained, so we don't sign more than expected.
     * Example: Oracle is implemented to check only for commands, if it gets an attachment and doesn't expect it - it can sign
     * over a transaction with the attachment that wasn't verified. Of course it depends on how you implement it, but else -> false
     * should solve a problem with possible later extensions to WireTransaction.
     * @param checkingFun function that performs type checking on the structure fields and provides verification logic accordingly.
     * @return false if no elements were matched on a structure or checkingFun returned false.
     */
    fun checkWithFun(checkingFun: (Any) -> Boolean): Boolean {
        val checkList = availableComponentGroups.flatten().map { checkingFun(it) }
        return (!checkList.isEmpty()) && checkList.all { it }
    }

    /**
     * Function that checks if all of the components in a particular group are visible.
     * This functionality is required on non-Validating Notaries to check that all inputs are visible.
     * It might also be applied in Oracles or any other entity requiring [Command] visibility, but because this method
     * cannot distinguish between related and unrelated to the signer [Command]s, one should use the
     * [checkCommandVisibility] method, which is specifically designed for [Command] visibility purposes.
     * The logic behind this algorithm is that we check that the root of the provided group partialMerkleTree matches with the
     * root of a fullMerkleTree if computed using all visible components.
     * Note that this method is usually called after or before [verify], to also ensure that the provided partial Merkle
     * tree corresponds to the correct leaf in the top Merkle tree.
     * @param componentGroupEnum the [ComponentGroupEnum] that corresponds to the componentGroup for which we require full component visibility.
     * @throws ComponentVisibilityException if not all of the components are visible or if the component group is not present in the [FilteredTransaction].
     */
    @Throws(ComponentVisibilityException::class)
    fun checkAllComponentsVisible(componentGroupEnum: ComponentGroupEnum) {
        val group = filteredComponentGroups.firstOrNull { it.groupIndex == componentGroupEnum.ordinal }
        if (group == null) {
            // If we don't receive elements of a particular component, check if its ordinal is bigger that the
            // groupHashes.size or if the group hash is allOnesHash,
            // to ensure there were indeed no elements in the original wire transaction.
            visibilityCheck(componentGroupEnum.ordinal >= groupHashes.size || groupHashes[componentGroupEnum.ordinal] == SecureHash.allOnesHashFor(id.algorithm)) {
                "Did not receive components for group ${componentGroupEnum.ordinal} and cannot verify they didn't exist in the original wire transaction"
            }
        } else {
            visibilityCheck(group.groupIndex < groupHashes.size) { "There is no matching component group hash for group ${group.groupIndex}" }
            val groupPartialRoot = groupHashes[group.groupIndex]
            val groupFullRoot = MerkleTree.getMerkleTree(group.components.mapIndexed { index, component -> componentHash(group.nonces[index], component) }).hash
            visibilityCheck(groupPartialRoot == groupFullRoot) { "Some components for group ${group.groupIndex} are not visible" }
            // Verify the top level Merkle tree from groupHashes.
            visibilityCheck(MerkleTree.getMerkleTree(groupHashes).hash == id) {
                "Transaction is malformed. Top level Merkle tree cannot be verified against transaction's id"
            }
        }
    }

    /**
     * Function that checks if all of the commands that should be signed by the input public key are visible.
     * This functionality is required from Oracles to check that all of the commands they should sign are visible.
     * This algorithm uses the [ComponentGroupEnum.SIGNERS_GROUP] to count how many commands should be signed by the
     * input [PublicKey] and it then matches it with the size of received [commands].
     * Note that this method does not throw if there are no commands for this key to sign in the original [WireTransaction].
     * @param publicKey signer's [PublicKey]
     * @throws ComponentVisibilityException if not all of the related commands are visible.
     */
    @Throws(ComponentVisibilityException::class)
    fun checkCommandVisibility(publicKey: PublicKey) {
        val commandSigners = componentGroups.firstOrNull { it.groupIndex == SIGNERS_GROUP.ordinal }
        val expectedNumOfCommands = expectedNumOfCommands(publicKey, commandSigners)
        val receivedForThisKeyNumOfCommands = commands.filter { publicKey in it.signers }.size
        visibilityCheck(expectedNumOfCommands == receivedForThisKeyNumOfCommands) {
            "$expectedNumOfCommands commands were expected, but received $receivedForThisKeyNumOfCommands"
        }
    }

    // Function to return number of expected commands to sign.
    private fun expectedNumOfCommands(publicKey: PublicKey, commandSigners: ComponentGroup?): Int {
        checkAllComponentsVisible(SIGNERS_GROUP)
        if (commandSigners == null) return 0
        fun signersKeys(internalIndex: Int, opaqueBytes: OpaqueBytes): List<PublicKey> {
            try {
                return SerializedBytes<List<PublicKey>>(opaqueBytes.bytes).deserialize()
            } catch (e: Exception) {
                throw Exception("Malformed transaction, signers at index $internalIndex cannot be deserialised", e)
            }
        }

        return commandSigners.components
                .mapIndexed { internalIndex, opaqueBytes -> signersKeys(internalIndex, opaqueBytes) }
                .filter { signers -> publicKey in signers }.size
    }

    private inline fun verificationCheck(value: Boolean, lazyMessage: () -> Any) {
        if (!value) {
            val message = lazyMessage()
            throw FilteredTransactionVerificationException(id, message.toString())
        }
    }

    private inline fun visibilityCheck(value: Boolean, lazyMessage: () -> Any) {
        if (!value) {
            val message = lazyMessage()
            throw ComponentVisibilityException(id, message.toString())
        }
    }
}

/**
 * A FilteredComponentGroup is used to store the filtered list of transaction components of the same type in serialised form.
 * This is similar to [ComponentGroup], but it also includes the corresponding nonce per component.
 */
@KeepForDJVM
@CordaSerializable
data class FilteredComponentGroup(override val groupIndex: Int,
                                  override val components: List<OpaqueBytes>,
                                  val nonces: List<SecureHash>,
                                  val partialMerkleTree: PartialMerkleTree) : ComponentGroup(groupIndex, components) {
    init {
        check(components.size == nonces.size) { "Size of transaction components and nonces do not match" }
    }
}

/** Thrown when checking for visibility of all-components in a group in [FilteredTransaction.checkAllComponentsVisible].
 * @param id transaction's id.
 * @param reason information about the exception.
 */
@KeepForDJVM
@CordaSerializable
class ComponentVisibilityException(val id: SecureHash, val reason: String) : CordaException("Component visibility error for transaction with id:$id. Reason: $reason")

/** Thrown when [FilteredTransaction.verify] fails.
 * @param id transaction's id.
 * @param reason information about the exception.
 */
@KeepForDJVM
@CordaSerializable
class FilteredTransactionVerificationException(val id: SecureHash, val reason: String) : CordaException("Transaction with id:$id cannot be verified. Reason: $reason")

/** Wrapper over [StateRef] to be used when filtering reference states. */
@KeepForDJVM
@CordaSerializable
data class ReferenceStateRef(val stateRef: StateRef)

/** Wrapper over [SecureHash] to be used when filtering network parameters hash. */
@KeepForDJVM
@CordaSerializable
data class NetworkParametersHash(val hash: SecureHash)