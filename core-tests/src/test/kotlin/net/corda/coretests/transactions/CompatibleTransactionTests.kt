package net.corda.coretests.transactions

import net.corda.core.contracts.*
import net.corda.core.contracts.ComponentGroupEnum.*
import net.corda.core.crypto.*
import net.corda.core.internal.accessAvailableComponentHashes
import net.corda.core.internal.accessGroupHashes
import net.corda.core.internal.accessGroupMerkleRoots
import net.corda.core.internal.createComponentGroups
import net.corda.core.serialization.serialize
import net.corda.core.transactions.*
import net.corda.core.utilities.OpaqueBytes
import net.corda.testing.contracts.DummyContract
import net.corda.testing.contracts.DummyState
import net.corda.testing.core.*
import org.junit.Rule
import org.junit.Test
import java.time.Instant
import java.util.function.Predicate
import kotlin.test.*

class CompatibleTransactionTests {
    private companion object {
        val DUMMY_KEY_1 = generateKeyPair()
        val DUMMY_KEY_2 = generateKeyPair()
        val BOB = TestIdentity(BOB_NAME, 80).party
        val DUMMY_NOTARY = TestIdentity(DUMMY_NOTARY_NAME, 20).party
    }

    @Rule
    @JvmField
    val testSerialization = SerializationEnvironmentRule()
    private val dummyOutState = TransactionState(DummyState(0), DummyContract.PROGRAM_ID, DUMMY_NOTARY)
    private val stateRef1 = StateRef(SecureHash.randomSHA256(), 0)
    private val stateRef2 = StateRef(SecureHash.randomSHA256(), 1)
    private val stateRef3 = StateRef(SecureHash.randomSHA256(), 0)

    private val inputs = listOf(stateRef1, stateRef2, stateRef3) // 3 elements.
    private val outputs = listOf(dummyOutState, dummyOutState.copy(notary = BOB)) // 2 elements.
    private val commands = listOf(dummyCommand(DUMMY_KEY_1.public, DUMMY_KEY_2.public)) // 1 element.
    private val attachments = emptyList<SecureHash>() // Empty list.
    private val notary = DUMMY_NOTARY
    private val timeWindow = TimeWindow.fromOnly(Instant.now())
    private val privacySalt: PrivacySalt = PrivacySalt()
    private val paramsHash = SecureHash.randomSHA256()

    private val inputGroup by lazy { ComponentGroup(INPUTS_GROUP.ordinal, inputs.map { it.serialize() }) }
    private val outputGroup by lazy { ComponentGroup(OUTPUTS_GROUP.ordinal, outputs.map { it.serialize() }) }
    private val commandGroup by lazy { ComponentGroup(COMMANDS_GROUP.ordinal, commands.map { it.value.serialize() }) }
    private val attachmentGroup by lazy { ComponentGroup(ATTACHMENTS_GROUP.ordinal, attachments.map { it.serialize() }) } // The list is empty.
    private val notaryGroup by lazy { ComponentGroup(NOTARY_GROUP.ordinal, listOf(notary.serialize())) }
    private val timeWindowGroup by lazy { ComponentGroup(TIMEWINDOW_GROUP.ordinal, listOf(timeWindow.serialize())) }
    private val signersGroup by lazy { ComponentGroup(SIGNERS_GROUP.ordinal, commands.map { it.signers.serialize() }) }
    private val networkParamsGroup by lazy { ComponentGroup(PARAMETERS_GROUP.ordinal, listOf(paramsHash.serialize())) }

    private val newUnknownComponentGroup = ComponentGroup(100, listOf(OpaqueBytes(secureRandomBytes(4)), OpaqueBytes(secureRandomBytes(8))))
    private val newUnknownComponentEmptyGroup = ComponentGroup(101, emptyList())

    // Do not add attachments (empty list).
    private val componentGroupsA by lazy {
        listOf(
                inputGroup,
                outputGroup,
                commandGroup,
                notaryGroup,
                timeWindowGroup,
                signersGroup
        )
    }
    private val wireTransactionA by lazy { WireTransaction(componentGroups = componentGroupsA, privacySalt = privacySalt, txVersion = 0) }

    @Test
    fun `Merkle root computations`() {
        // Merkle tree computation is deterministic if the same salt and ordering are used.
        val wireTransactionB = WireTransaction(componentGroups = componentGroupsA, privacySalt = privacySalt, txVersion = 0)
        assertEquals(wireTransactionA, wireTransactionB)

        // Merkle tree computation will change if privacy salt changes.
        val wireTransactionOtherPrivacySalt = WireTransaction(componentGroups = componentGroupsA, privacySalt = PrivacySalt(), txVersion = 0)
        assertNotEquals(wireTransactionA, wireTransactionOtherPrivacySalt)

        // Full Merkle root is computed from the list of Merkle roots of each component group.
        assertEquals(wireTransactionA.merkleTree.hash, MerkleTree.getMerkleTree(wireTransactionA.accessGroupHashes()).hash)

        // Trying to add an empty component group (not allowed), e.g. the empty attachmentGroup.
        val componentGroupsEmptyAttachment = listOf(
                inputGroup,
                outputGroup,
                commandGroup,
                attachmentGroup,
                notaryGroup,
                timeWindowGroup,
                signersGroup
        )
        assertFails { WireTransaction(componentGroups = componentGroupsEmptyAttachment, privacySalt = privacySalt, txVersion = 0) }

        // Ordering inside a component group matters.
        val inputsShuffled = listOf(stateRef2, stateRef1, stateRef3)
        val inputShuffledGroup = ComponentGroup(INPUTS_GROUP.ordinal, inputsShuffled.map { it -> it.serialize() })
        val componentGroupsB = listOf(
                inputShuffledGroup,
                outputGroup,
                commandGroup,
                notaryGroup,
                timeWindowGroup,
                signersGroup
        )
        val wireTransaction1ShuffledInputs = WireTransaction(componentGroups = componentGroupsB, privacySalt = privacySalt, txVersion = 0)
        // The ID has changed due to change of the internal ordering in inputs.
        assertNotEquals(wireTransaction1ShuffledInputs, wireTransactionA)

        // Inputs group Merkle roots are not equal.
        assertNotEquals(wireTransactionA.accessGroupMerkleRoots()[INPUTS_GROUP.ordinal], wireTransaction1ShuffledInputs.accessGroupMerkleRoots()[INPUTS_GROUP.ordinal])
        // But outputs group Merkle leaf (and the rest) remained the same.
        assertEquals(wireTransactionA.accessGroupMerkleRoots()[OUTPUTS_GROUP.ordinal], wireTransaction1ShuffledInputs.accessGroupMerkleRoots()[OUTPUTS_GROUP.ordinal])
        assertEquals(wireTransactionA.accessGroupMerkleRoots()[NOTARY_GROUP.ordinal], wireTransaction1ShuffledInputs.accessGroupMerkleRoots()[NOTARY_GROUP.ordinal])
        assertNull(wireTransactionA.accessGroupMerkleRoots()[ATTACHMENTS_GROUP.ordinal])
        assertNull(wireTransaction1ShuffledInputs.accessGroupMerkleRoots()[ATTACHMENTS_GROUP.ordinal])

        // Group leaves (components) ordering does not affect the id. In this case, we added outputs group before inputs.
        val shuffledComponentGroupsA = listOf(
                outputGroup,
                inputGroup,
                commandGroup,
                notaryGroup,
                timeWindowGroup,
                signersGroup
        )
        assertEquals(wireTransactionA, WireTransaction(componentGroups = shuffledComponentGroupsA, privacySalt = privacySalt, txVersion = 0))
    }

    @Test
    fun `WireTransaction constructors and compatibility`() {
        val groups = createComponentGroups(inputs, outputs, commands, attachments, notary, timeWindow, emptyList(), null)
        val wireTransactionOldConstructor = WireTransaction(groups, privacySalt, 0)
        assertEquals(wireTransactionA, wireTransactionOldConstructor)

        // Malformed tx - attachments is not List<SecureHash>. For this example, we mistakenly added input-state (StateRef) serialised objects with ATTACHMENTS_GROUP.ordinal.
        val componentGroupsB = listOf(
                inputGroup,
                outputGroup,
                commandGroup,
                ComponentGroup(ATTACHMENTS_GROUP.ordinal, inputGroup.components),
                notaryGroup,
                timeWindowGroup,
                signersGroup
        )
        assertFails { WireTransaction(componentGroupsB, privacySalt, 0).attachments.toList() }

        // Malformed tx - duplicated component group detected.
        val componentGroupsDuplicatedCommands = listOf(
                inputGroup,
                outputGroup,
                commandGroup, // First commandsGroup.
                commandGroup, // Second commandsGroup.
                notaryGroup,
                timeWindowGroup,
                signersGroup
        )
        assertFails { WireTransaction(componentGroupsDuplicatedCommands, privacySalt, 0) }

        // Malformed tx - inputs is not a serialised object at all.
        val componentGroupsC = listOf(
                ComponentGroup(INPUTS_GROUP.ordinal, listOf(OpaqueBytes(ByteArray(8)))),
                outputGroup,
                commandGroup,
                notaryGroup,
                timeWindowGroup,
                signersGroup
        )
        assertFails { WireTransaction(componentGroupsC, privacySalt, 0) }

        val componentGroupsCompatibleA = listOf(
                inputGroup,
                outputGroup,
                commandGroup,
                notaryGroup,
                timeWindowGroup,
                signersGroup,
                newUnknownComponentGroup // A new unknown component with ordinal 100 that we cannot process.
        )

        // The old client (receiving more component types than expected) is still compatible.
        val wireTransactionCompatibleA = WireTransaction(componentGroupsCompatibleA, privacySalt, 0)
        assertEquals(wireTransactionCompatibleA.availableComponentGroups, wireTransactionA.availableComponentGroups) // The known components are the same.
        assertNotEquals(wireTransactionCompatibleA, wireTransactionA) // But obviously, its Merkle root has changed Vs wireTransactionA (which doesn't include this extra component).

        // The old client will throw if receiving an empty component (even if this is unknown).
        val componentGroupsCompatibleEmptyNew = listOf(
                inputGroup,
                outputGroup,
                commandGroup,
                notaryGroup,
                timeWindowGroup,
                signersGroup,
                newUnknownComponentEmptyGroup // A new unknown component with ordinal 101 that we cannot process.
        )
        assertFails { WireTransaction(componentGroupsCompatibleEmptyNew, privacySalt, 0) }
    }

    @Test
    fun `FilteredTransaction constructors and compatibility`() {
        // Filter out all of the components.
        val ftxNothing = wireTransactionA.buildFilteredTransaction(Predicate { false }) // Nothing filtered.
        // Although nothing filtered, we still receive the group hashes for the top level Merkle tree.
        // Note that attachments are not sent, but group hashes include the allOnesHash flag for the attachment group hash; that's why we expect +1 group hashes.
        assertEquals(wireTransactionA.componentGroups.size + 1, ftxNothing.groupHashes.size)
        ftxNothing.verify()

        // Include all of the components.
        val ftxAll = wireTransactionA.buildFilteredTransaction(Predicate { true }) // All filtered.
        ftxAll.verify()
        ftxAll.checkAllComponentsVisible(INPUTS_GROUP)
        ftxAll.checkAllComponentsVisible(OUTPUTS_GROUP)
        ftxAll.checkAllComponentsVisible(COMMANDS_GROUP)
        ftxAll.checkAllComponentsVisible(ATTACHMENTS_GROUP)
        ftxAll.checkAllComponentsVisible(NOTARY_GROUP)
        ftxAll.checkAllComponentsVisible(TIMEWINDOW_GROUP)
        ftxAll.checkAllComponentsVisible(SIGNERS_GROUP)
        ftxAll.checkAllComponentsVisible(PARAMETERS_GROUP)

        // Filter inputs only.
        fun filtering(elem: Any): Boolean {
            return when (elem) {
                is StateRef -> true
                else -> false
            }
        }

        val ftxInputs = wireTransactionA.buildFilteredTransaction(Predicate(::filtering)) // Inputs only filtered.
        ftxInputs.verify()
        ftxInputs.checkAllComponentsVisible(INPUTS_GROUP)

        assertEquals(1, ftxInputs.filteredComponentGroups.size) // We only add component groups that are not empty, thus in this case: the inputs only.
        assertEquals(3, ftxInputs.filteredComponentGroups.firstOrNull { it.groupIndex == INPUTS_GROUP.ordinal }!!.components.size) // All 3 inputs are present.
        assertEquals(3, ftxInputs.filteredComponentGroups.firstOrNull { it.groupIndex == INPUTS_GROUP.ordinal }!!.nonces.size) // And their corresponding nonces.
        assertNotNull(ftxInputs.filteredComponentGroups.firstOrNull { it.groupIndex == INPUTS_GROUP.ordinal }!!.partialMerkleTree) // And the Merkle tree.

        // Filter one input only.
        fun filteringOneInput(elem: Any) = elem == inputs[0]

        val ftxOneInput = wireTransactionA.buildFilteredTransaction(Predicate(::filteringOneInput)) // First input only filtered.
        ftxOneInput.verify()
        assertFailsWith<ComponentVisibilityException> { ftxOneInput.checkAllComponentsVisible(INPUTS_GROUP) }

        assertEquals(1, ftxOneInput.filteredComponentGroups.size) // We only add component groups that are not empty, thus in this case: the inputs only.
        assertEquals(1, ftxOneInput.filteredComponentGroups.firstOrNull { it.groupIndex == INPUTS_GROUP.ordinal }!!.components.size) // 1 input is present.
        assertEquals(1, ftxOneInput.filteredComponentGroups.firstOrNull { it.groupIndex == INPUTS_GROUP.ordinal }!!.nonces.size) // And its corresponding nonce.
        assertNotNull(ftxOneInput.filteredComponentGroups.firstOrNull { it.groupIndex == INPUTS_GROUP.ordinal }!!.partialMerkleTree) // And the Merkle tree.

        // The old client (receiving more component types than expected) is still compatible.
        val componentGroupsCompatibleA = listOf(
                inputGroup,
                outputGroup,
                commandGroup,
                notaryGroup,
                timeWindowGroup,
                signersGroup,
                newUnknownComponentGroup // A new unknown component with ordinal 100 that we cannot process.
        )
        val wireTransactionCompatibleA = WireTransaction(componentGroupsCompatibleA, privacySalt, 0)
        val ftxCompatible = wireTransactionCompatibleA.buildFilteredTransaction(Predicate(::filtering))
        ftxCompatible.verify()
        assertEquals(ftxInputs.inputs, ftxCompatible.inputs)
        assertEquals(wireTransactionCompatibleA.id, ftxCompatible.id)

        assertEquals(1, ftxCompatible.filteredComponentGroups.size)
        assertEquals(3, ftxCompatible.filteredComponentGroups.firstOrNull { it.groupIndex == INPUTS_GROUP.ordinal }!!.components.size)
        assertEquals(3, ftxCompatible.filteredComponentGroups.firstOrNull { it.groupIndex == INPUTS_GROUP.ordinal }!!.nonces.size)
        assertNotNull(ftxCompatible.filteredComponentGroups.firstOrNull { it.groupIndex == INPUTS_GROUP.ordinal }!!.partialMerkleTree)
        assertNull(wireTransactionCompatibleA.networkParametersHash)
        assertNull(ftxCompatible.networkParametersHash)

        // Now, let's allow everything, including the new component type that we cannot process.
        val ftxCompatibleAll = wireTransactionCompatibleA.buildFilteredTransaction(Predicate { true }) // All filtered, including the unknown component.
        ftxCompatibleAll.verify()
        assertEquals(wireTransactionCompatibleA.id, ftxCompatibleAll.id)

        // Check we received the last element that we cannot process (backwards compatibility).
        assertEquals(wireTransactionCompatibleA.componentGroups.size, ftxCompatibleAll.filteredComponentGroups.size)

        // Hide one component group only.
        // Filter inputs only.
        fun filterOutInputs(elem: Any): Boolean {
            return when (elem) {
                is StateRef -> false
                else -> true
            }
        }

        val ftxCompatibleNoInputs = wireTransactionCompatibleA.buildFilteredTransaction(Predicate(::filterOutInputs))
        ftxCompatibleNoInputs.verify()
        assertFailsWith<ComponentVisibilityException> { ftxCompatibleNoInputs.checkAllComponentsVisible(INPUTS_GROUP) }
        assertEquals(wireTransactionCompatibleA.componentGroups.size - 1, ftxCompatibleNoInputs.filteredComponentGroups.size)
        assertEquals(wireTransactionCompatibleA.componentGroups.map { it.groupIndex }.max()!!, ftxCompatibleNoInputs.groupHashes.size - 1)
    }

    @Test
    fun `Command visibility tests`() {
        // 1st and 3rd commands require a signature from KEY_1.
        val twoCommandsforKey1 = listOf(dummyCommand(DUMMY_KEY_1.public, DUMMY_KEY_2.public), dummyCommand(DUMMY_KEY_2.public), dummyCommand(DUMMY_KEY_1.public))
        val componentGroups = listOf(
                inputGroup,
                outputGroup,
                ComponentGroup(COMMANDS_GROUP.ordinal, twoCommandsforKey1.map { it.value.serialize() }),
                notaryGroup,
                timeWindowGroup,
                ComponentGroup(SIGNERS_GROUP.ordinal, twoCommandsforKey1.map { it.signers.serialize() }),
                newUnknownComponentGroup // A new unknown component with ordinal 100 that we cannot process.
        )
        val wtx = WireTransaction(componentGroups = componentGroups, privacySalt = PrivacySalt(), txVersion = 0)

        // Filter all commands.
        fun filterCommandsOnly(elem: Any): Boolean {
            return when (elem) {
                is Command<*> -> true // Even if one Command is filtered, all signers are automatically filtered as well
                else -> false
            }
        }

        // Filter out commands only.
        fun filterOutCommands(elem: Any): Boolean {
            return when (elem) {
                is Command<*> -> false
                else -> true
            }
        }

        // Filter KEY_1 commands.
        fun filterKEY1Commands(elem: Any): Boolean {
            return when (elem) {
                is Command<*> -> DUMMY_KEY_1.public in elem.signers
                else -> false
            }
        }

        // Filter only one KEY_1 command.
        fun filterTwoSignersCommands(elem: Any): Boolean {
            return when (elem) {
                is Command<*> -> elem.signers.size == 2 // dummyCommand(DUMMY_KEY_1.public) is filtered out.
                else -> false
            }
        }

        // Again filter only one KEY_1 command.
        fun filterSingleSignersCommands(elem: Any): Boolean {
            return when (elem) {
                is Command<*> -> elem.signers.size == 1 // dummyCommand(DUMMY_KEY_1.public, DUMMY_KEY_2.public) is filtered out.
                else -> false
            }
        }

        val allCommandsFtx = wtx.buildFilteredTransaction(Predicate(::filterCommandsOnly))
        val noCommandsFtx = wtx.buildFilteredTransaction(Predicate(::filterOutCommands))
        val key1CommandsFtx = wtx.buildFilteredTransaction(Predicate(::filterKEY1Commands))
        val oneKey1CommandFtxA = wtx.buildFilteredTransaction(Predicate(::filterTwoSignersCommands))
        val oneKey1CommandFtxB = wtx.buildFilteredTransaction(Predicate(::filterSingleSignersCommands))

        allCommandsFtx.checkCommandVisibility(DUMMY_KEY_1.public)
        assertFailsWith<ComponentVisibilityException> { noCommandsFtx.checkCommandVisibility(DUMMY_KEY_1.public) }
        key1CommandsFtx.checkCommandVisibility(DUMMY_KEY_1.public)
        assertFailsWith<ComponentVisibilityException> { oneKey1CommandFtxA.checkCommandVisibility(DUMMY_KEY_1.public) }
        assertFailsWith<ComponentVisibilityException> { oneKey1CommandFtxB.checkCommandVisibility(DUMMY_KEY_1.public) }

        allCommandsFtx.checkAllComponentsVisible(SIGNERS_GROUP)
        assertFailsWith<ComponentVisibilityException> { noCommandsFtx.checkAllComponentsVisible(SIGNERS_GROUP) } // If we filter out all commands, signers are not sent as well.
        key1CommandsFtx.checkAllComponentsVisible(SIGNERS_GROUP) // If at least one Command is visible, then all Signers are visible.
        oneKey1CommandFtxA.checkAllComponentsVisible(SIGNERS_GROUP) // If at least one Command is visible, then all Signers are visible.
        oneKey1CommandFtxB.checkAllComponentsVisible(SIGNERS_GROUP) // If at least one Command is visible, then all Signers are visible.

        // We don't send a list of signers.
        val componentGroupsCompatible = listOf(
                inputGroup,
                outputGroup,
                ComponentGroup(COMMANDS_GROUP.ordinal, twoCommandsforKey1.map { it.value.serialize() }),
                notaryGroup,
                timeWindowGroup,
                // ComponentGroup(SIGNERS_GROUP.ordinal, twoCommandsforKey1.map { it.signers.serialize() }),
                newUnknownComponentGroup // A new unknown component with ordinal 100 that we cannot process.
        )

        // Invalid Transaction. Sizes of CommandData and Signers (empty) do not match.
        assertFailsWith<IllegalStateException> { WireTransaction(componentGroups = componentGroupsCompatible, privacySalt = PrivacySalt(), txVersion = 0) }

        // We send smaller list of signers.
        val componentGroupsLessSigners = listOf(
                inputGroup,
                outputGroup,
                ComponentGroup(COMMANDS_GROUP.ordinal, twoCommandsforKey1.map { it.value.serialize() }),
                notaryGroup,
                timeWindowGroup,
                ComponentGroup(SIGNERS_GROUP.ordinal, twoCommandsforKey1.map { it.signers.serialize() }.subList(0, 1)), // Send first signer only.
                newUnknownComponentGroup // A new unknown component with ordinal 100 that we cannot process.
        )

        // Invalid Transaction. Sizes of CommandData and Signers (empty) do not match.
        assertFailsWith<IllegalStateException> { WireTransaction(componentGroups = componentGroupsLessSigners, privacySalt = PrivacySalt(), txVersion = 0) }

        // Test if there is no command to sign.
        val commandsNoKey1 = listOf(dummyCommand(DUMMY_KEY_2.public))

        val componentGroupsNoKey1ToSign = listOf(
                inputGroup,
                outputGroup,
                ComponentGroup(COMMANDS_GROUP.ordinal, commandsNoKey1.map { it.value.serialize() }),
                notaryGroup,
                timeWindowGroup,
                ComponentGroup(SIGNERS_GROUP.ordinal, commandsNoKey1.map { it.signers.serialize() }),
                newUnknownComponentGroup // A new unknown component with ordinal 100 that we cannot process.
        )

        val wtxNoKey1 = WireTransaction(componentGroups = componentGroupsNoKey1ToSign, privacySalt = PrivacySalt(), txVersion = 0)
        val allCommandsNoKey1Ftx = wtxNoKey1.buildFilteredTransaction(Predicate(::filterCommandsOnly))
        allCommandsNoKey1Ftx.checkCommandVisibility(DUMMY_KEY_1.public) // This will pass, because there are indeed no commands to sign in the original transaction.
    }

    @Test
    fun `FilteredTransaction signer manipulation tests`() {
        // Required to call the private constructor.
        val ftxConstructor = FilteredTransaction::class.constructors.first()

        // 1st and 3rd commands require a signature from KEY_1.
        val twoCommandsforKey1 = listOf(dummyCommand(DUMMY_KEY_1.public, DUMMY_KEY_2.public), dummyCommand(DUMMY_KEY_2.public), dummyCommand(DUMMY_KEY_1.public))
        val componentGroups = listOf(
                inputGroup,
                outputGroup,
                ComponentGroup(COMMANDS_GROUP.ordinal, twoCommandsforKey1.map { it.value.serialize() }),
                notaryGroup,
                timeWindowGroup,
                ComponentGroup(SIGNERS_GROUP.ordinal, twoCommandsforKey1.map { it.signers.serialize() })
        )
        val wtx = WireTransaction(componentGroups = componentGroups, privacySalt = PrivacySalt(), txVersion = 0)

        // Filter KEY_1 commands (commands 1 and 3).
        fun filterKEY1Commands(elem: Any): Boolean {
            return when (elem) {
                is Command<*> -> DUMMY_KEY_1.public in elem.signers
                else -> false
            }
        }

        // Filter KEY_2 commands (commands 1 and 2).
        fun filterKEY2Commands(elem: Any): Boolean {
            return when (elem) {
                is Command<*> -> DUMMY_KEY_2.public in elem.signers
                else -> false
            }
        }

        val key1CommandsFtx = wtx.buildFilteredTransaction(Predicate(::filterKEY1Commands))
        val key2CommandsFtx = wtx.buildFilteredTransaction(Predicate(::filterKEY2Commands))

        // val commandDataComponents = key1CommandsFtx.filteredComponentGroups[0].components
        val commandDataHashes = wtx.accessAvailableComponentHashes()[ComponentGroupEnum.COMMANDS_GROUP.ordinal]!!
        val noLastCommandDataPMT = PartialMerkleTree.build(
                MerkleTree.getMerkleTree(commandDataHashes),
                commandDataHashes.subList(0, 1)
        )
        val noLastCommandDataComponents = key1CommandsFtx.filteredComponentGroups[0].components.subList(0, 1)
        val noLastCommandDataNonces = key1CommandsFtx.filteredComponentGroups[0].nonces.subList(0, 1)
        val noLastCommandDataGroup = FilteredComponentGroup(
                COMMANDS_GROUP.ordinal,
                noLastCommandDataComponents,
                noLastCommandDataNonces,
                noLastCommandDataPMT
        )

        val signerComponents = key1CommandsFtx.filteredComponentGroups[1].components
        val signerHashes = wtx.accessAvailableComponentHashes()[ComponentGroupEnum.SIGNERS_GROUP.ordinal]!!
        val noLastSignerPMT = PartialMerkleTree.build(
                MerkleTree.getMerkleTree(signerHashes),
                signerHashes.subList(0, 2)
        )
        val noLastSignerComponents = key1CommandsFtx.filteredComponentGroups[1].components.subList(0, 2)
        val noLastSignerNonces = key1CommandsFtx.filteredComponentGroups[1].nonces.subList(0, 2)
        val noLastSignerGroup = FilteredComponentGroup(
                SIGNERS_GROUP.ordinal,
                noLastSignerComponents,
                noLastSignerNonces,
                noLastSignerPMT
        )
        val noLastSignerGroupSamePartialTree = FilteredComponentGroup(
                SIGNERS_GROUP.ordinal,
                noLastSignerComponents,
                noLastSignerNonces,
                key1CommandsFtx.filteredComponentGroups[1].partialMerkleTree) // We don't update that, so we can catch the index mismatch.

        val updatedFilteredComponentsNoSignersKey2 = listOf(key2CommandsFtx.filteredComponentGroups[0], noLastSignerGroup)
        val updatedFilteredComponentsNoSignersKey2SamePMT = listOf(key2CommandsFtx.filteredComponentGroups[0], noLastSignerGroupSamePartialTree)

        // There are only two components in key1CommandsFtx (commandData and signers).
        assertEquals(2, key1CommandsFtx.componentGroups.size)

        // Remove last signer for which there is a pointer from a visible commandData. This is the case of Key1.
        // This will result to an invalid transaction.
        // A command with no corresponding signer detected
        // because the pointer of CommandData (3rd leaf) cannot find a corresponding (3rd) signer.
        val updatedFilteredComponentsNoSignersKey1SamePMT = listOf(key1CommandsFtx.filteredComponentGroups[0], noLastSignerGroupSamePartialTree)
        assertFails { ftxConstructor.call(key1CommandsFtx.id, updatedFilteredComponentsNoSignersKey1SamePMT, key1CommandsFtx.groupHashes) }

        // Remove both last signer (KEY1) and related command.
        // Update partial Merkle tree for signers.
        val updatedFilteredComponentsNoLastCommandAndSigners = listOf(noLastCommandDataGroup, noLastSignerGroup)
        val ftxNoLastCommandAndSigners = ftxConstructor.call(key1CommandsFtx.id, updatedFilteredComponentsNoLastCommandAndSigners, key1CommandsFtx.groupHashes)
        // verify() will pass as the transaction is well-formed.
        ftxNoLastCommandAndSigners.verify()
        // checkCommandVisibility() will not pass, because checkAllComponentsVisible(ComponentGroupEnum.SIGNERS_GROUP) will fail.
        assertFailsWith<ComponentVisibilityException> { ftxNoLastCommandAndSigners.checkCommandVisibility(DUMMY_KEY_1.public) }

        // Remove last signer for which there is no pointer from a visible commandData. This is the case of Key2.
        // Do not change partial Merkle tree for signers.
        // This time the object can be constructed as there is no pointer mismatch.
        val ftxNoLastSigner = ftxConstructor.call(key2CommandsFtx.id, updatedFilteredComponentsNoSignersKey2SamePMT, key2CommandsFtx.groupHashes)
        // verify() will fail as we didn't change the partial Merkle tree.
        assertFailsWith<FilteredTransactionVerificationException> { ftxNoLastSigner.verify() }
        // checkCommandVisibility() will not pass.
        assertFailsWith<ComponentVisibilityException> { ftxNoLastSigner.checkCommandVisibility(DUMMY_KEY_2.public) }

        // Remove last signer for which there is no pointer from a visible commandData. This is the case of Key2.
        // Update partial Merkle tree for signers.
        val ftxNoLastSignerB = ftxConstructor.call(key2CommandsFtx.id, updatedFilteredComponentsNoSignersKey2, key2CommandsFtx.groupHashes)
        // verify() will pass, the transaction is well-formed.
        ftxNoLastSignerB.verify()
        // But, checkAllComponentsVisible() will not pass.
        assertFailsWith<ComponentVisibilityException> { ftxNoLastSignerB.checkCommandVisibility(DUMMY_KEY_2.public) }

        // Modify last signer (we have a pointer from commandData).
        // Update partial Merkle tree for signers.
        val alterSignerComponents = signerComponents.subList(0, 2) + signerComponents[1] // Third one is removed and the 2nd command is added twice.
        val alterSignersHashes = wtx.accessAvailableComponentHashes()[ComponentGroupEnum.SIGNERS_GROUP.ordinal]!!.subList(0, 2) + componentHash(key1CommandsFtx.filteredComponentGroups[1].nonces[2], alterSignerComponents[2])
        val alterMTree = MerkleTree.getMerkleTree(alterSignersHashes)
        val alterSignerPMTK = PartialMerkleTree.build(
                alterMTree,
                alterSignersHashes
        )

        val alterSignerGroup = FilteredComponentGroup(
                SIGNERS_GROUP.ordinal,
                alterSignerComponents,
                key1CommandsFtx.filteredComponentGroups[1].nonces,
                alterSignerPMTK
        )
        val alterFilteredComponents = listOf(key1CommandsFtx.filteredComponentGroups[0], alterSignerGroup)

        // Do not update groupHashes.
        val ftxAlterSigner = ftxConstructor.call(key1CommandsFtx.id, alterFilteredComponents, key1CommandsFtx.groupHashes)
        // Visible components in signers group cannot be verified against their partial Merkle tree.
        assertFailsWith<FilteredTransactionVerificationException> { ftxAlterSigner.verify() }
        // Also, checkAllComponentsVisible() will not pass (groupHash matching will fail).
        assertFailsWith<ComponentVisibilityException> { ftxAlterSigner.checkCommandVisibility(DUMMY_KEY_1.public) }

        // Update groupHashes.
        val ftxAlterSignerB = ftxConstructor.call(key1CommandsFtx.id, alterFilteredComponents, key1CommandsFtx.groupHashes.subList(0, 6) + alterMTree.hash)
        // Visible components in signers group cannot be verified against their partial Merkle tree.
        assertFailsWith<FilteredTransactionVerificationException> { ftxAlterSignerB.verify() }
        // Also, checkAllComponentsVisible() will not pass (top level Merkle tree cannot be verified against transaction's id).
        assertFailsWith<ComponentVisibilityException> { ftxAlterSignerB.checkCommandVisibility(DUMMY_KEY_1.public) }
    }

    @Test
    fun `parameters hash visibility`() {
        fun paramsFilter(elem: Any): Boolean = elem is NetworkParametersHash && elem.hash == paramsHash
        fun attachmentFilter(elem: Any): Boolean = elem is SecureHash && elem == paramsHash
        val attachments = ComponentGroup(ATTACHMENTS_GROUP.ordinal, listOf(paramsHash.serialize())) // Same hash as network parameters
        val componentGroups = listOf(
                inputGroup,
                outputGroup,
                attachments,
                commandGroup,
                notaryGroup,
                timeWindowGroup,
                signersGroup,
                networkParamsGroup
        )
        val wtx = WireTransaction(componentGroups, privacySalt, 0)
        val ftx1 = wtx.buildFilteredTransaction(Predicate(::paramsFilter)) // Filter only network parameters hash.
        ftx1.verify()
        assertEquals(wtx.id, ftx1.id)
        ftx1.checkAllComponentsVisible(PARAMETERS_GROUP)
        assertFailsWith<ComponentVisibilityException> { ftx1.checkAllComponentsVisible(ATTACHMENTS_GROUP) }
        // Filter only attachment.
        val ftx2 = wtx.buildFilteredTransaction(Predicate(::attachmentFilter))
        ftx2.verify()
        assertEquals(wtx.id, ftx2.id)
        ftx2.checkAllComponentsVisible(ATTACHMENTS_GROUP)
        assertFailsWith<ComponentVisibilityException> { ftx2.checkAllComponentsVisible(PARAMETERS_GROUP) }
    }
}

