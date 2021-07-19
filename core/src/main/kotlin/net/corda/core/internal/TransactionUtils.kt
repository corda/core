package net.corda.core.internal

import net.corda.core.KeepForDJVM
import net.corda.core.contracts.*
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.SecureHash.Companion.SHA2_256
import net.corda.core.crypto.componentHash
import net.corda.core.crypto.hashAs
import net.corda.core.flows.FlowLogic
import net.corda.core.identity.Party
import net.corda.core.serialization.*
import net.corda.core.transactions.*
import net.corda.core.utilities.OpaqueBytes
import java.io.ByteArrayOutputStream
import java.security.PublicKey
import kotlin.reflect.KClass

/** Constructs a [NotaryChangeWireTransaction]. */
class NotaryChangeTransactionBuilder(val inputs: List<StateRef>,
                                     val notary: Party,
                                     val newNotary: Party,
                                     val networkParametersHash: SecureHash) {
    var hashAlgorithm = SHA2_256
        set(value) {
            field = value.toUpperCase()
        }

    fun setHashAlgorithm(hashAlgorithm: String): NotaryChangeTransactionBuilder {
        this.hashAlgorithm = hashAlgorithm
        return this
    }

    fun build(): NotaryChangeWireTransaction {
        val components = listOf(inputs, notary, newNotary, networkParametersHash).map { it.serialize() }
        return NotaryChangeWireTransaction(components, hashAlgorithm)
    }
}

/** Constructs a [ContractUpgradeWireTransaction]. */
class ContractUpgradeTransactionBuilder(
        val inputs: List<StateRef>,
        val notary: Party,
        val legacyContractAttachmentId: SecureHash,
        val upgradedContractClassName: ContractClassName,
        val upgradedContractAttachmentId: SecureHash,
        privacySalt: PrivacySalt = PrivacySalt(),
        val networkParametersHash: SecureHash) {
    var privacySalt: PrivacySalt = privacySalt
        private set

    var hashAlgorithm = SHA2_256
        set(value) {
            field = value.toUpperCase()
        }

    fun setHashAlgorithm(hashAlgorithm: String): ContractUpgradeTransactionBuilder {
        this.hashAlgorithm = hashAlgorithm
        return this
    }

    fun resalt(): ContractUpgradeTransactionBuilder {
        privacySalt = PrivacySalt.createFor(hashAlgorithm)
        return this
    }

    fun build(): ContractUpgradeWireTransaction {
        val components = listOf(inputs, notary, legacyContractAttachmentId, upgradedContractClassName, upgradedContractAttachmentId, networkParametersHash).map { it.serialize() }
        return ContractUpgradeWireTransaction(components, privacySalt, hashAlgorithm)
    }
}

/** Concatenates the hash components into a single [ByteArray] and returns its hash. */
fun combinedHash(components: Iterable<SecureHash>): SecureHash {
    val stream = ByteArrayOutputStream()
    components.forEach {
        stream.write(it.bytes)
    }
    return stream.toByteArray().hashAs(components.first().algorithm)
}

/**
 * This function knows how to deserialize a transaction component group.
 *
 * In case the [componentGroups] is an instance of [LazyMappedList], this function will just use the original deserialized version, and avoid an unnecessary deserialization.
 * The [forceDeserialize] will force deserialization. In can be used in case the SerializationContext changes.
 */
fun <T : Any> deserialiseComponentGroup(componentGroups: List<ComponentGroup>,
                                        clazz: KClass<T>,
                                        groupEnum: ComponentGroupEnum,
                                        forceDeserialize: Boolean = false,
                                        factory: SerializationFactory = SerializationFactory.defaultFactory,
                                        context: SerializationContext = factory.defaultContext): List<T> {
    val group = componentGroups.firstOrNull { it.groupIndex == groupEnum.ordinal }

    if (group == null || group.components.isEmpty()) {
        return emptyList()
    }

    // If the componentGroup is a [LazyMappedList] it means that the original deserialized version is already available.
    val components = group.components
    if (!forceDeserialize && components is LazyMappedList<*, OpaqueBytes>) {
        return uncheckedCast(components.originalList)
    }

    return components.lazyMapped { component, internalIndex ->
        try {
            factory.deserialize(component, clazz.java, context)
        } catch (e: MissingAttachmentsException) {
            /**
             * [ServiceHub.signInitialTransaction] forgets to declare that
             * it may throw any checked exceptions. Wrap this one inside
             * an unchecked version to avoid breaking Java CorDapps.
             */
            throw MissingAttachmentsRuntimeException(e.ids, e.message, e)
        } catch (e: Exception) {
            throw TransactionDeserialisationException(groupEnum, internalIndex, e)
        }
    }
}

/**
 * Exception raised if an error was encountered while attempting to deserialise a component group in a transaction.
 */
class TransactionDeserialisationException(groupEnum: ComponentGroupEnum, index: Int, cause: Exception):
        RuntimeException("Failed to deserialise group $groupEnum at index $index in transaction: ${cause.message}", cause)

/**
 * Method to deserialise Commands from its two groups:
 *  * COMMANDS_GROUP which contains the CommandData part
 *  * and SIGNERS_GROUP which contains the Signers part.
 *
 *  This method used the [deserialiseComponentGroup] method.
 */
fun deserialiseCommands(
        componentGroups: List<ComponentGroup>,
        forceDeserialize: Boolean = false,
        factory: SerializationFactory = SerializationFactory.defaultFactory,
        @Suppress("UNUSED_PARAMETER") context: SerializationContext = factory.defaultContext
): List<Command<*>> {
    // TODO: we could avoid deserialising unrelated signers.
    //      However, current approach ensures the transaction is not malformed
    //      and it will throw if any of the signers objects is not List of public keys).
    val signersList: List<List<PublicKey>> = uncheckedCast(deserialiseComponentGroup(componentGroups, List::class, ComponentGroupEnum.SIGNERS_GROUP, forceDeserialize))
    val commandDataList: List<CommandData> = deserialiseComponentGroup(componentGroups, CommandData::class, ComponentGroupEnum.COMMANDS_GROUP, forceDeserialize)
    val group = componentGroups.firstOrNull { it.groupIndex == ComponentGroupEnum.COMMANDS_GROUP.ordinal }
    return if (group is FilteredComponentGroup) {
        check(commandDataList.size <= signersList.size) {
            "Invalid Transaction. Less Signers (${signersList.size}) than CommandData (${commandDataList.size}) objects"
        }
        val componentHashes = group.components.mapIndexed { index, component -> componentHash(group.nonces[index], component) }
        val leafIndices = componentHashes.map { group.partialMerkleTree.leafIndex(it) }
        if (leafIndices.isNotEmpty())
            check(leafIndices.max()!! < signersList.size) { "Invalid Transaction. A command with no corresponding signer detected" }
        commandDataList.lazyMapped { commandData, index -> Command(commandData, signersList[leafIndices[index]]) }
    } else {
        // It is a WireTransaction
        // or a FilteredTransaction with no Commands (in which case group is null).
        check(commandDataList.size == signersList.size) {
            "Invalid Transaction. Sizes of CommandData (${commandDataList.size}) and Signers (${signersList.size}) do not match"
        }
        commandDataList.lazyMapped { commandData, index -> Command(commandData, signersList[index]) }
    }
}

/**
 * Creating list of [ComponentGroup] used in one of the constructors of [WireTransaction] required
 * for backwards compatibility purposes.
 */
fun createComponentGroups(inputs: List<StateRef>,
                          outputs: List<TransactionState<ContractState>>,
                          commands: List<Command<*>>,
                          attachments: List<SecureHash>,
                          notary: Party?,
                          timeWindow: TimeWindow?,
                          references: List<StateRef>,
                          networkParametersHash: SecureHash?): List<ComponentGroup> {
    val serialize = { value: Any, _: Int -> value.serialize() }
    val componentGroupMap: MutableList<ComponentGroup> = mutableListOf()
    if (inputs.isNotEmpty()) componentGroupMap.add(ComponentGroup(ComponentGroupEnum.INPUTS_GROUP.ordinal, inputs.lazyMapped(serialize)))
    if (references.isNotEmpty()) componentGroupMap.add(ComponentGroup(ComponentGroupEnum.REFERENCES_GROUP.ordinal, references.lazyMapped(serialize)))
    if (outputs.isNotEmpty()) componentGroupMap.add(ComponentGroup(ComponentGroupEnum.OUTPUTS_GROUP.ordinal, outputs.lazyMapped(serialize)))
    // Adding commandData only to the commands group. Signers are added in their own group.
    if (commands.isNotEmpty()) componentGroupMap.add(ComponentGroup(ComponentGroupEnum.COMMANDS_GROUP.ordinal, commands.map { it.value }.lazyMapped(serialize)))
    if (attachments.isNotEmpty()) componentGroupMap.add(ComponentGroup(ComponentGroupEnum.ATTACHMENTS_GROUP.ordinal, attachments.lazyMapped(serialize)))
    if (notary != null) componentGroupMap.add(ComponentGroup(ComponentGroupEnum.NOTARY_GROUP.ordinal, listOf(notary).lazyMapped(serialize)))
    if (timeWindow != null) componentGroupMap.add(ComponentGroup(ComponentGroupEnum.TIMEWINDOW_GROUP.ordinal, listOf(timeWindow).lazyMapped(serialize)))
    // Adding signers to their own group. This is required for command visibility purposes: a party receiving
    // a FilteredTransaction can now verify it sees all the commands it should sign.
    if (commands.isNotEmpty()) componentGroupMap.add(ComponentGroup(ComponentGroupEnum.SIGNERS_GROUP.ordinal, commands.map { it.signers }.lazyMapped(serialize)))
    if (networkParametersHash != null) componentGroupMap.add(ComponentGroup(ComponentGroupEnum.PARAMETERS_GROUP.ordinal, listOf(networkParametersHash.serialize())))
    return componentGroupMap
}

/**
 * A SerializedStateAndRef is a pair (BinaryStateRepresentation, StateRef).
 * The [serializedState] is the actual component from the original wire transaction.
 */
@KeepForDJVM
data class SerializedStateAndRef(val serializedState: SerializedBytes<TransactionState<ContractState>>, val ref: StateRef) {
    fun toStateAndRef(): StateAndRef<ContractState> = StateAndRef(serializedState.deserialize(), ref)
}

/** Check that network parameters hash on this transaction is the current hash for the network. */
fun FlowLogic<*>.checkParameterHash(networkParametersHash: SecureHash?) {
    // Transactions created on Corda 3.x or below do not contain network parameters,
    // so no checking is done until the minimum platform version is at least 4.
    if (networkParametersHash == null) {
        if (serviceHub.networkParameters.minimumPlatformVersion < PlatformVersionSwitches.NETWORK_PARAMETERS_COMPONENT_GROUP) return
        else throw IllegalArgumentException("Transaction for notarisation doesn't contain network parameters hash.")
    } else {
        serviceHub.networkParametersService.lookup(networkParametersHash) ?: throw IllegalArgumentException("Transaction for notarisation contains unknown parameters hash: $networkParametersHash")
    }

    // TODO: [ENT-2666] Implement network parameters fuzzy checking. By design in Corda network we have propagation time delay.
    //       We will never end up in perfect synchronization with all the nodes. However, network parameters update process
    //       lets us predict what is the reasonable time window for changing parameters on most of the nodes.
    //       For now we don't check whether the attached network parameters match the current ones.
}

val SignedTransaction.dependencies: Set<SecureHash>
    get() = (inputs.asSequence() + references.asSequence()).map { it.txhash }.toSet()

