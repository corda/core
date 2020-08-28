package net.corda.node.internal.djvm

import net.corda.core.contracts.CommandData
import net.corda.core.contracts.ComponentGroupEnum.COMMANDS_GROUP
import net.corda.core.contracts.ComponentGroupEnum.OUTPUTS_GROUP
import net.corda.core.contracts.ComponentGroupEnum.SIGNERS_GROUP
import net.corda.core.contracts.TransactionState
import net.corda.core.contracts.TransactionVerificationException
import net.corda.core.crypto.SecureHash
import net.corda.core.internal.ContractVerifier
import net.corda.core.internal.Verifier
import net.corda.core.internal.getNamesOfClassesImplementing
import net.corda.core.serialization.SerializationCustomSerializer
import net.corda.core.serialization.SerializationWhitelist
import net.corda.core.serialization.serialize
import net.corda.core.transactions.LedgerTransaction
import net.corda.djvm.SandboxConfiguration
import net.corda.djvm.execution.ExecutionSummary
import net.corda.djvm.execution.IsolatedTask
import net.corda.djvm.execution.SandboxException
import net.corda.djvm.messages.Message
import net.corda.djvm.rewiring.SandboxClassLoader
import net.corda.djvm.source.ClassSource
import net.corda.node.djvm.LtxFactory
import java.util.function.Function
import kotlin.collections.LinkedHashSet

class DeterministicVerifier(
    ltx: LedgerTransaction,
    transactionClassLoader: ClassLoader,
    private val sandboxConfiguration: SandboxConfiguration,
    private val lowMemoryMode: Boolean = false
) : Verifier(ltx, transactionClassLoader) {
    /**
     * Read the whitelisted classes without using the [java.util.ServiceLoader] mechanism
     * because the whitelists themselves are untrusted.
     */
    private fun getSerializationWhitelistNames(classLoader: ClassLoader): Set<String> {
        return classLoader.getResources("META-INF/services/${SerializationWhitelist::class.java.name}").asSequence()
            .flatMapTo(LinkedHashSet()) { url ->
                url.openStream().bufferedReader().useLines { lines ->
                    // Parse file format, as documented for java.util.ServiceLoader:
                    // - Remove everything after comment character '#'.
                    // - Strip whitespace.
                    // - Ignore empty lines.
                    lines.map { it.substringBefore('#') }.map(String::trim).filterNot(String::isEmpty).toList()
                }.asSequence()
            }
    }

    override fun verifyContracts() {
        val customSerializerNames = getNamesOfClassesImplementing(transactionClassLoader, SerializationCustomSerializer::class.java, lowMemoryMode = lowMemoryMode)
        val serializationWhitelistNames = getSerializationWhitelistNames(transactionClassLoader)
        val result = IsolatedTask(ltx.id.toString(), sandboxConfiguration).run<Any>(Function { classLoader ->
            (classLoader.parent as? SandboxClassLoader)?.apply {
                /**
                 * We don't need to add either Java APIs or Corda's own classes
                 * into the external cache because these are already being cached
                 * more efficiently inside the [SandboxConfiguration].
                 *
                 * The external cache is for this Nodes's CorDapps, where classes
                 * with the same names may appear in multiple different jars.
                 */
                externalCaching = false
            }

            val taskFactory = classLoader.createRawTaskFactory().compose(classLoader.createSandboxFunction())
            val sandboxBasicInput = classLoader.createBasicInput()

            /**
             * Deserialise the [LedgerTransaction] again into something
             * that we can execute inside the DJVM's sandbox.
             */
            val sandboxTx = ltx.transform { componentGroups, serializedInputs, serializedReferences ->
                val serializer = Serializer(classLoader, customSerializerNames, serializationWhitelistNames)
                val componentFactory = ComponentFactory(
                    classLoader,
                    taskFactory,
                    sandboxBasicInput,
                    serializer,
                    componentGroups
                )
                val attachmentFactory = AttachmentFactory(
                    classLoader,
                    taskFactory,
                    sandboxBasicInput,
                    serializer
                )

                val idData = ltx.id.serialize()
                val notaryData = ltx.notary?.serialize()
                val timeWindowData = ltx.timeWindow?.serialize()
                val privacySaltData = ltx.privacySalt.serialize()
                val networkingParametersData = ltx.networkParameters?.serialize()

                val createSandboxTx = taskFactory.apply(LtxFactory::class.java)
                createSandboxTx.apply(arrayOf(
                    serializer.deserialize(serializedInputs),
                    componentFactory.toSandbox(OUTPUTS_GROUP, TransactionState::class.java),
                    CommandFactory(taskFactory).toSandbox(
                        componentFactory.toSandbox(SIGNERS_GROUP, List::class.java),
                        componentFactory.toSandbox(COMMANDS_GROUP, CommandData::class.java),
                        componentFactory.calculateLeafIndicesFor(COMMANDS_GROUP)
                    ),
                    attachmentFactory.toSandbox(ltx.attachments),
                    serializer.deserialize(idData),
                    serializer.deserialize(notaryData),
                    serializer.deserialize(timeWindowData),
                    serializer.deserialize(privacySaltData),
                    serializer.deserialize(networkingParametersData),
                    serializer.deserialize(serializedReferences)
                ))
            }

            val verifier = taskFactory.apply(ContractVerifier::class.java)

            // Now execute the contract verifier task within the sandbox...
            verifier.apply(sandboxTx)
        })

        with (result.costs) {
            logger.info("Verify {} complete: allocations={}, invocations={}, jumps={}, throws={}",
                        ltx.id, allocations, invocations, jumps, throws)
        }

        result.exception?.run {
            val sandboxEx = SandboxException(
                Message.getMessageFromException(this),
                result.identifier,
                ClassSource.fromClassName(ContractVerifier::class.java.name),
                ExecutionSummary(result.costs),
                this
            )
            logger.error("Error validating transaction ${ltx.id}.", sandboxEx)
            throw DeterministicVerificationException(ltx.id, sandboxEx.message ?: "", sandboxEx)
        }
    }
}

class DeterministicVerificationException(txId: SecureHash, message: String, cause: Throwable)
    : TransactionVerificationException(txId, message, cause)
