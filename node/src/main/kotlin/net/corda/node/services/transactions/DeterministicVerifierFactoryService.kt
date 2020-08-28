package net.corda.node.services.transactions

import net.corda.core.internal.BasicVerifier
import net.corda.core.internal.Verifier
import net.corda.core.serialization.ConstructorForDeserialization
import net.corda.core.serialization.CordaSerializable
import net.corda.core.serialization.CordaSerializationTransformEnumDefault
import net.corda.core.serialization.CordaSerializationTransformEnumDefaults
import net.corda.core.serialization.CordaSerializationTransformRename
import net.corda.core.serialization.CordaSerializationTransformRenames
import net.corda.core.serialization.DeprecatedConstructorForDeserialization
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.transactions.LedgerTransaction
import net.corda.djvm.SandboxConfiguration
import net.corda.djvm.analysis.AnalysisConfiguration
import net.corda.djvm.execution.ExecutionProfile
import net.corda.djvm.rewiring.ByteCode
import net.corda.djvm.rewiring.ByteCodeKey
import net.corda.djvm.source.ApiSource
import net.corda.djvm.source.UserPathSource
import net.corda.djvm.source.UserSource
import net.corda.node.internal.djvm.DeterministicVerifier
import java.net.URL
import java.net.URLClassLoader
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Consumer
import java.util.function.UnaryOperator

interface VerifierFactoryService : UnaryOperator<LedgerTransaction>, AutoCloseable

class DeterministicVerifierFactoryService(
    private val bootstrapSource: ApiSource,
    private val cordaSource: UserSource,
    private val lowMemoryMode: Boolean = false
) : SingletonSerializeAsToken(), VerifierFactoryService {
    private val baseSandboxConfiguration: SandboxConfiguration
    private val cordappByteCodeCache = ConcurrentHashMap<ByteCodeKey, ByteCode>()

    init {
        val baseAnalysisConfiguration = AnalysisConfiguration.createRoot(
            userSource = cordaSource,
            visibleAnnotations = setOf(
                CordaSerializable::class.java,
                CordaSerializationTransformEnumDefault::class.java,
                CordaSerializationTransformEnumDefaults::class.java,
                CordaSerializationTransformRename::class.java,
                CordaSerializationTransformRenames::class.java,
                ConstructorForDeserialization::class.java,
                DeprecatedConstructorForDeserialization::class.java
            ),
            bootstrapSource = bootstrapSource,
            overrideClasses = setOf(
                /**
                 * These classes are all duplicated into the sandbox
                 * without the DJVM modifying their byte-code first.
                 * The goal is to delegate cryptographic operations
                 * out to the Node rather than perform them inside
                 * the sandbox, because this is MUCH FASTER.
                 */
                sandbox.net.corda.core.crypto.Crypto::class.java.name,
                "sandbox.net.corda.core.crypto.DJVM",
                "sandbox.net.corda.core.crypto.DJVMPublicKey",
                "sandbox.net.corda.core.crypto.internal.ProviderMapKt"
            )
        )

        baseSandboxConfiguration = SandboxConfiguration.createFor(
            analysisConfiguration = baseAnalysisConfiguration,
            profile = NODE_PROFILE
        )
    }

    /**
     * Generate sandbox classes for every Corda jar with META-INF/DJVM-preload.
     */
    fun generateSandbox(): DeterministicVerifierFactoryService {
        baseSandboxConfiguration.preload()
        return this
    }

    override fun apply(ledgerTransaction: LedgerTransaction): LedgerTransaction {
        // Specialise the LedgerTransaction here so that
        // contracts are verified inside the DJVM!
        return ledgerTransaction.specialise(::specialise)
    }

    private fun specialise(ltx: LedgerTransaction, classLoader: ClassLoader): Verifier {
        return (classLoader as? URLClassLoader)?.run {
            DeterministicVerifier(ltx, classLoader, createSandbox(classLoader.urLs), lowMemoryMode)
        } ?: BasicVerifier(ltx, classLoader)
    }

    private fun createSandbox(userSource: Array<URL>): SandboxConfiguration {
        return baseSandboxConfiguration.createChild(UserPathSource(userSource), Consumer {
            it.setExternalCache(cordappByteCodeCache)
        })
    }

    override fun close() {
        bootstrapSource.use {
            cordaSource.close()
        }
    }

    private companion object {
        private val NODE_PROFILE = ExecutionProfile(
            allocationCostThreshold = 1024 * 1024 * 1024,
            invocationCostThreshold = 100_000_000,
            jumpCostThreshold = 500_000_000,
            throwCostThreshold = 1_000_000
        )
    }
}

class BasicVerifierFactoryService : VerifierFactoryService {
    override fun apply(ledgerTransaction: LedgerTransaction)= ledgerTransaction
    override fun close() {}
}
