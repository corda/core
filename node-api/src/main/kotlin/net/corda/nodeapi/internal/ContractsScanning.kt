package net.corda.nodeapi.internal

import io.github.classgraph.ClassGraph
import net.corda.core.contracts.Contract
import net.corda.core.contracts.ContractClassName
import net.corda.core.contracts.UpgradedContract
import net.corda.core.contracts.UpgradedContractWithLegacyConstraint
import net.corda.core.crypto.SecureHash
import net.corda.core.internal.*
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.Collections.singleton

// When scanning of the CorDapp Jar is performed without "corda-core.jar" being in the classpath, there is no way to appreciate
// relationships between those interfaces, therefore they have to be listed explicitly.
val coreContractClasses = setOf(Contract::class, UpgradedContractWithLegacyConstraint::class, UpgradedContract::class)

interface ContractsJar {
    val hash: SecureHash
    fun scan(): List<ContractClassName>
}

class ContractsJarFile(private val file: Path, private val lowMemoryMode: Boolean = false) : ContractsJar {
    override val hash: SecureHash by lazy(LazyThreadSafetyMode.NONE, file::hash)

    override fun scan(): List<ContractClassName> {
        val scanResult = ClassGraph().overrideClasspath(singleton(file)).setMaxBufferedJarRAMSize(if (lowMemoryMode) ClassGraphConstants.LOW_MEMORY_MODE_MAX_BUFFERED_JAR_SIZE else ClassGraphConstants.DEFAULT_MAX_BUFFERED_JAR_SIZE).enableClassInfo().pooledScan()

        return scanResult.use { result ->
            coreContractClasses
                    .flatMap { result.getClassesImplementing(it.qualifiedName)}
                    .filterNot { it.isAbstract }
                    .filterNot { it.isInterface }
                    .map { it.name }
                    .toList()
        }
    }
}

private val logger = LoggerFactory.getLogger("ClassloaderUtils")

fun <T> withContractsInJar(jarInputStream: InputStream, lowMemoryMode: Boolean, withContracts: (List<ContractClassName>, InputStream) -> T): T {
    val tempFile = Files.createTempFile("attachment", ".jar")
    try {
        jarInputStream.use {
            it.copyTo(tempFile, StandardCopyOption.REPLACE_EXISTING)
        }
        val cordappJar = tempFile.toAbsolutePath()
        val contracts = logElapsedTime("Contracts loading for '$cordappJar'", logger) {
            ContractsJarFile(tempFile.toAbsolutePath(), lowMemoryMode).scan()
        }
        return tempFile.read { withContracts(contracts, it) }
    } finally {
        tempFile.deleteIfExists()
    }
}
