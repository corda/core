package net.corda.nodeapi.internal.network

import net.corda.core.contracts.ContractClassName
import net.corda.core.internal.div
import net.corda.core.internal.exists
import net.corda.core.internal.readAllLines
import net.corda.core.internal.toMultiMap
import net.corda.core.node.NetworkParameters
import net.corda.core.node.services.AttachmentId
import net.corda.nodeapi.internal.ContractsJar
import org.slf4j.LoggerFactory
import java.nio.file.Path

private const val EXCLUDE_WHITELIST_FILE_NAME = "exclude_whitelist.txt"
private const val INCLUDE_WHITELIST_FILE_NAME = "include_whitelist.txt"
private val logger = LoggerFactory.getLogger("net.corda.nodeapi.internal.network.WhitelistGenerator")

fun generateWhitelist(networkParameters: NetworkParameters?,
                      excludeContracts: List<ContractClassName>,
                      includeContracts: List<ContractClassName>,
                      cordappJars: List<ContractsJar>): Map<ContractClassName, List<AttachmentId>> {
    val existingWhitelist = networkParameters?.whitelistedContractImplementations ?: emptyMap()

    if (excludeContracts.isNotEmpty()) {
        logger.info("Exclude contracts from whitelist: ${excludeContracts.joinToString()}")
        existingWhitelist.keys.forEach {
            require(it !in excludeContracts) { "$it is already part of the existing whitelist and cannot be excluded." }
        }
    }

    val intersection = excludeContracts.intersect(includeContracts)
    require(intersection.isEmpty()) { "Contract classes $intersection cannot be defined in both $EXCLUDE_WHITELIST_FILE_NAME and $INCLUDE_WHITELIST_FILE_NAME." }

    val newWhiteList = cordappJars
            .flatMap { jar -> (jar.scan() - excludeContracts + includeContracts).map { it to jar.hash } }
            .toMultiMap()

    return (newWhiteList.keys + existingWhitelist.keys).associateBy({ it }) {
        val existingHashes = existingWhitelist[it] ?: emptyList()
        val newHashes = newWhiteList[it] ?: emptyList()
        (existingHashes + newHashes).distinct()
    }
}

fun readExcludeWhitelist(directory: Path): List<String> = readList(directory, EXCLUDE_WHITELIST_FILE_NAME)

fun readIncludeWhitelist(directory: Path): List<String> = readList(directory, INCLUDE_WHITELIST_FILE_NAME)

fun readList(directory: Path, fileName: String): List<String> {
    val file = directory / fileName
    return if (file.exists()) file.readAllLines().map(String::trim) else emptyList()
}