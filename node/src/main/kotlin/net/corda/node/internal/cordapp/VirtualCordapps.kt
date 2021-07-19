package net.corda.node.internal.cordapp

import net.corda.core.cordapp.Cordapp
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.ContractUpgradeFlow
import net.corda.core.internal.cordapp.CordappImpl
import net.corda.core.internal.location
import net.corda.node.VersionInfo
import net.corda.node.internal.membership.flows.PublishMemberInfoResponderFlow
import net.corda.node.internal.membership.flows.SyncMembershipGroupCacheResponderFlow
import net.corda.node.services.network.FlowMembershipGroupService
import net.corda.notary.experimental.bftsmart.BFTSmartNotarySchemaV1
import net.corda.notary.experimental.bftsmart.BFTSmartNotaryService
import net.corda.notary.experimental.raft.RaftNotarySchemaV1
import net.corda.notary.experimental.raft.RaftNotaryService
import net.corda.notary.jpa.JPANotarySchemaV1
import net.corda.notary.jpa.JPANotaryService

internal object VirtualCordapp {
    /** A list of the core RPC flows present in Corda */
    private val coreRpcFlows = listOf(
            ContractUpgradeFlow.Initiate::class.java,
            ContractUpgradeFlow.Authorise::class.java,
            ContractUpgradeFlow.Deauthorise::class.java
    )

    /** A list of the membership group initiated flows **/
    private val membershipGroupInitiatedFlows = listOf(
            PublishMemberInfoResponderFlow::class.java,
            SyncMembershipGroupCacheResponderFlow::class.java
    )

    /** A Cordapp representing the core package which is not scanned automatically. */
    fun generateCore(versionInfo: VersionInfo): CordappImpl {
        return CordappImpl(
                contractClassNames = listOf(),
                initiatedFlows = membershipGroupInitiatedFlows,
                rpcFlows = coreRpcFlows,
                serviceFlows = listOf(),
                schedulableFlows = listOf(),
                services = listOf(FlowMembershipGroupService::class.java),
                serializationWhitelists = listOf(),
                serializationCustomSerializers = listOf(),
                checkpointCustomSerializers = listOf(),
                customSchemas = setOf(),
                info = Cordapp.Info.Default("corda-core", versionInfo.vendor, versionInfo.releaseVersion, "Open Source (Apache 2)"),
                allFlows = listOf(),
                jarPath = ContractUpgradeFlow.javaClass.location, // Core JAR location
                jarHash = SecureHash.allOnesHash,
                minimumPlatformVersion = versionInfo.platformVersion,
                targetPlatformVersion = versionInfo.platformVersion,
                notaryService = null,
                isLoaded = false
        )
    }

    /** A Cordapp for the built-in notary service implementation. */
    fun generateJPANotary(versionInfo: VersionInfo): CordappImpl {
        return CordappImpl(
                contractClassNames = listOf(),
                initiatedFlows = listOf(),
                rpcFlows = listOf(),
                serviceFlows = listOf(),
                schedulableFlows = listOf(),
                services = listOf(),
                serializationWhitelists = listOf(),
                serializationCustomSerializers = listOf(),
                checkpointCustomSerializers = listOf(),
                customSchemas = setOf(JPANotarySchemaV1),
                info = Cordapp.Info.Default("corda-notary", versionInfo.vendor, versionInfo.releaseVersion, "Open Source (Apache 2)"),
                allFlows = listOf(),
                jarPath = JPANotaryService::class.java.location,
                jarHash = SecureHash.allOnesHash,
                minimumPlatformVersion = versionInfo.platformVersion,
                targetPlatformVersion = versionInfo.platformVersion,
                notaryService = JPANotaryService::class.java,
                isLoaded = false,
                isVirtual = true
        )
    }

    /** A Cordapp for the built-in Raft notary service implementation. */
    fun generateRaftNotary(versionInfo: VersionInfo): CordappImpl {
        return CordappImpl(
                contractClassNames = listOf(),
                initiatedFlows = listOf(),
                rpcFlows = listOf(),
                serviceFlows = listOf(),
                schedulableFlows = listOf(),
                services = listOf(),
                serializationWhitelists = listOf(),
                serializationCustomSerializers = listOf(),
                checkpointCustomSerializers = listOf(),
                customSchemas = setOf(RaftNotarySchemaV1),
                info = Cordapp.Info.Default("corda-notary-raft", versionInfo.vendor, versionInfo.releaseVersion, "Open Source (Apache 2)"),
                allFlows = listOf(),
                jarPath = RaftNotaryService::class.java.location,
                jarHash = SecureHash.allOnesHash,
                minimumPlatformVersion = versionInfo.platformVersion,
                targetPlatformVersion = versionInfo.platformVersion,
                notaryService = RaftNotaryService::class.java,
                isLoaded = false
        )
    }

    /** A Cordapp for the built-in BFT-Smart notary service implementation. */
    fun generateBFTSmartNotary(versionInfo: VersionInfo): CordappImpl {
        return CordappImpl(
                contractClassNames = listOf(),
                initiatedFlows = listOf(),
                rpcFlows = listOf(),
                serviceFlows = listOf(),
                schedulableFlows = listOf(),
                services = listOf(),
                serializationWhitelists = listOf(),
                serializationCustomSerializers = listOf(),
                checkpointCustomSerializers = listOf(),
                customSchemas = setOf(BFTSmartNotarySchemaV1),
                info = Cordapp.Info.Default("corda-notary-bft-smart", versionInfo.vendor, versionInfo.releaseVersion, "Open Source (Apache 2)"),
                allFlows = listOf(),
                jarPath = BFTSmartNotaryService::class.java.location,
                jarHash = SecureHash.allOnesHash,
                minimumPlatformVersion = versionInfo.platformVersion,
                targetPlatformVersion = versionInfo.platformVersion,
                notaryService = BFTSmartNotaryService::class.java,
                isLoaded = false
        )
    }
}