package net.corda.node.services.rpc

import net.corda.core.internal.div
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.node.internal.artemis.BrokerJaasLoginModule
import net.corda.node.internal.artemis.SecureArtemisConfiguration
import net.corda.node.services.config.shell.INTERNAL_SHELL_USER
import net.corda.nodeapi.BrokerRpcSslOptions
import net.corda.nodeapi.RPCApi
import net.corda.nodeapi.internal.ArtemisConstants
import net.corda.nodeapi.internal.ArtemisMessagingComponent
import net.corda.nodeapi.internal.ArtemisTcpTransport.Companion.rpcAcceptorTcpTransport
import net.corda.nodeapi.internal.ArtemisTcpTransport.Companion.rpcInternalAcceptorTcpTransport
import net.corda.nodeapi.internal.config.MutualSslConfiguration
import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration
import org.apache.activemq.artemis.core.security.Role
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy
import org.apache.activemq.artemis.core.settings.impl.AddressSettings
import java.nio.file.Path

internal class RpcBrokerConfiguration(baseDirectory: Path, maxMessageSize: Int, journalBufferTimeout: Int?, jmxEnabled: Boolean,
                                      address: NetworkHostAndPort, adminAddress: NetworkHostAndPort?, sslOptions: BrokerRpcSslOptions?,
                                      useSsl: Boolean, nodeConfiguration: MutualSslConfiguration, shouldStartLocalShell: Boolean, val lowMemoryMode: Boolean = false) : SecureArtemisConfiguration() {
    val loginListener: (String) -> Unit

    init {
        name = "RPC"

        setDirectories(baseDirectory)

        val acceptorConfigurationsSet = mutableSetOf(
                rpcAcceptorTcpTransport(address, sslOptions, useSsl, lowMemoryMode)
        )
        adminAddress?.let {
            acceptorConfigurationsSet += rpcInternalAcceptorTcpTransport(it, nodeConfiguration)
        }
        acceptorConfigurations = acceptorConfigurationsSet

        queueConfigurations = queueConfigurations()

        managementNotificationAddress = SimpleString(ArtemisMessagingComponent.NOTIFICATIONS_ADDRESS)
        addressesSettings = mapOf(
                "${RPCApi.RPC_CLIENT_QUEUE_NAME_PREFIX}.#" to AddressSettings().apply {
                    maxSizeBytes = 5L * maxMessageSize
                    addressFullMessagePolicy = AddressFullMessagePolicy.PAGE
                    pageSizeBytes = 1L * maxMessageSize
                }
        )

        globalMaxSize = Runtime.getRuntime().maxMemory() / 8
        initialiseSettings(maxMessageSize, journalBufferTimeout)

        val nodeInternalRole = Role(BrokerJaasLoginModule.NODE_RPC_ROLE, true, true, true, true, true, true, true, true, true, true)

        val addRPCRoleToUsers = if (shouldStartLocalShell) listOf(INTERNAL_SHELL_USER) else emptyList()
        val rolesAdderOnLogin = RolesAdderOnLogin(addRPCRoleToUsers) { username ->
            "${RPCApi.RPC_CLIENT_QUEUE_NAME_PREFIX}.$username.#" to setOf(nodeInternalRole, restrictedRole(
                    "${RPCApi.RPC_CLIENT_QUEUE_NAME_PREFIX}.$username",
                    consume = true,
                    createNonDurableQueue = true,
                    deleteNonDurableQueue = true)
            )
        }

        configureAddressSecurity(nodeInternalRole, rolesAdderOnLogin)

        if (jmxEnabled) {
            enableJmx()
        }

        loginListener = { username: String -> rolesAdderOnLogin.onLogin(username) }
    }

    private fun configureAddressSecurity(nodeInternalRole: Role, rolesAdderOnLogin: RolesAdderOnLogin) {
        securityRoles["${ArtemisMessagingComponent.INTERNAL_PREFIX}#"] = setOf(nodeInternalRole)
        securityRoles[RPCApi.RPC_SERVER_QUEUE_NAME] = setOf(nodeInternalRole, restrictedRole(BrokerJaasLoginModule.RPC_ROLE, send = true))
        securitySettingPlugins.add(rolesAdderOnLogin)
        securityInvalidationInterval = ArtemisMessagingComponent.SECURITY_INVALIDATION_INTERVAL
    }

    private fun enableJmx() {
        isJMXManagementEnabled = true
        isJMXUseBrokerName = true
    }

    private fun initialiseSettings(maxMessageSize: Int, journalBufferTimeout: Int?) {
        // Enable built in message deduplication. Note we still have to do our own as the delayed commits
        // and our own definition of commit mean that the built in deduplication cannot remove all duplicates.
        idCacheSize = if (lowMemoryMode) ArtemisConstants.LOW_MEMORY_MODE_ID_CACHE_SIZE else ArtemisConstants.DEFAULT_ID_CACHE_SIZE // Artemis Default duplicate cache size i.e. a guess
        isPersistIDCache = true
        isPopulateValidatedUser = true
        journalBufferSize_NIO = maxMessageSize // Artemis default is 490KiB - required to address IllegalArgumentException (when Artemis uses Java NIO): Record is too large to store.
        journalBufferTimeout_NIO = journalBufferTimeout ?: ActiveMQDefaultConfiguration.getDefaultJournalBufferTimeoutNio()
        journalBufferSize_AIO = maxMessageSize // Required to address IllegalArgumentException (when Artemis uses Linux Async IO): Record is too large to store.
        journalBufferTimeout_AIO = journalBufferTimeout ?: ActiveMQDefaultConfiguration.getDefaultJournalBufferTimeoutAio()
        journalFileSize = maxMessageSize // The size of each journal file in bytes. Artemis default is 10MiB.
    }

    private fun queueConfigurations(): List<CoreQueueConfiguration> {
        return listOf(
                queueConfiguration(RPCApi.RPC_SERVER_QUEUE_NAME, durable = false),
                queueConfiguration(
                        name = RPCApi.RPC_CLIENT_BINDING_REMOVALS,
                        address = ArtemisMessagingComponent.NOTIFICATIONS_ADDRESS,
                        filter = RPCApi.RPC_CLIENT_BINDING_REMOVAL_FILTER_EXPRESSION,
                        durable = false
                ),
                queueConfiguration(
                        name = RPCApi.RPC_CLIENT_BINDING_ADDITIONS,
                        address = ArtemisMessagingComponent.NOTIFICATIONS_ADDRESS,
                        filter = RPCApi.RPC_CLIENT_BINDING_ADDITION_FILTER_EXPRESSION,
                        durable = false
                )
        )
    }

    private fun setDirectories(baseDirectory: Path) {
        bindingsDirectory = (baseDirectory / "bindings").toString()
        journalDirectory = (baseDirectory / "journal").toString()
        largeMessagesDirectory = (baseDirectory / "large-messages").toString()
        pagingDirectory = (baseDirectory / "paging").toString()
    }

    private fun queueConfiguration(name: String, address: String = name, filter: String? = null, durable: Boolean): CoreQueueConfiguration {
        val configuration = CoreQueueConfiguration()

        configuration.name = name
        configuration.address = address
        configuration.filterString = filter
        configuration.isDurable = durable

        return configuration
    }

    private fun restrictedRole(name: String, send: Boolean = false, consume: Boolean = false, createDurableQueue: Boolean = false,
                               deleteDurableQueue: Boolean = false, createNonDurableQueue: Boolean = false,
                               deleteNonDurableQueue: Boolean = false, manage: Boolean = false, browse: Boolean = false): Role {
        return Role(name, send, consume, createDurableQueue, deleteDurableQueue, createNonDurableQueue, deleteNonDurableQueue, manage, browse, createDurableQueue || createNonDurableQueue, deleteDurableQueue || deleteNonDurableQueue)
    }
}