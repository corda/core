package net.corda.node.migration

import com.codahale.metrics.MetricRegistry
import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.CacheLoader
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.node.services.config.NodeConfiguration
import net.corda.node.utilities.BindableNamedCacheFactory
import net.corda.nodeapi.internal.persistence.DatabaseConfig
import java.lang.IllegalArgumentException

// A cache factory suitable for use while migrating the database to a new version. This version does not need node configuration in order to
// construct a cache.
class MigrationNamedCacheFactory(private val metricRegistry: MetricRegistry?,
                                 private val nodeConfiguration: NodeConfiguration?) : BindableNamedCacheFactory, SingletonSerializeAsToken() {

    override fun bindWithMetrics(metricRegistry: MetricRegistry) = MigrationNamedCacheFactory(metricRegistry, this.nodeConfiguration)
    override fun bindWithConfig(nodeConfiguration: NodeConfiguration) = MigrationNamedCacheFactory(this.metricRegistry, nodeConfiguration)

    private fun determineSchemaCacheSize(): Long {
        var schemaCacheSize: Long = DatabaseConfig.Defaults.mappedSchemaCacheSize
        if (nodeConfiguration?.lowMemoryMode != null && nodeConfiguration.lowMemoryMode) {
            schemaCacheSize = DatabaseConfig.Defaults.mappedSchemaCacheSizeLowMemoryMode.coerceAtMost(nodeConfiguration.database.mappedSchemaCacheSize)
        }
        return schemaCacheSize
    }

    private fun <K, V> configuredForNamed(caffeine: Caffeine<K, V>, name: String): Caffeine<K, V> {
        return when(name) {
            "HibernateConfiguration_sessionFactories" -> caffeine.maximumSize(determineSchemaCacheSize())
            "DBTransactionStorage_transactions" -> caffeine.maximumWeight(
                    nodeConfiguration?.transactionCacheSizeBytes ?: NodeConfiguration.defaultTransactionCacheSize
            )
            "PersistentIdentityService_keyToPartyAndCert" -> caffeine.maximumSize(defaultCacheSize)
            "PersistentIdentityService_nameToKey" -> caffeine.maximumSize(defaultCacheSize)
            "PersistentIdentityService_keyToName" -> caffeine.maximumSize(defaultCacheSize)
            "PersistentIdentityService_hashToKey" -> caffeine.maximumSize(defaultCacheSize)
            "BasicHSMKeyManagementService_keys" -> caffeine.maximumSize(defaultCacheSize)
            "NodeAttachmentService_attachmentContent" -> caffeine.maximumWeight(defaultCacheSize)
            "NodeAttachmentService_attachmentPresence" -> caffeine.maximumSize(defaultCacheSize)
            "NodeAttachmentService_contractAttachmentVersions" -> caffeine.maximumSize(defaultCacheSize)
            "NodeParametersStorage_networkParametersByHash" -> caffeine.maximumSize(defaultCacheSize)
            "NodeAttachmentTrustCalculator_trustedKeysCache" -> caffeine.maximumSize(defaultCacheSize)
            else -> throw IllegalArgumentException("Unexpected cache name $name.")
        }
    }

    override fun <K, V> buildNamed(caffeine: Caffeine<in K, in V>, name: String): Cache<K, V> {
        return configuredForNamed(caffeine, name).build()
    }

    override fun <K, V> buildNamed(caffeine: Caffeine<in K, in V>, name: String, loader: CacheLoader<K, V>): LoadingCache<K, V> {
        return configuredForNamed(caffeine, name).build(loader)
    }

    private val defaultCacheSize = 1024L
}