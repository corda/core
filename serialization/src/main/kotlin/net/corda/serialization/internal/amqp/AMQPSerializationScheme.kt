@file:JvmName("AMQPSerializationScheme")

package net.corda.serialization.internal.amqp

import net.corda.core.DeleteForDJVM
import net.corda.core.KeepForDJVM
import net.corda.core.StubOutForDJVM
import net.corda.core.cordapp.Cordapp
import net.corda.core.internal.VisibleForTesting
import net.corda.core.internal.toSynchronised
import net.corda.core.internal.uncheckedCast
import net.corda.core.serialization.*
import net.corda.core.utilities.ByteSequence
import net.corda.serialization.internal.CordaSerializationMagic
import net.corda.serialization.internal.DefaultWhitelist
import net.corda.serialization.internal.MutableClassWhitelist
import net.corda.serialization.internal.SerializationScheme
import java.util.*

val AMQP_ENABLED get() = SerializationDefaults.P2P_CONTEXT.preferredSerializationVersion == amqpMagic

data class SerializationFactoryCacheKey(val classWhitelist: ClassWhitelist,
                                        val deserializationClassLoader: ClassLoader,
                                        val preventDataLoss: Boolean,
                                        val customSerializers: Set<SerializationCustomSerializer<*, *>>)

fun SerializerFactory.addToWhitelist(types: Collection<Class<*>>) {
    require(types.toSet().size == types.size) {
        val duplicates = types.toMutableList()
        types.toSet().forEach { duplicates -= it }
        "Cannot add duplicate classes to the whitelist ($duplicates)."
    }
    for (type in types) {
        (this.whitelist as? MutableClassWhitelist)?.add(type)
    }
}

// Allow us to create a SerializerFactory with a different ClassCarpenter implementation.
interface SerializerFactoryFactory {
    fun make(context: SerializationContext): SerializerFactory
}

@KeepForDJVM
abstract class AbstractAMQPSerializationScheme(
        private val cordappCustomSerializers: Set<SerializationCustomSerializer<*, *>>,
        private val cordappSerializationWhitelists: Set<SerializationWhitelist>,
        maybeNotConcurrentSerializerFactoriesForContexts: MutableMap<SerializationFactoryCacheKey, SerializerFactory>,
        val sff: SerializerFactoryFactory = createSerializerFactoryFactory()
) : SerializationScheme {
    @DeleteForDJVM
    constructor(cordapps: List<Cordapp>) : this(
            cordapps.customSerializers,
            cordapps.serializationWhitelists,
            AccessOrderLinkedHashMap<SerializationFactoryCacheKey, SerializerFactory>(128).toSynchronised()
    )

    @VisibleForTesting
    fun getRegisteredCustomSerializers() = cordappCustomSerializers

    // This is a bit gross but a broader check for ConcurrentMap is not allowed inside DJVM.
    private val serializerFactoriesForContexts: MutableMap<SerializationFactoryCacheKey, SerializerFactory> =
            if (maybeNotConcurrentSerializerFactoriesForContexts is
                            AccessOrderLinkedHashMap<SerializationFactoryCacheKey, SerializerFactory>) {
                Collections.synchronizedMap(maybeNotConcurrentSerializerFactoriesForContexts)
            } else {
                maybeNotConcurrentSerializerFactoriesForContexts
            }

    companion object {
        private val serializationWhitelists: List<SerializationWhitelist> by lazy { listOf(DefaultWhitelist) }

        @DeleteForDJVM
        val List<Cordapp>.customSerializers
            get() = flatMapTo(LinkedHashSet(), Cordapp::serializationCustomSerializers)

        @DeleteForDJVM
        val List<Cordapp>.serializationWhitelists
            get() = flatMapTo(LinkedHashSet(), Cordapp::serializationWhitelists)
    }

    private fun registerCustomSerializers(context: SerializationContext, factory: SerializerFactory) {
        factory.register(publicKeySerializer)
        registerCustomSerializers(factory)

        // This step is registering custom serializers, which have been added after node initialisation (i.e. via attachments during
        // transaction verification).
        // Note: the order between the registration of customSerializers and cordappCustomSerializers must be preserved as-is. The reason
        // is the following:
        // Currently, the serialization infrastructure does not support multiple versions of a class (the first one that is
        // registered dominates). As a result, when inside a context with attachments class loader, we prioritize serializers loaded
        // on-demand from attachments to serializers that had been loaded during node initialisation, by scanning the cordapps folder.
        context.customSerializers.forEach { customSerializer ->
            factory.registerExternal(CorDappCustomSerializer(customSerializer, factory))
        }
        cordappCustomSerializers.forEach { customSerializer ->
            // We won't be able to use this custom serializer unless it also belongs to
            // the deserialization classloader.
            if (customSerializer::class.java.classLoader == context.deserializationClassLoader) {
                factory.registerExternal(CorDappCustomSerializer(customSerializer, factory))
            }
        }

        context.properties[ContextPropertyKeys.SERIALIZERS]?.apply {
            uncheckedCast<Any, List<CustomSerializer<out Any>>>(this).forEach {
                factory.register(it)
            }
        }
    }

    private fun registerCustomWhitelists(factory: SerializerFactory) {
        serializationWhitelists.forEach {
            factory.addToWhitelist(it.whitelist)
        }
        cordappSerializationWhitelists.forEach {
            factory.addToWhitelist(it.whitelist)
        }
    }

    protected abstract fun rpcClientSerializerFactory(context: SerializationContext): SerializerFactory
    protected abstract fun rpcServerSerializerFactory(context: SerializationContext): SerializerFactory

    // Not used as a simple direct import to facilitate testing
    open val publicKeySerializer: CustomSerializer<*> = net.corda.serialization.internal.amqp.custom.PublicKeySerializer

    fun getSerializerFactory(context: SerializationContext): SerializerFactory {
        val key = SerializationFactoryCacheKey(context.whitelist, context.deserializationClassLoader, context.preventDataLoss, context.customSerializers)
        // ConcurrentHashMap.get() is lock free, but computeIfAbsent is not, even if the key is in the map already.
        return serializerFactoriesForContexts[key] ?: serializerFactoriesForContexts.computeIfAbsent(key) {
            when (context.useCase) {
                SerializationContext.UseCase.RPCClient ->
                    rpcClientSerializerFactory(context)
                SerializationContext.UseCase.RPCServer ->
                    rpcServerSerializerFactory(context)
                else -> sff.make(context)
            }.also {
                registerCustomSerializers(context, it)
                registerCustomWhitelists(it)
            }
        }
    }

    override fun <T : Any> deserialize(byteSequence: ByteSequence, clazz: Class<T>, context: SerializationContext): T {
        // This is a hack introduced in version 3 to fix a spring boot issue - CORDA-1747.
        // It breaks the shell because it overwrites the CordappClassloader with the system classloader that doesn't know about any CorDapps.
        // In case a spring boot serialization issue with generics is found, a better solution needs to be found to address it.
//        var contextToUse = context
//        if (context.useCase == SerializationContext.UseCase.RPCClient) {
//            contextToUse = context.withClassLoader(getContextClassLoader())
//        }
        val serializerFactory = getSerializerFactory(context)
        return DeserializationInput(serializerFactory).deserialize(byteSequence, clazz, context)
    }

    override fun <T : Any> serialize(obj: T, context: SerializationContext): SerializedBytes<T> {
        // See the above comment.
//        var contextToUse = context
//        if (context.useCase == SerializationContext.UseCase.RPCClient) {
//            contextToUse = context.withClassLoader(getContextClassLoader())
//        }
        val serializerFactory = getSerializerFactory(context)
        return SerializationOutput(serializerFactory).serialize(obj, context)
    }

    protected fun canDeserializeVersion(magic: CordaSerializationMagic) = magic == amqpMagic
}

fun registerCustomSerializers(factory: SerializerFactory) {
    with(factory) {
        register(net.corda.serialization.internal.amqp.custom.ThrowableSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.BigDecimalSerializer)
        register(net.corda.serialization.internal.amqp.custom.BigIntegerSerializer)
        register(net.corda.serialization.internal.amqp.custom.CurrencySerializer)
        register(net.corda.serialization.internal.amqp.custom.OpaqueBytesSubSequenceSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.InstantSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.DurationSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.LocalDateSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.LocalDateTimeSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.LocalTimeSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.ZonedDateTimeSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.ZoneIdSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.OffsetTimeSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.OffsetDateTimeSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.OptionalSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.YearSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.YearMonthSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.MonthDaySerializer(this))
        register(net.corda.serialization.internal.amqp.custom.PeriodSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.ClassSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.X509CertificateSerializer)
        register(net.corda.serialization.internal.amqp.custom.X509CRLSerializer)
        register(net.corda.serialization.internal.amqp.custom.CertPathSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.StringBufferSerializer)
        register(net.corda.serialization.internal.amqp.custom.InputStreamSerializer)
        register(net.corda.serialization.internal.amqp.custom.BitSetSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.EnumSetSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.ContractAttachmentSerializer(this))
        register(net.corda.serialization.internal.amqp.custom.X500PrincipalSerializer(this))
    }
    registerNonDeterministicSerializers(factory)
}

/*
 * Register the serializers which will be excluded from the DJVM.
 */
@StubOutForDJVM
private fun registerNonDeterministicSerializers(factory: SerializerFactory) {
    with(factory) {
        register(net.corda.serialization.internal.amqp.custom.PrivateKeySerializer)
        register(net.corda.serialization.internal.amqp.custom.SimpleStringSerializer)
    }
}

