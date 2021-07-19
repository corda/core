package net.corda.serialization.internal.amqp.testutils

import net.corda.core.internal.*
import net.corda.core.serialization.SerializationContext
import net.corda.core.serialization.SerializationEncoding
import net.corda.core.serialization.SerializedBytes
import net.corda.core.utilities.OpaqueBytes
import net.corda.serialization.internal.AllWhitelist
import net.corda.serialization.internal.EmptyWhitelist
import net.corda.serialization.internal.amqp.*
import net.corda.serialization.internal.carpenter.ClassCarpenterImpl
import org.apache.qpid.proton.codec.Data
import org.junit.Test
import java.io.File.separatorChar
import java.io.NotSerializableException
import java.nio.file.Path
import java.nio.file.StandardCopyOption.REPLACE_EXISTING

/**
 * For tests that want to see inside the serializer registry
 */
class TestDescriptorBasedSerializerRegistry : DescriptorBasedSerializerRegistry {
    val contents = mutableMapOf<String, AMQPSerializer<Any>>()

    override fun get(descriptor: String): AMQPSerializer<Any>? = contents[descriptor]

    override fun set(descriptor: String, serializer: AMQPSerializer<Any>) {
        contents.putIfAbsent(descriptor, serializer)
    }

    override fun getOrBuild(descriptor: String, builder: () -> AMQPSerializer<Any>): AMQPSerializer<Any> =
            get(descriptor) ?: builder().also { set(descriptor, it) }
}

@JvmOverloads
fun testDefaultFactory(descriptorBasedSerializerRegistry: DescriptorBasedSerializerRegistry =
                               DefaultDescriptorBasedSerializerRegistry()) =
        SerializerFactoryBuilder.build(
            AllWhitelist,
            ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader()),
            descriptorBasedSerializerRegistry = descriptorBasedSerializerRegistry)

@JvmOverloads
fun testDefaultFactoryNoEvolution(descriptorBasedSerializerRegistry: DescriptorBasedSerializerRegistry =
                                          DefaultDescriptorBasedSerializerRegistry()): SerializerFactory =
    SerializerFactoryBuilder.build(
            AllWhitelist,
            ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader()),
            descriptorBasedSerializerRegistry = descriptorBasedSerializerRegistry,
            allowEvolution = false)

@JvmOverloads
fun testDefaultFactoryWithWhitelist(descriptorBasedSerializerRegistry: DescriptorBasedSerializerRegistry =
                                            DefaultDescriptorBasedSerializerRegistry()) =
        SerializerFactoryBuilder.build(EmptyWhitelist,
        ClassCarpenterImpl(EmptyWhitelist, ClassLoader.getSystemClassLoader()),
        descriptorBasedSerializerRegistry = descriptorBasedSerializerRegistry)

class TestSerializationOutput(
        private val verbose: Boolean,
        serializerFactory: SerializerFactory = testDefaultFactory())
    : SerializationOutput(serializerFactory) {

    override fun writeSchema(schema: Schema, data: Data) {
        if (verbose) println(schema)
        super.writeSchema(schema, data)
    }

    override fun writeTransformSchema(transformsSchema: TransformsSchema, data: Data) {
        if(verbose) {
            println ("Writing Transform Schema")
            println (transformsSchema)
        }
        super.writeTransformSchema(transformsSchema, data)
    }

    @Throws(NotSerializableException::class)
    fun <T : Any> serialize(obj: T): SerializedBytes<T> {
        try {
            return serializeInternal(obj, testSerializationContext)
        } finally {
            andFinally()
        }
    }
}

fun testName(): String {
    val classLoader = Thread.currentThread().contextClassLoader
    return Thread.currentThread().stackTrace.first {
        try {
            classLoader.loadClass(it.className).getMethod(it.methodName).isAnnotationPresent(Test::class.java)
        } catch (e: Exception) {
            false
        }
    }.methodName
}

fun Any.testResourceName(): String = "${javaClass.simpleName}.${testName()}"

internal object ProjectStructure {
    val projectRootDir: Path = run {
        var dir = javaClass.getResource("/").toPath()
        while (!(dir / ".git").isDirectory()) {
            dir = dir.parent
        }
        dir
    }
}

fun Any.writeTestResource(bytes: OpaqueBytes) {
    val dir = ProjectStructure.projectRootDir / "serialization" / "src" / "test" / "resources" / javaClass.packageName_.replace('.', separatorChar)
    bytes.open().copyTo(dir / testResourceName(), REPLACE_EXISTING)
}

fun Any.readTestResource(): ByteArray = javaClass.getResourceAsStream(testResourceName()).readBytes()

@Throws(NotSerializableException::class)
inline fun <reified T : Any> DeserializationInput.deserializeAndReturnEnvelope(
        bytes: SerializedBytes<T>,
        context: SerializationContext? = null
) : ObjectAndEnvelope<T> {
    return deserializeAndReturnEnvelope(bytes, T::class.java,
            context ?: testSerializationContext)
}

@Throws(NotSerializableException::class)
inline fun <reified T : Any> DeserializationInput.deserialize(
        bytes: SerializedBytes<T>,
        context: SerializationContext? = null
)  : T = deserialize(bytes, T::class.java, context ?: testSerializationContext)


@Throws(NotSerializableException::class)
fun <T : Any> SerializationOutput.serializeAndReturnSchema(
        obj: T, context: SerializationContext? = null
): BytesAndSchemas<T> = serializeAndReturnSchema(obj, context ?: testSerializationContext)


@Throws(NotSerializableException::class)
fun <T : Any> SerializationOutput.serialize(obj: T, encoding: SerializationEncoding? = null): SerializedBytes<T> {
    try {
        return serializeInternal(obj, testSerializationContext.withEncoding(encoding))
    } finally {
        andFinally()
    }
}
