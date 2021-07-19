package net.corda.serialization.internal.amqp

import net.corda.core.KeepForDJVM
import net.corda.core.serialization.SerializationContext
import net.corda.core.serialization.SerializedBytes
import net.corda.core.utilities.contextLogger
import net.corda.serialization.internal.CordaSerializationEncoding
import net.corda.serialization.internal.SectionId
import net.corda.serialization.internal.SerializationContextImpl
import net.corda.serialization.internal.byteArrayOutput
import net.corda.serialization.internal.model.TypeIdentifier
import org.apache.qpid.proton.codec.Data
import java.io.NotSerializableException
import java.io.OutputStream
import java.lang.reflect.Type
import java.lang.reflect.WildcardType
import java.nio.ByteBuffer
import java.util.*
import kotlin.collections.LinkedHashSet
import kotlin.math.min

@KeepForDJVM
data class BytesAndSchemas<T : Any>(
        val obj: SerializedBytes<T>,
        val schema: Schema,
        val transformsSchema: TransformsSchema)

/**
 * Main entry point for serializing an object to AMQP.
 *
 * @param serializerFactory This is the factory for [AMQPSerializer] instances and can be shared across multiple
 * instances and threads.
 */
@KeepForDJVM
open class SerializationOutput constructor(
        internal val serializerFactory: LocalSerializerFactory
) {
    companion object {
        private val logger = contextLogger()

        var verify = false

        private fun writeAmqpListDimensions(stream: OutputStream, listSize: Int, listcount: Int) {
            assert(listcount < 255) { "larger list count not implemented" }
            if (listSize <= 254) {
                stream.write(0xc0)
                stream.write(listSize + 1)
                stream.write(listcount)
            } else {
                // list type
                stream.write(0xd0)

                // write list length as int
                stream.write(ByteBuffer.allocate(4).putInt(listSize + 4).array())

                // write count=3 for [data, schema, transform]
                stream.write(0)
                stream.write(0)
                stream.write(0)
                stream.write(listcount)
            }
        }
    }

    private val objectHistory: MutableMap<Any, Int> = IdentityHashMap()
    private val serializerHistory: MutableSet<AMQPSerializer<*>> = LinkedHashSet()
    internal val schemaHistory: MutableSet<TypeNotation> = LinkedHashSet()

    /**
     * Serialize the given object to AMQP, wrapped in our [Envelope] wrapper which carries an AMQP 1.0 schema, and prefixed
     * with a header to indicate that this is serialized with AMQP and not Kryo, and what version of the Corda implementation
     * of AMQP serialization constructed the serialized form.
     */
    @Throws(NotSerializableException::class)
    fun <T : Any> serialize(obj: T, context: SerializationContext): SerializedBytes<T> {
        try {
            return serializeInternal(obj, context)
        } catch (amqp: AMQPNotSerializableException) {
            amqp.log("Serialize", logger)
            throw NotSerializableException(amqp.mitigation)
        } finally {
            andFinally()
        }
    }

    // NOTE: No need to handle AMQPNotSerializableExceptions here as this is an internal
    // only / testing function and it doesn't matter if they escape
    @Throws(NotSerializableException::class)
    fun <T : Any> serializeAndReturnSchema(obj: T, context: SerializationContext): BytesAndSchemas<T> {
        try {
            val blob = serializeInternal(obj, context)
            val schema = Schema(schemaHistory.toList())
            return BytesAndSchemas(blob, schema, TransformsSchema.build(schema, serializerFactory))
        } finally {
            andFinally()
        }
    }

    internal fun andFinally() {
        objectHistory.clear()
        serializerHistory.clear()
        schemaHistory.clear()
    }

    internal fun <T : Any> serializeInternal(obj: T, context: SerializationContext): SerializedBytes<T> {
        val opt = serializeWithCachedSchema(obj, context)

        if (verify) {
            // if verification is enabled, it will double check that the optimized serialization produces a valid output
            andFinally()
            val unopt = serializeUnoptimized(obj, context)
            for (x in 0 until min(opt.size, unopt.size)) {
                assert(opt.bytes[opt.offset + x] == unopt.bytes[unopt.offset + x]) {
                    "serialization mismatch at position $x out of ${opt.size}"
                }
            }
            assert(opt.size == unopt.size)
        }

        return opt;
    }

    fun <T : Any> serializeUnoptimized(obj: T, context: SerializationContext): SerializedBytes<T> {
        val data = Data.Factory.create()
        data.withDescribed(Envelope.DESCRIPTOR_OBJECT) {
            withList {
                writeObject(obj, this, context)
                val schema = Schema(schemaHistory.toList())
                var transformedSchema = TransformsSchema.build(schema, serializerFactory)
                writeSchema(schema, this)
                writeTransformSchema(transformedSchema, this)
            }
        }

        return SerializedBytes(byteArrayOutput {
            var stream: OutputStream = it
            try {
                amqpMagic.writeTo(stream)
                val encoding = context.encoding
                if (encoding != null) {
                    SectionId.ENCODING.writeTo(stream)
                    (encoding as CordaSerializationEncoding).writeTo(stream)
                    stream = encoding.wrap(stream)
                }
                SectionId.DATA_AND_STOP.writeTo(stream)
                stream.alsoAsByteBuffer(data.encodedSize().toInt(), data::encode)
            } finally {
                stream.close()
            }
        })
    }

    /**
     * Performs optimized serialization by allowing the caching and reuse of the serialized schema. The schema
     * can take up to 80% to 90% of the data and is equally expensive to serialize. As such it can impact serialization performance
     * by a factor 10. This method serializes the three building blocks (data, schema, transformation) separately, caches the schema
     * and combines them together with the necessary Corda/AMQP envelop.
     */
    private fun <T : Any> serializeWithCachedSchema(obj: T, context: SerializationContext): SerializedBytes<T> {
        val data = Data.Factory.create()
        writeObject(obj, data, context)

        val schema = Schema(schemaHistory.toList())
        var transformedSchema = TransformsSchema.build(schema, serializerFactory)

        val transform = Data.Factory.create()
        writeTransformSchema(transformedSchema, transform)

        val encodedData = data.encode()
        val encodedSchema = (context as SerializationContextImpl).serializeSchema(schema)
        val encodedTransform = transform.encode()

        return SerializedBytes(byteArrayOutput {
            var stream: OutputStream = it
            try {
                amqpMagic.writeTo(stream)
                val encoding = context.encoding
                if (encoding != null) {
                    SectionId.ENCODING.writeTo(stream)
                    (encoding as CordaSerializationEncoding).writeTo(stream)
                    stream = encoding.wrap(stream)
                }
                SectionId.DATA_AND_STOP.writeTo(stream)

                // write described element header, serialized as DescribedTypeElement
                stream.write(0); // see DescribedTypeElement
                stream.write(0x80);  // see UnsignedLongElement
                stream.write(ByteBuffer.allocate(8).putLong(Envelope.DESCRIPTOR_OBJECT.code!!.toLong()).array())

                // write list size
                val dataSize = encodedData.length + encodedSchema.length + encodedTransform.length
                writeAmqpListDimensions(stream, dataSize, 3)

                // write list entries
                stream.write(encodedData.array, encodedData.arrayOffset, encodedData.length)
                stream.write(encodedSchema.array, encodedSchema.arrayOffset, encodedSchema.length)
                stream.write(encodedTransform.array, encodedTransform.arrayOffset, encodedTransform.length)
            } finally {
                stream.close()
            }
        })
    }

    internal fun writeObject(obj: Any, data: Data, context: SerializationContext) {
        writeObject(obj, data, obj.javaClass, context)
    }

    open fun writeSchema(schema: Schema, data: Data) {
        data.putObject(schema)
    }

    open fun writeTransformSchema(transformsSchema: TransformsSchema, data: Data) {
        data.putObject(transformsSchema)
    }

    internal fun writeObjectOrNull(obj: Any?, data: Data, type: Type, context: SerializationContext, debugIndent: Int) {
        if (obj == null) {
            data.putNull()
        } else {
            writeObject(obj, data, if (type == TypeIdentifier.UnknownType.getLocalType()) obj.javaClass else type, context, debugIndent)
        }
    }

    internal fun writeObject(obj: Any, data: Data, type: Type, context: SerializationContext, debugIndent: Int = 0) {
        val serializer = serializerFactory.get(obj.javaClass, type)
        if (serializer !in serializerHistory) {
            serializerHistory.add(serializer)
            serializer.writeClassInfo(this)
        }

        val retrievedRefCount = objectHistory[obj]
        if (retrievedRefCount == null) {
            serializer.writeObject(obj, data, type, this, context, debugIndent)
            // Important to do it after serialization such that dependent object will have preceding reference numbers
            // assigned to them first as they will be first read from the stream on receiving end.
            // Skip for primitive types as they are too small and overhead of referencing them will be much higher than their content
            if (serializerFactory.isSuitableForObjectReference(obj.javaClass)) {
                objectHistory[obj] = objectHistory.size
            }
        } else {
            data.writeReferencedObject(ReferencedObject(retrievedRefCount))
        }
    }

    internal open fun writeTypeNotations(vararg typeNotation: TypeNotation): Boolean {
        return schemaHistory.addAll(typeNotation)
    }

    internal open fun requireSerializer(type: Type) {
        if (type != Object::class.java && type.typeName != "?") {
            val resolvedType = when (type) {
                is WildcardType ->
                    if (type.upperBounds.size == 1) type.upperBounds[0]
                    else throw NotSerializableException("Cannot obtain upper bound for type $type")
                else -> type
            }

            val serializer = serializerFactory.get(resolvedType)
            if (serializer !in serializerHistory) {
                serializerHistory.add(serializer)
                serializer.writeClassInfo(this)
            }
        }
    }
}

