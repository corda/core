package net.corda.serialization.internal.amqp

import net.corda.core.KeepForDJVM
import net.corda.core.internal.uncheckedCast
import net.corda.serialization.internal.CordaSerializationMagic
import net.corda.serialization.internal.amqp.AMQPTypeIdentifiers.isPrimitive
import net.corda.serialization.internal.model.TypeIdentifier
import net.corda.serialization.internal.model.TypeIdentifier.Companion.forGenericType
import net.corda.serialization.internal.model.TypeIdentifier.TopType
import org.apache.qpid.proton.amqp.Binary
import org.apache.qpid.proton.amqp.DescribedType
import org.apache.qpid.proton.amqp.Symbol
import org.apache.qpid.proton.amqp.UnsignedInteger
import org.apache.qpid.proton.amqp.UnsignedLong
import org.apache.qpid.proton.codec.DescribedTypeConstructor
import java.io.NotSerializableException
import java.lang.reflect.Type
import java.security.MessageDigest

const val DESCRIPTOR_DOMAIN: String = "net.corda"
val amqpMagic = CordaSerializationMagic("corda".toByteArray() + byteArrayOf(1, 0))

fun typeDescriptorFor(typeId: TypeIdentifier): Symbol = Symbol.valueOf("$DESCRIPTOR_DOMAIN:${AMQPTypeIdentifiers.nameForType(typeId)}")
fun typeDescriptorFor(type: Type): Symbol = typeDescriptorFor(forGenericType(type))

/**
 * Repackages a naked, non-primitive [obj] as a [DescribedType]. If [obj] is primitive, [Binary] or already
 * an instance of [DescribedType]] then it is returned unchanged. This allows Corda to search for a serializer
 * capable of handling instances of [type].
 */
fun redescribe(obj: Any?, type: Type): Any? {
    return if (obj == null || obj is DescribedType || obj is Binary || forGenericType(type).run { isPrimitive(this) || this == TopType }) {
        obj
    } else {
        /**
         * This must be a primitive [obj] that has a non-primitive [type].
         * Rewrap it with the required descriptor for further deserialization.
         */
        RedescribedType(typeDescriptorFor(type), obj)
    }
}

private class RedescribedType(
        private val descriptor: Symbol,
        private val described: Any?
) : DescribedType {
    override fun getDescriptor(): Symbol = descriptor
    override fun getDescribed(): Any? = described
}

/**
 * This and the classes below are OO representations of the AMQP XML schema described in the specification. Their
 * [toString] representations generate the associated XML form.
 */
@KeepForDJVM
data class Schema(val types: List<TypeNotation>) : DescribedType {
    companion object : DescribedTypeConstructor<Schema> {
        val DESCRIPTOR = AMQPDescriptorRegistry.SCHEMA.amqpDescriptor

        fun get(obj: Any): Schema {
            val describedType = obj as DescribedType
            if (describedType.descriptor != DESCRIPTOR) {
                throw NotSerializableException("Unexpected descriptor ${describedType.descriptor}.")
            }
            val list = describedType.described as List<*>
            return newInstance(listOf((list[0] as List<*>).map { TypeNotation.get(it!!) }))
        }

        override fun getTypeClass(): Class<*> = Schema::class.java

        override fun newInstance(described: Any?): Schema {
            val list = described as? List<*> ?: throw IllegalStateException("Was expecting a list")
            return Schema(uncheckedCast(list[0]))
        }
    }

    override fun getDescriptor(): Any = DESCRIPTOR

    override fun getDescribed(): Any = listOf(types)

    override fun toString(): String = types.joinToString("\n")
}

@KeepForDJVM
data class Descriptor(val name: Symbol?, val code: UnsignedLong? = null) : DescribedType {
    constructor(name: String?) : this(Symbol.valueOf(name))

    companion object : DescribedTypeConstructor<Descriptor> {
        val DESCRIPTOR = AMQPDescriptorRegistry.OBJECT_DESCRIPTOR.amqpDescriptor

        fun get(obj: Any): Descriptor {
            val describedType = obj as DescribedType
            if (describedType.descriptor != DESCRIPTOR) {
                throw NotSerializableException("Unexpected descriptor ${describedType.descriptor}.")
            }
            return newInstance(describedType.described)
        }

        override fun getTypeClass(): Class<*> = Descriptor::class.java

        override fun newInstance(described: Any?): Descriptor {
            val list = described as? List<*> ?: throw IllegalStateException("Was expecting a list")
            return Descriptor(list[0] as? Symbol, list[1] as? UnsignedLong)
        }
    }

    override fun getDescriptor(): Any = DESCRIPTOR

    override fun getDescribed(): Any = listOf(name, code)

    override fun toString(): String {
        val sb = StringBuilder("<descriptor")
        if (name != null) {
            sb.append(" name=\"$name\"")
        }
        if (code != null) {
            val code = String.format("0x%08x:0x%08x", code.toLong().shr(32), code.toLong().and(0xffff))
            sb.append(" code=\"$code\"")
        }
        sb.append("/>")
        return sb.toString()
    }
}

@KeepForDJVM
data class Field(
        val name: String,
        val type: String,
        val requires: List<String>,
        val default: String?,
        val label: String?,
        val mandatory: Boolean,
        val multiple: Boolean) : DescribedType {
    companion object : DescribedTypeConstructor<Field> {
        val DESCRIPTOR = AMQPDescriptorRegistry.FIELD.amqpDescriptor

        fun get(obj: Any): Field {
            val describedType = obj as DescribedType
            if (describedType.descriptor != DESCRIPTOR) {
                throw NotSerializableException("Unexpected descriptor ${describedType.descriptor}.")
            }
            return newInstance(describedType.described)
        }

        override fun getTypeClass(): Class<*> = Field::class.java

        override fun newInstance(described: Any?): Field {
            val list = described as? List<*> ?: throw IllegalStateException("Was expecting a list")
            return Field(list[0] as String, list[1] as String, uncheckedCast(list[2]), list[3] as? String, list[4] as? String, list[5] as Boolean, list[6] as Boolean)
        }
    }

    override fun getDescriptor(): Any = DESCRIPTOR

    override fun getDescribed(): Any = listOf(name, type, requires, default, label, mandatory, multiple)

    override fun toString(): String {
        val sb = StringBuilder("<field name=\"$name\" type=\"$type\" mandatory=\"$mandatory\" multiple=\"$multiple\"")
        if (requires.isNotEmpty()) {
            sb.append(" requires=\"")
            sb.append(requires.joinToString(","))
            sb.append("\"")
        }
        if (default != null) {
            sb.append(" default=\"$default\"")
        }
        if (!label.isNullOrBlank()) {
            sb.append(" label=\"$label\"")
        }
        sb.append("/>")
        return sb.toString()
    }
}

sealed class TypeNotation : DescribedType {
    companion object {
        fun get(obj: Any): TypeNotation {
            val describedType = obj as DescribedType
            return when (describedType.descriptor) {
                CompositeType.DESCRIPTOR -> CompositeType.get(describedType)
                RestrictedType.DESCRIPTOR -> RestrictedType.get(describedType)
                else -> throw NotSerializableException("Unexpected descriptor ${describedType.descriptor}.")
            }
        }
    }

    abstract val name: String
    abstract val label: String?
    abstract val provides: List<String>
    abstract val descriptor: Descriptor

    val id: ByteArray by lazy {
        // assumed that toString holds a complete representation of TypeNotation
        hashString(toString())
    }
}

private fun hashString(input: String): ByteArray {
    val md = MessageDigest.getInstance("SHA-256")
    return md.digest(input.toByteArray())
}

@KeepForDJVM
data class CompositeType(
        override val name: String,
        override val label: String?,
        override val provides: List<String>,
        override val descriptor: Descriptor,
        val fields: List<Field>
) : TypeNotation() {
    companion object : DescribedTypeConstructor<CompositeType> {
        val DESCRIPTOR = AMQPDescriptorRegistry.COMPOSITE_TYPE.amqpDescriptor

        fun get(describedType: DescribedType): CompositeType {
            if (describedType.descriptor != DESCRIPTOR) {
                throw NotSerializableException("Unexpected descriptor ${describedType.descriptor}.")
            }
            val list = describedType.described as List<*>
            return newInstance(listOf(list[0], list[1], list[2], Descriptor.get(list[3]!!), (list[4] as List<*>).map { Field.get(it!!) }))
        }

        override fun getTypeClass(): Class<*> = CompositeType::class.java

        override fun newInstance(described: Any?): CompositeType {
            val list = described as? List<*> ?: throw IllegalStateException("Was expecting a list")
            return CompositeType(list[0] as String, list[1] as? String, uncheckedCast(list[2]), list[3] as Descriptor, uncheckedCast(list[4]))
        }
    }

    override fun getDescriptor(): Any = DESCRIPTOR

    override fun getDescribed(): Any = listOf(name, label, provides, descriptor, fields)

    override fun toString(): String {
        val sb = StringBuilder("<type class=\"composite\" name=\"$name\"")
        if (!label.isNullOrBlank()) {
            sb.append(" label=\"$label\"")
        }
        if (provides.isNotEmpty()) {
            sb.append(" provides=\"")
            sb.append(provides.joinToString(","))
            sb.append("\"")
        }
        sb.append(">\n")
        sb.append("  $descriptor\n")
        for (field in fields) {
            sb.append("  $field\n")
        }
        sb.append("</type>")
        return sb.toString()
    }
}

@KeepForDJVM
data class RestrictedType(override val name: String,
                          override val label: String?,
                          override val provides: List<String>,
                          val source: String,
                          override val descriptor: Descriptor,
                          val choices: List<Choice>) : TypeNotation() {
    companion object : DescribedTypeConstructor<RestrictedType> {
        val DESCRIPTOR = AMQPDescriptorRegistry.RESTRICTED_TYPE.amqpDescriptor

        fun get(describedType: DescribedType): RestrictedType {
            if (describedType.descriptor != DESCRIPTOR) {
                throw AMQPNoTypeNotSerializableException("Unexpected descriptor ${describedType.descriptor}.")
            }
            val list = describedType.described as List<*>
            return newInstance(listOf(list[0], list[1], list[2], list[3], Descriptor.get(list[4]!!), (list[5] as List<*>).map { Choice.get(it!!) }))
        }

        override fun getTypeClass(): Class<*> = RestrictedType::class.java

        override fun newInstance(described: Any?): RestrictedType {
            val list = described as? List<*> ?: throw IllegalStateException("Was expecting a list")
            return RestrictedType(list[0] as String, list[1] as? String, uncheckedCast(list[2]), list[3] as String, list[4] as Descriptor, uncheckedCast(list[5]))
        }
    }

    override fun getDescriptor(): Any = DESCRIPTOR

    override fun getDescribed(): Any = listOf(name, label, provides, source, descriptor, choices)

    override fun toString(): String {
        val sb = StringBuilder("<type class=\"restricted\" name=\"$name\"")
        if (!label.isNullOrBlank()) {
            sb.append(" label=\"$label\"")
        }
        sb.append(" source=\"$source\"")
        if (provides.isNotEmpty()) {
            sb.append(" provides=\"")
            sb.append(provides.joinToString(","))
            sb.append("\"")
        }
        sb.append(">\n")
        sb.append("  $descriptor\n")
        choices.forEach {
            sb.append("  $it\n")
        }
        sb.append("</type>")
        return sb.toString()
    }
}

@KeepForDJVM
data class Choice(val name: String, val value: String) : DescribedType {
    companion object : DescribedTypeConstructor<Choice> {
        val DESCRIPTOR = AMQPDescriptorRegistry.CHOICE.amqpDescriptor

        fun get(obj: Any): Choice {
            val describedType = obj as DescribedType
            if (describedType.descriptor != DESCRIPTOR) {
                throw NotSerializableException("Unexpected descriptor ${describedType.descriptor}.")
            }
            return newInstance(describedType.described)
        }

        override fun getTypeClass(): Class<*> = Choice::class.java

        override fun newInstance(described: Any?): Choice {
            val list = described as? List<*> ?: throw IllegalStateException("Was expecting a list")
            return Choice(list[0] as String, list[1] as String)
        }
    }

    override fun getDescriptor(): Any = DESCRIPTOR

    override fun getDescribed(): Any = listOf(name, value)

    override fun toString(): String {
        return "<choice name=\"$name\" value=\"$value\"/>"
    }
}

@KeepForDJVM
data class ReferencedObject(private val refCounter: Int) : DescribedType {
    companion object : DescribedTypeConstructor<ReferencedObject> {
        val DESCRIPTOR = AMQPDescriptorRegistry.REFERENCED_OBJECT.amqpDescriptor

        fun get(obj: Any): ReferencedObject {
            val describedType = obj as DescribedType
            if (describedType.descriptor != DESCRIPTOR) {
                throw AMQPNoTypeNotSerializableException("Unexpected descriptor ${describedType.descriptor}.")
            }
            return newInstance(describedType.described)
        }

        override fun getTypeClass(): Class<*> = ReferencedObject::class.java

        override fun newInstance(described: Any?): ReferencedObject {
            val unInt = described as? UnsignedInteger ?: throw IllegalStateException("Was expecting an UnsignedInteger")
            return ReferencedObject(unInt.toInt())
        }
    }

    override fun getDescriptor(): Any = DESCRIPTOR

    override fun getDescribed(): UnsignedInteger = UnsignedInteger(refCounter)

    override fun toString(): String = "<refObject refCounter=$refCounter/>"
}


