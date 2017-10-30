package net.corda.nodeapi.internal.serialization.amqp

import com.google.common.primitives.Primitives
import com.google.common.reflect.TypeResolver
import net.corda.core.internal.uncheckedCast
import net.corda.core.serialization.ClassWhitelist
import net.corda.nodeapi.internal.serialization.carpenter.*
import org.apache.qpid.proton.amqp.*
import java.io.NotSerializableException
import java.lang.reflect.*
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import javax.annotation.concurrent.ThreadSafe

data class FactorySchemaAndDescriptor(val schema: Schema, val typeDescriptor: Any)

/**
 * Factory of serializers designed to be shared across threads and invocations.
 */
// TODO: support for intern-ing of deserialized objects for some core types (e.g. PublicKey) for memory efficiency
// TODO: maybe support for caching of serialized form of some core types for performance
// TODO: profile for performance in general
// TODO: use guava caches etc so not unbounded
// TODO: allow definition of well known types that are left out of the schema.
// TODO: migrate some core types to unsigned integer descriptor
// TODO: document and alert to the fact that classes cannot default superclass/interface properties otherwise they are "erased" due to matching with constructor.
// TODO: type name prefixes for interfaces and abstract classes?  Or use label?
// TODO: generic types should define restricted type alias with source of the wildcarded version, I think, if we're to generate classes from schema
// TODO: need to rethink matching of constructor to properties in relation to implementing interfaces and needing those properties etc.
// TODO: need to support super classes as well as interfaces with our current code base... what's involved?  If we continue to ban, what is the impact?
@ThreadSafe
open class SerializerFactory(val whitelist: ClassWhitelist, cl: ClassLoader) {
    private val serializersByType = ConcurrentHashMap<Type, AMQPSerializer<Any>>()
    private val serializersByDescriptor = ConcurrentHashMap<Any, AMQPSerializer<Any>>()
    private val customSerializers = CopyOnWriteArrayList<CustomSerializer<out Any>>()
    val transformsCache = ConcurrentHashMap<String, EnumMap<TransformTypes, MutableList<Transform>>>()

    open val classCarpenter = ClassCarpenter(cl, whitelist)
    val classloader: ClassLoader
        get() = classCarpenter.classloader

    private fun getEvolutionSerializer(typeNotation: TypeNotation, newSerializer: AMQPSerializer<Any>): AMQPSerializer<Any> {
        return serializersByDescriptor.computeIfAbsent(typeNotation.descriptor.name!!) {
            when (typeNotation) {
                is CompositeType -> EvolutionSerializer.make(typeNotation, newSerializer as ObjectSerializer, this)
                is RestrictedType -> throw NotSerializableException("Enum evolution is not currently supported")
            }
        }
    }

    /**
     * Look up, and manufacture if necessary, a serializer for the given type.
     *
     * @param actualClass Will be null if there isn't an actual object instance available (e.g. for
     * restricted type processing).
     */
    @Throws(NotSerializableException::class)
    fun get(actualClass: Class<*>?, declaredType: Type): AMQPSerializer<Any> {
        val declaredClass = declaredType.asClass() ?: throw NotSerializableException(
                "Declared types of $declaredType are not supported.")

        val actualType: Type = inferTypeVariables(actualClass, declaredClass, declaredType) ?: declaredType

        val serializer = when {
        // Declared class may not be set to Collection, but actual class could be a collection.
        // In this case use of CollectionSerializer is perfectly appropriate.
            (Collection::class.java.isAssignableFrom(declaredClass) ||
                    (actualClass != null && Collection::class.java.isAssignableFrom(actualClass))) &&
                    !EnumSet::class.java.isAssignableFrom(actualClass ?: declaredClass) -> {
                val declaredTypeAmended = CollectionSerializer.deriveParameterizedType(declaredType, declaredClass, actualClass)
                serializersByType.computeIfAbsent(declaredTypeAmended) {
                    CollectionSerializer(declaredTypeAmended, this)
                }
            }
        // Declared class may not be set to Map, but actual class could be a map.
        // In this case use of MapSerializer is perfectly appropriate.
            (Map::class.java.isAssignableFrom(declaredClass) ||
                    (actualClass != null && Map::class.java.isAssignableFrom(actualClass))) -> {
                val declaredTypeAmended = MapSerializer.deriveParameterizedType(declaredType, declaredClass, actualClass)
                serializersByType.computeIfAbsent(declaredTypeAmended) {
                    makeMapSerializer(declaredTypeAmended)
                }
            }
            Enum::class.java.isAssignableFrom(actualClass ?: declaredClass) -> serializersByType.computeIfAbsent(actualClass ?: declaredClass) {
                whitelist.requireWhitelisted(actualType)
                EnumSerializer(actualType, actualClass ?: declaredClass, this)
            }
            else -> makeClassSerializer(actualClass ?: declaredClass, actualType, declaredType)
        }

        serializersByDescriptor.putIfAbsent(serializer.typeDescriptor, serializer)

        return serializer
    }

    /**
     * Try and infer concrete types for any generics type variables for the actual class encountered, based on the declared
     * type.
     */
    // TODO: test GenericArrayType
    private fun inferTypeVariables(actualClass: Class<*>?, declaredClass: Class<*>, declaredType: Type): Type? =
            when (declaredType) {
                is ParameterizedType -> inferTypeVariables(actualClass, declaredClass, declaredType)
            // Nothing to infer, otherwise we'd have ParameterizedType
                is Class<*> -> actualClass
                is GenericArrayType -> {
                    val declaredComponent = declaredType.genericComponentType
                    inferTypeVariables(actualClass?.componentType, declaredComponent.asClass()!!, declaredComponent)?.asArray()
                }
                is TypeVariable<*> -> actualClass
                is WildcardType -> actualClass
                else -> null
            }

    /**
     * Try and infer concrete types for any generics type variables for the actual class encountered, based on the declared
     * type, which must be a [ParameterizedType].
     */
    private fun inferTypeVariables(actualClass: Class<*>?, declaredClass: Class<*>, declaredType: ParameterizedType): Type? {
        if (actualClass == null || declaredClass == actualClass) {
            return null
        } else if (declaredClass.isAssignableFrom(actualClass)) {
            return if (actualClass.typeParameters.isNotEmpty()) {
                // The actual class can never have type variables resolved, due to the JVM's use of type erasure, so let's try and resolve them
                // Search for declared type in the inheritance hierarchy and then see if that fills in all the variables
                val implementationChain: List<Type>? = findPathToDeclared(actualClass, declaredType, mutableListOf<Type>())
                if (implementationChain != null) {
                    val start = implementationChain.last()
                    val rest = implementationChain.dropLast(1).drop(1)
                    val resolver = rest.reversed().fold(TypeResolver().where(start, declaredType)) { resolved, chainEntry ->
                        val newResolved = resolved.resolveType(chainEntry)
                        TypeResolver().where(chainEntry, newResolved)
                    }
                    // The end type is a special case as it is a Class, so we need to fake up a ParameterizedType for it to get the TypeResolver to do anything.
                    val endType = DeserializedParameterizedType(actualClass, actualClass.typeParameters)
                    val resolvedType = resolver.resolveType(endType)
                    resolvedType
                } else throw NotSerializableException("No inheritance path between actual $actualClass and declared $declaredType.")
            } else actualClass
        } else throw NotSerializableException("Found object of type $actualClass in a property expecting $declaredType")
    }

    // Stop when reach declared type or return null if we don't find it.
    private fun findPathToDeclared(startingType: Type, declaredType: Type, chain: MutableList<Type>): List<Type>? {
        chain.add(startingType)
        val startingClass = startingType.asClass()
        if (startingClass == declaredType.asClass()) {
            // We're done...
            return chain
        }
        // Now explore potential options of superclass and all interfaces
        val superClass = startingClass?.genericSuperclass
        val superClassChain = if (superClass != null) {
            val resolved = TypeResolver().where(startingClass.asParameterizedType(), startingType.asParameterizedType()).resolveType(superClass)
            findPathToDeclared(resolved, declaredType, ArrayList(chain))
        } else null
        if (superClassChain != null) return superClassChain
        for (iface in startingClass?.genericInterfaces ?: emptyArray()) {
            val resolved = TypeResolver().where(startingClass!!.asParameterizedType(), startingType.asParameterizedType()).resolveType(iface)
            return findPathToDeclared(resolved, declaredType, ArrayList(chain)) ?: continue
        }
        return null
    }

    /**
     * Lookup and manufacture a serializer for the given AMQP type descriptor, assuming we also have the necessary types
     * contained in the [Schema].
     */
    @Throws(NotSerializableException::class)
    fun get(typeDescriptor: Any, schema: Schema): AMQPSerializer<Any> {
        return serializersByDescriptor[typeDescriptor] ?: {
            processSchema(FactorySchemaAndDescriptor(schema, typeDescriptor))
            serializersByDescriptor[typeDescriptor] ?: throw NotSerializableException(
                    "Could not find type matching descriptor $typeDescriptor.")
        }()
    }

    /**
     * Register a custom serializer for any type that cannot be serialized or deserialized by the default serializer
     * that expects to find getters and a constructor with a parameter for each property.
     */
    fun register(customSerializer: CustomSerializer<out Any>) {
        if (!serializersByDescriptor.containsKey(customSerializer.typeDescriptor)) {
            customSerializers += customSerializer
            serializersByDescriptor[customSerializer.typeDescriptor] = customSerializer
            for (additional in customSerializer.additionalSerializers) {
                register(additional)
            }
        }
    }

    /**
     * Iterate over an AMQP schema, for each type ascertain weather it's on ClassPath of [classloader] amd
     * if not use the [ClassCarpenter] to generate a class to use in it's place
     */
    private fun processSchema(schema: FactorySchemaAndDescriptor, sentinel: Boolean = false) {
        val metaSchema = CarpenterMetaSchema.newInstance()
        for (typeNotation in schema.schema.types) {
            try {
                val serialiser = processSchemaEntry(typeNotation)

                // if we just successfully built a serialiser for the type but the type fingerprint
                // doesn't match that of the serialised object then we are dealing with  different
                // instance of the class, as such we need to build an EvolutionSerialiser
                if (serialiser.typeDescriptor != typeNotation.descriptor.name) {
                    getEvolutionSerializer(typeNotation, serialiser)
                }
            } catch (e: ClassNotFoundException) {
                if (sentinel) throw e
                metaSchema.buildFor(typeNotation, classloader)
            }
        }

        if (metaSchema.isNotEmpty()) {
            val mc = MetaCarpenter(metaSchema, classCarpenter)
            mc.build()
            processSchema(schema, true)
        }
    }

    private fun processSchemaEntry(typeNotation: TypeNotation) = when (typeNotation) {
        is CompositeType -> processCompositeType(typeNotation) // java.lang.Class (whether a class or interface)
        is RestrictedType -> processRestrictedType(typeNotation) // Collection / Map, possibly with generics
    }

    // TODO: class loader logic, and compare the schema.
    private fun processRestrictedType(typeNotation: RestrictedType) = get(null,
            typeForName(typeNotation.name, classloader))

    private fun processCompositeType(typeNotation: CompositeType): AMQPSerializer<Any> {
        // TODO: class loader logic, and compare the schema.
        val type = typeForName(typeNotation.name, classloader)
        return get(type.asClass() ?: throw NotSerializableException("Unable to build composite type for $type"), type)
    }

    private fun makeClassSerializer(clazz: Class<*>, type: Type, declaredType: Type): AMQPSerializer<Any> = serializersByType.computeIfAbsent(type) {
        if (isPrimitive(clazz)) {
            AMQPPrimitiveSerializer(clazz)
        } else {
            findCustomSerializer(clazz, declaredType) ?: run {
                if (type.isArray()) {
                    // Don't need to check the whitelist since each element will come back through the whitelisting process.
                    if (clazz.componentType.isPrimitive) PrimArraySerializer.make(type, this)
                    else ArraySerializer.make(type, this)
                } else if (clazz.kotlin.objectInstance != null) {
                    whitelist.requireWhitelisted(clazz)
                    SingletonSerializer(clazz, clazz.kotlin.objectInstance!!, this)
                } else {
                    whitelist.requireWhitelisted(type)
                    ObjectSerializer(type, this)
                }
            }
        }
    }

    internal fun findCustomSerializer(clazz: Class<*>, declaredType: Type): AMQPSerializer<Any>? {
        // e.g. Imagine if we provided a Map serializer this way, then it won't work if the declared type is
        // AbstractMap, only Map. Otherwise it needs to inject additional schema for a RestrictedType source of the
        // super type.  Could be done, but do we need it?
        for (customSerializer in customSerializers) {
            if (customSerializer.isSerializerFor(clazz)) {
                val declaredSuperClass = declaredType.asClass()?.superclass
                if (declaredSuperClass == null || !customSerializer.isSerializerFor(declaredSuperClass) || !customSerializer.revealSubclassesInSchema) {
                    return customSerializer
                } else {
                    // Make a subclass serializer for the subclass and return that...
                    return CustomSerializer.SubClass(clazz, uncheckedCast(customSerializer))
                }
            }
        }
        return null
    }

    private fun makeMapSerializer(declaredType: ParameterizedType): AMQPSerializer<Any> {
        val rawType = declaredType.rawType as Class<*>
        rawType.checkSupportedMapType()
        return MapSerializer(declaredType, this)
    }

    companion object {
        fun isPrimitive(type: Type): Boolean = primitiveTypeName(type) != null

        fun primitiveTypeName(type: Type): String? {
            val clazz = type as? Class<*> ?: return null
            return primitiveTypeNames[Primitives.unwrap(clazz)]
        }

        fun primitiveType(type: String): Class<*>? {
            return namesOfPrimitiveTypes[type]
        }

        private val primitiveTypeNames: Map<Class<*>, String> = mapOf(
                Character::class.java to "char",
                Char::class.java to "char",
                Boolean::class.java to "boolean",
                Byte::class.java to "byte",
                UnsignedByte::class.java to "ubyte",
                Short::class.java to "short",
                UnsignedShort::class.java to "ushort",
                Int::class.java to "int",
                UnsignedInteger::class.java to "uint",
                Long::class.java to "long",
                UnsignedLong::class.java to "ulong",
                Float::class.java to "float",
                Double::class.java to "double",
                Decimal32::class.java to "decimal32",
                Decimal64::class.java to "decimal62",
                Decimal128::class.java to "decimal128",
                Date::class.java to "timestamp",
                UUID::class.java to "uuid",
                ByteArray::class.java to "binary",
                String::class.java to "string",
                Symbol::class.java to "symbol")

        private val namesOfPrimitiveTypes: Map<String, Class<*>> = primitiveTypeNames.map { it.value to it.key }.toMap()

        fun nameForType(type: Type): String = when (type) {
            is Class<*> -> {
                primitiveTypeName(type) ?: if (type.isArray) {
                    "${nameForType(type.componentType)}${if (type.componentType.isPrimitive) "[p]" else "[]"}"
                } else type.name
            }
            is ParameterizedType -> "${nameForType(type.rawType)}<${type.actualTypeArguments.joinToString { nameForType(it) }}>"
            is GenericArrayType -> "${nameForType(type.genericComponentType)}[]"
            is WildcardType -> "?"
            is TypeVariable<*> -> "?"
            else -> throw NotSerializableException("Unable to render type $type to a string.")
        }

        private fun typeForName(name: String, classloader: ClassLoader): Type {
            return if (name.endsWith("[]")) {
                val elementType = typeForName(name.substring(0, name.lastIndex - 1), classloader)
                if (elementType is ParameterizedType || elementType is GenericArrayType) {
                    DeserializedGenericArrayType(elementType)
                } else if (elementType is Class<*>) {
                    java.lang.reflect.Array.newInstance(elementType, 0).javaClass
                } else {
                    throw NotSerializableException("Not able to deserialize array type: $name")
                }
            } else if (name.endsWith("[p]")) {
                // There is no need to handle the ByteArray case as that type is coercible automatically
                // to the binary type and is thus handled by the main serializer and doesn't need a
                // special case for a primitive array of bytes
                when (name) {
                    "int[p]" -> IntArray::class.java
                    "char[p]" -> CharArray::class.java
                    "boolean[p]" -> BooleanArray::class.java
                    "float[p]" -> FloatArray::class.java
                    "double[p]" -> DoubleArray::class.java
                    "short[p]" -> ShortArray::class.java
                    "long[p]" -> LongArray::class.java
                    else -> throw NotSerializableException("Not able to deserialize array type: $name")
                }
            } else {
                DeserializedParameterizedType.make(name, classloader)
            }
        }
    }

    object AnyType : WildcardType {
        override fun getUpperBounds(): Array<Type> = arrayOf(Object::class.java)

        override fun getLowerBounds(): Array<Type> = emptyArray()

        override fun toString(): String = "?"
    }
}
