@file:Suppress("unused", "MemberVisibilityCanPrivate")

package net.corda.serialization.internal.amqp

import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.whenever
import net.corda.client.rpc.RPCException
import net.corda.core.CordaException
import net.corda.core.CordaRuntimeException
import net.corda.core.contracts.*
import net.corda.core.crypto.Crypto
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.secureRandomBytes
import net.corda.core.flows.FlowException
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.CordaX500Name
import net.corda.core.internal.AbstractAttachment
import net.corda.core.serialization.*
import net.corda.core.transactions.LedgerTransaction
import net.corda.core.utilities.OpaqueBytes
import net.corda.node.serialization.amqp.AMQPServerSerializationScheme
import net.corda.nodeapi.internal.crypto.ContentSignerBuilder
import net.corda.serialization.internal.*
import net.corda.serialization.internal.amqp.testutils.*
import net.corda.serialization.internal.carpenter.ClassCarpenterImpl
import net.corda.testing.contracts.DummyContract
import net.corda.testing.core.BOB_NAME
import net.corda.testing.core.SerializationEnvironmentRule
import net.corda.testing.core.TestIdentity
import net.corda.testing.internal.rigorousMock
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.qpid.proton.amqp.*
import org.apache.qpid.proton.codec.DecoderImpl
import org.apache.qpid.proton.codec.EncoderImpl
import org.assertj.core.api.Assertions.*
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.cert.X509v2CRLBuilder
import org.bouncycastle.cert.jcajce.JcaX509CRLConverter
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.Assert.*
import org.junit.Ignore
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import java.io.IOException
import java.io.NotSerializableException
import java.math.BigDecimal
import java.math.BigInteger
import java.security.cert.X509CRL
import java.time.*
import java.time.temporal.ChronoUnit
import java.util.*
import kotlin.reflect.full.superclasses
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

object AckWrapper {
    object Ack

    fun serialize() {
        val factory = testDefaultFactoryNoEvolution()
        SerializationOutput(factory).serialize(Ack)
    }
}

object PrivateAckWrapper {
    private object Ack

    fun serialize() {
        val factory = testDefaultFactoryNoEvolution()
        SerializationOutput(factory).serialize(Ack)
    }
}

@RunWith(Parameterized::class)
class SerializationOutputTests(private val compression: CordaSerializationEncoding?) {
    private companion object {
        val BOB_IDENTITY = TestIdentity(BOB_NAME, 80).identity
        val megaCorp = TestIdentity(CordaX500Name("MegaCorp", "London", "GB"))
        val miniCorp = TestIdentity(CordaX500Name("MiniCorp", "London", "GB"))
        val MEGA_CORP get() = megaCorp.party
        val MEGA_CORP_PUBKEY get() = megaCorp.publicKey
        val MINI_CORP get() = miniCorp.party
        val MINI_CORP_PUBKEY get() = miniCorp.publicKey
        @Parameters(name = "{0}")
        @JvmStatic
        fun compression() = arrayOf<CordaSerializationEncoding?>(null) + CordaSerializationEncoding.values()
    }

    @Rule
    @JvmField
    val testSerialization = SerializationEnvironmentRule()

    data class Foo(val bar: String, val pub: Int)

    data class testFloat(val f: Float)

    data class testDouble(val d: Double)

    data class testShort(val s: Short)

    data class testBoolean(val b: Boolean)

    interface FooInterface {
        val pub: Int
    }

    data class FooImplements(val bar: String, override val pub: Int) : FooInterface

    data class FooImplementsAndList(val bar: String, override val pub: Int, val names: List<String>) : FooInterface

    data class WrapHashMap(val map: Map<String, String>)

    data class WrapFooListArray(val listArray: Array<List<Foo>>) {
        override fun equals(other: Any?): Boolean {
            return other is WrapFooListArray && Objects.deepEquals(listArray, other.listArray)
        }

        override fun hashCode(): Int {
            return 1 // This isn't used, but without overriding we get a warning.
        }
    }

    data class Woo(val fred: Int) {
        val bob = "Bob"
    }

    data class Woo2(val fred: Int, val bob: String = "Bob") {
        @ConstructorForDeserialization constructor(fred: Int) : this(fred, "Ginger")
    }

    @CordaSerializable
    data class AnnotatedWoo(val fred: Int) {
        val bob = "Bob"
    }

    class FooList : ArrayList<Foo>()

    @Suppress("AddVarianceModifier")
    data class GenericFoo<T>(val bar: String, val pub: T)

    data class ContainsGenericFoo(val contain: GenericFoo<String>)

    data class NestedGenericFoo<T>(val contain: GenericFoo<T>)

    data class ContainsNestedGenericFoo(val contain: NestedGenericFoo<String>)

    data class TreeMapWrapper(val tree: TreeMap<Int, Foo>)

    data class NavigableMapWrapper(val tree: NavigableMap<Int, Foo>)

    data class SortedSetWrapper(val set: SortedSet<Int>)

    open class InheritedGeneric<out X>(val foo: X)

    data class ExtendsGeneric(val bar: Int, val pub: String) : InheritedGeneric<String>(pub)

    interface GenericInterface<out X> {
        val pub: X
    }

    data class ImplementsGenericString(val bar: Int, override val pub: String) : GenericInterface<String>

    data class ImplementsGenericX<out Y>(val bar: Int, override val pub: Y) : GenericInterface<Y>

    abstract class AbstractGenericX<out Z> : GenericInterface<Z>

    data class InheritGenericX<out A>(val duke: Double, override val pub: A) : AbstractGenericX<A>()

    data class CapturesGenericX(val foo: GenericInterface<String>)

    object KotlinObject

    class Mismatch(fred: Int) {
        private val ginger: Int = fred

        override fun equals(other: Any?): Boolean = (other as? Mismatch)?.ginger == ginger
        override fun hashCode(): Int = ginger
    }

    class MismatchType(fred: Long) {
        private val ginger: Int = fred.toInt()

        override fun equals(other: Any?): Boolean = (other as? MismatchType)?.ginger == ginger
        override fun hashCode(): Int = ginger
    }

    @CordaSerializable
    interface AnnotatedInterface

    data class InheritAnnotation(val foo: String) : AnnotatedInterface

    data class PolymorphicProperty(val foo: FooInterface?)

    @CordaSerializable
    class NonZeroByte(val value: Byte) {
        init {
            require(value.toInt() != 0) { "Zero not allowed" }
        }
    }

    private val encodingWhitelist = rigorousMock<EncodingWhitelist>().also {
        if (compression != null) doReturn(true).whenever(it).acceptEncoding(compression)
    }

    private fun defaultFactory(): SerializerFactory {
        return SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader()),
                allowEvolution = false
        )
    }

    private inline fun <reified T : Any> serdes(obj: T,
                                                factory: SerializerFactory = defaultFactory(),
                                                freshDeserializationFactory: SerializerFactory = defaultFactory(),
                                                expectedEqual: Boolean = true,
                                                expectDeserializedEqual: Boolean = true,
                                                reproducableInput: Boolean = true): T {
        val ser = SerializationOutput(factory)
        val bytes = ser.serialize(obj, compression)

        if(reproducableInput) {
            val unoptimizedBytes = ser.serializeUnoptimized(obj, testSerializationContext.withEncoding(compression))
            assertThat(bytes).isEqualTo(unoptimizedBytes).withFailMessage("failed to serialize {}", obj)
        }

        val decoder = DecoderImpl().apply {
            this.register(Envelope.DESCRIPTOR, Envelope)
            this.register(Schema.DESCRIPTOR, Schema)
            this.register(Descriptor.DESCRIPTOR, Descriptor)
            this.register(Field.DESCRIPTOR, Field)
            this.register(CompositeType.DESCRIPTOR, CompositeType)
            this.register(Choice.DESCRIPTOR, Choice)
            this.register(RestrictedType.DESCRIPTOR, RestrictedType)
            this.register(ReferencedObject.DESCRIPTOR, ReferencedObject)
            this.register(TransformsSchema.DESCRIPTOR, TransformsSchema)
            this.register(TransformTypes.DESCRIPTOR, TransformTypes)
        }
        EncoderImpl(decoder)
        DeserializationInput.withDataBytes(bytes, encodingWhitelist) {
            decoder.byteBuffer = it
            // Check that a vanilla AMQP decoder can deserialize without schema.
            val result = decoder.readObject() as Envelope
            assertNotNull(result)
        }
        val des = DeserializationInput(freshDeserializationFactory)
        val desObj = des.deserialize(bytes, testSerializationContext.withEncodingWhitelist(encodingWhitelist))
        assertTrue(deepEquals(obj, desObj) == expectedEqual)

        // Now repeat with a re-used factory
        val ser2 = SerializationOutput(factory)
        val des2 = DeserializationInput(factory)
        val desObj2 = des2.deserialize(ser2.serialize(obj, compression), testSerializationContext.withEncodingWhitelist(encodingWhitelist))
        assertTrue(deepEquals(obj, desObj2) == expectedEqual)
        assertTrue(deepEquals(desObj, desObj2) == expectDeserializedEqual)

        // TODO: add some schema assertions to check correctly formed.
        return desObj
    }

    @Test(timeout=300_000)
	fun isPrimitive() {
        assertTrue(AMQPTypeIdentifiers.isPrimitive(Character::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(Boolean::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(Byte::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(UnsignedByte::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(Short::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(UnsignedShort::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(Int::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(UnsignedInteger::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(Long::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(UnsignedLong::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(Float::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(Double::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(Decimal32::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(Decimal64::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(Decimal128::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(Char::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(Date::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(UUID::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(ByteArray::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(String::class.java))
        assertTrue(AMQPTypeIdentifiers.isPrimitive(Symbol::class.java))
    }

    @Test(timeout=300_000)
	fun `test foo`() {
        val obj = Foo("Hello World!", 123)
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test float`() {
        val obj = testFloat(10.0F)
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test double`() {
        val obj = testDouble(10.0)
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test short`() {
        val obj = testShort(1)
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test bool`() {
        val obj = testBoolean(true)
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test foo implements`() {
        val obj = FooImplements("Hello World!", 123)
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test foo implements and list`() {
        val obj = FooImplementsAndList("Hello World!", 123, listOf("Fred", "Ginger"))
        serdes(obj)
    }

    @Test(expected = IllegalArgumentException::class, timeout=300_000)
    fun `test dislike of HashMap`() {
        val obj = WrapHashMap(HashMap())
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test string array`() {
        val obj = arrayOf("Fred", "Ginger")
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test foo array`() {
        val obj = arrayOf(Foo("Fred", 1), Foo("Ginger", 2))
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test top level list array`() {
        val obj = arrayOf(listOf("Fred", "Ginger"), listOf("Rogers", "Hammerstein"))
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test foo list array`() {
        val obj = WrapFooListArray(arrayOf(listOf(Foo("Fred", 1), Foo("Ginger", 2)), listOf(Foo("Rogers", 3), Foo("Hammerstein", 4))))
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test not all properties in constructor`() {
        val obj = Woo(2)
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test annotated constructor`() {
        val obj = Woo2(3)
        serdes(obj)
    }

    @Test(expected = NotSerializableException::class, timeout=300_000)
    fun `test whitelist`() {
        val obj = Woo2(4)
        serdes(obj, SerializerFactoryBuilder.build(EmptyWhitelist,
                ClassCarpenterImpl(EmptyWhitelist, ClassLoader.getSystemClassLoader())
        ))
    }

    @Test(timeout=300_000)
	fun `test annotation whitelisting`() {
        val obj = AnnotatedWoo(5)
        serdes(obj, SerializerFactoryBuilder.build(EmptyWhitelist,
                ClassCarpenterImpl(EmptyWhitelist, ClassLoader.getSystemClassLoader())
        ))
    }

    @Test(expected = NotSerializableException::class, timeout=300_000)
    fun `test generic list subclass is not supported`() {
        val obj = FooList()
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test generic foo`() {
        val obj = GenericFoo("Fred", "Ginger")
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test generic foo as property`() {
        val obj = ContainsGenericFoo(GenericFoo("Fred", "Ginger"))
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test nested generic foo as property`() {
        val obj = ContainsNestedGenericFoo(NestedGenericFoo(GenericFoo("Fred", "Ginger")))
        serdes(obj)
    }

    // TODO: Generic interfaces / superclasses

    @Test(timeout=300_000)
	fun `test extends generic`() {
        val obj = ExtendsGeneric(1, "Ginger")
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test implements generic`() {
        val obj = ImplementsGenericString(1, "Ginger")
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test implements generic captured`() {
        val obj = CapturesGenericX(ImplementsGenericX(1, "Ginger"))
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test inherits generic captured`() {
        val obj = CapturesGenericX(InheritGenericX(1.0, "Ginger"))
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test TreeMap`() {
        val obj = TreeMap<Int, Foo>()
        obj[456] = Foo("Fred", 123)
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test TreeMap property`() {
        val obj = TreeMapWrapper(TreeMap())
        obj.tree[456] = Foo("Fred", 123)
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test NavigableMap property`() {
        val obj = NavigableMapWrapper(TreeMap<Int, Foo>())
        obj.tree[456] = Foo("Fred", 123)
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test SortedSet property`() {
        val obj = SortedSetWrapper(TreeSet<Int>())
        obj.set += 456
        serdes(obj)
    }

    @Test(expected = NotSerializableException::class, timeout=300_000)
    fun `test mismatched property and constructor naming`() {
        val obj = Mismatch(456)
        serdes(obj)
    }

    @Test(expected = NotSerializableException::class, timeout=300_000)
    fun `test mismatched property and constructor type`() {
        val obj = MismatchType(456)
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `class constructor is invoked on deserialisation`() {
        compression == null || return // Manipulation of serialized bytes is invalid if they're compressed.
        val serializerFactory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        val ser = SerializationOutput(serializerFactory)
        val des = DeserializationInput(serializerFactory)
        val serialisedOne = ser.serialize(NonZeroByte(1), compression).bytes
        val serialisedTwo = ser.serialize(NonZeroByte(2), compression).bytes

        // Find the index that holds the value byte
        val valueIndex = serialisedOne.zip(serialisedTwo).mapIndexedNotNull { index, (oneByte, twoByte) ->
            if (oneByte.toInt() == 1 && twoByte.toInt() == 2) index else null
        }.single()

        val copy = serialisedTwo.clone()

        // Double check
        copy[valueIndex] = 0x03
        assertThat(des.deserialize(OpaqueBytes(copy), NonZeroByte::class.java, testSerializationContext.withEncodingWhitelist(encodingWhitelist)).value).isEqualTo(3)

        // Now use the forbidden value
        copy[valueIndex] = 0x00
        assertThatExceptionOfType(NotSerializableException::class.java).isThrownBy {
            des.deserialize(OpaqueBytes(copy), NonZeroByte::class.java, testSerializationContext.withEncodingWhitelist(encodingWhitelist))
        }.withStackTraceContaining("Zero not allowed")
    }

    @Test(timeout=300_000)
	fun `test custom serializers on public key`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.PublicKeySerializer)
        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.PublicKeySerializer)
        val obj = MEGA_CORP_PUBKEY
        serdes(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test annotation is inherited`() {
        val obj = InheritAnnotation("blah")
        serdes(obj, SerializerFactoryBuilder.build(EmptyWhitelist,
                ClassCarpenterImpl(EmptyWhitelist, ClassLoader.getSystemClassLoader())
        ))
    }

    @Test(timeout=300_000)
	fun `generics from java are supported`() {
        val obj = DummyOptional<String>("YES")
        serdes(obj, SerializerFactoryBuilder.build(EmptyWhitelist,
                ClassCarpenterImpl(EmptyWhitelist, ClassLoader.getSystemClassLoader())
        ))
    }

    @Test(timeout=300_000)
	fun `test throwables serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.ThrowableSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.ThrowableSerializer(factory2))

        val t = IllegalAccessException("message").fillInStackTrace()

        val desThrowable = serdesThrowableWithInternalInfo(t, factory, factory2, false)
        assertSerializedThrowableEquivalent(t, desThrowable)
    }

    private fun serdesThrowableWithInternalInfo(t: Throwable, factory: SerializerFactory, factory2: SerializerFactory, expectedEqual: Boolean = true): Throwable {
        val newContext = SerializationFactory.defaultFactory.defaultContext.withProperty(CommonPropertyNames.IncludeInternalInfo, true)
        return SerializationFactory.defaultFactory.asCurrent { withCurrentContext(newContext) { serdes(t, factory, factory2, expectedEqual) } }
    }

    @Test(timeout=300_000)
	fun `test complex throwables serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.ThrowableSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.ThrowableSerializer(factory2))

        try {
            try {
                throw IOException("Layer 1")
            } catch (t: Throwable) {
                throw IllegalStateException("Layer 2", t)
            }
        } catch (t: Throwable) {
            val desThrowable = serdesThrowableWithInternalInfo(t, factory, factory2, false)
            assertSerializedThrowableEquivalent(t, desThrowable)
        }
    }

    private fun assertSerializedThrowableEquivalent(t: Throwable, desThrowable: Throwable) {
        assertTrue(desThrowable is CordaRuntimeException) // Since we don't handle the other case(s) yet
        if (desThrowable is CordaRuntimeException) {
            assertEquals("${t.javaClass.name}: ${t.message}", desThrowable.message)
            assertTrue(Objects.deepEquals(t.stackTrace.toStackTraceBasic, desThrowable.stackTrace.toStackTraceBasic))
            assertEquals(t.suppressed.size, desThrowable.suppressed.size)
            t.suppressed.zip(desThrowable.suppressed).forEach { (before, after) -> assertSerializedThrowableEquivalent(before, after) }
        }
    }

    @Test(timeout=300_000)
	fun `test suppressed throwables serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.ThrowableSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.ThrowableSerializer(factory2))

        try {
            try {
                throw IOException("Layer 1")
            } catch (t: Throwable) {
                val e = IllegalStateException("Layer 2")
                e.addSuppressed(t)
                throw e
            }
        } catch (t: Throwable) {
            val desThrowable = serdesThrowableWithInternalInfo(t, factory, factory2, false)
            assertSerializedThrowableEquivalent(t, desThrowable)
        }
    }

    @Test(timeout=300_000)
	fun `test flow corda exception subclasses serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.ThrowableSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.ThrowableSerializer(factory2))

        val obj = FlowException("message").fillInStackTrace()
        serdesThrowableWithInternalInfo(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test RPC corda exception subclasses serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.ThrowableSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.ThrowableSerializer(factory2))

        val obj = RPCException("message").fillInStackTrace()
        serdesThrowableWithInternalInfo(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test polymorphic property`() {
        val obj = PolymorphicProperty(FooImplements("Ginger", 12))
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test null polymorphic property`() {
        val obj = PolymorphicProperty(null)
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test kotlin object`() {
        serdes(KotlinObject)
    }

    object FooContract : Contract {
        override fun verify(tx: LedgerTransaction) {
        }
    }

    @Test(timeout=300_000)
	fun `test custom object`() {
        serdes(FooContract)
    }

    @Test(timeout=300_000)
@Ignore("Cannot serialize due to known Kotlin/serialization limitation")
    fun `test custom anonymous object`() {
        val anonymous: Contract = object : Contract {
            override fun verify(tx: LedgerTransaction) {
            }
        }
        serdes(anonymous)
    }

    private val FOO_PROGRAM_ID = "net.corda.serialization.internal.amqp.SerializationOutputTests.FooContract"

    class FooState : ContractState {
        override val participants: List<AbstractParty> = emptyList()
    }

    @Test(timeout=300_000)
	fun `test transaction state`() {
        val state = TransactionState(FooState(), FOO_PROGRAM_ID, MEGA_CORP)

        val scheme = AMQPServerSerializationScheme(emptyList())
        val func = scheme::class.superclasses.single { it.simpleName == "AbstractAMQPSerializationScheme" }
                .java.getDeclaredMethod("registerCustomSerializers",
                SerializationContext::class.java,
                SerializerFactory::class.java)
        func.isAccessible = true

        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        func.invoke(scheme, testSerializationContext, factory)

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        func.invoke(scheme, testSerializationContext, factory2)

        val desState = serdes(state, factory, factory2, expectedEqual = false, expectDeserializedEqual = false)
        assertTrue((desState as TransactionState<*>).data is FooState)
        assertTrue(desState.notary == state.notary)
        assertTrue(desState.encumbrance == state.encumbrance)
    }

    @Test(timeout=300_000)
	fun `test currencies serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.CurrencySerializer)

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.CurrencySerializer)

        val obj = Currency.getInstance("USD")
        serdes(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test big decimals serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.BigDecimalSerializer)

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.BigDecimalSerializer)

        val obj = BigDecimal("100000000000000000000000000000.00")
        serdes(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test instants serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.InstantSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.InstantSerializer(factory2))

        val obj = Instant.now()
        serdes(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test durations serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.DurationSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.DurationSerializer(factory2))

        val obj = Duration.of(1000000L, ChronoUnit.MILLIS)
        serdes(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test local date serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.LocalDateSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.LocalDateSerializer(factory2))

        val obj = LocalDate.now()
        serdes(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test local time serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.LocalTimeSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.LocalTimeSerializer(factory2))

        val obj = LocalTime.now()
        serdes(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test local date time serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.LocalDateTimeSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.LocalDateTimeSerializer(factory2))

        val obj = LocalDateTime.now()
        serdes(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test zoned date time serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.ZonedDateTimeSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.ZonedDateTimeSerializer(factory2))

        val obj = ZonedDateTime.now()
        serdes(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test offset time serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.OffsetTimeSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.OffsetTimeSerializer(factory2))

        val obj = OffsetTime.now()
        serdes(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test offset date time serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.OffsetDateTimeSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.OffsetDateTimeSerializer(factory2))

        val obj = OffsetDateTime.now()
        serdes(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test year serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.YearSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.YearSerializer(factory2))

        val obj = Year.now()
        serdes(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test year month serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.YearMonthSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.YearMonthSerializer(factory2))

        val obj = YearMonth.now()
        serdes(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test month day serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.MonthDaySerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.MonthDaySerializer(factory2))

        val obj = MonthDay.now()
        serdes(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test period serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.PeriodSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.PeriodSerializer(factory2))

        val obj = Period.of(99, 98, 97)
        serdes(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test month serialize`() {
        val obj = Month.APRIL
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test day of week serialize`() {
        val obj = DayOfWeek.FRIDAY
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test privacy salt serialize`() {
        serdes(PrivacySalt())
        serdes(PrivacySalt(secureRandomBytes(32)))
    }

    @Test(timeout=300_000)
	fun `test X509 certificate serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.X509CertificateSerializer)

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.X509CertificateSerializer)

        val obj = BOB_IDENTITY.certificate
        serdes(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test cert path serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.CertPathSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.CertPathSerializer(factory2))

        val obj = BOB_IDENTITY.certPath
        serdes(obj, factory, factory2)
    }

    class OtherGeneric<T : Any>

    open class GenericSuperclass<T : Any>(val param: OtherGeneric<T>)

    class GenericSubclass(param: OtherGeneric<String>) : GenericSuperclass<String>(param) {
        override fun equals(other: Any?): Boolean = other is GenericSubclass // This is a bit lame but we just want to check it doesn't throw exceptions
        override fun hashCode(): Int = javaClass.hashCode()
    }

    @Test(timeout=300_000)
	fun `test generic in constructor serialize`() {
        val obj = GenericSubclass(OtherGeneric())
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test StateRef serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )

        val obj = StateRef(SecureHash.randomSHA256(), 0)
        serdes(obj, factory, factory2)
    }

    interface Container

    data class SimpleContainer(val one: String, val another: String) : Container

    data class ParentContainer(val left: SimpleContainer, val right: Container)

    @Test(timeout=300_000)
	fun `test object referenced multiple times`() {
        val simple = SimpleContainer("Fred", "Ginger")
        val parentContainer = ParentContainer(simple, simple)
        assertSame(parentContainer.left, parentContainer.right)

        val parentCopy = serdes(parentContainer)
        assertSame(parentCopy.left, parentCopy.right)
    }

    data class TestNode(val content: String, val children: MutableCollection<TestNode> = ArrayList())

    @Test(timeout=300_000)
@Ignore("Ignored due to cyclic graphs not currently supported by AMQP serialization")
    fun `test serialization of cyclic graph`() {
        val nodeA = TestNode("A")
        val nodeB = TestNode("B", ArrayList(Arrays.asList(nodeA)))
        nodeA.children.add(nodeB)

        // Also blows with StackOverflow error
        assertTrue(nodeB.hashCode() > 0)

        val bCopy = serdes(nodeB)
        assertEquals("A", bCopy.children.single().content)
    }

    data class Bob(val byteArrays: List<ByteArray>)

    @Test(timeout=300_000)
	fun `test list of byte arrays`() {
        val a = ByteArray(1)
        val b = ByteArray(2)
        val obj = Bob(listOf(a, b, a))

        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        val obj2 = serdes(obj, factory, factory2, false, false)

        assertNotSame(obj2.byteArrays[0], obj2.byteArrays[2])
    }

    data class Vic(val a: List<String>, val b: List<String>)

    @Test(timeout=300_000)
	fun `test generics ignored from graph logic`() {
        val a = listOf("a", "b")
        val obj = Vic(a, a)

        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        val objCopy = serdes(obj, factory, factory2)
        assertSame(objCopy.a, objCopy.b)
    }

    class Spike private constructor(val a: String) {
        constructor() : this("a")

        override fun equals(other: Any?): Boolean = other is Spike && other.a == this.a
        override fun hashCode(): Int = a.hashCode()
    }

    @Test(timeout=300_000)
	fun `test private constructor`() {
        val obj = Spike()

        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        serdes(obj, factory, factory2)
    }

    data class BigDecimals(val a: BigDecimal, val b: BigDecimal)

    @Test(timeout=300_000)
	fun `test toString custom serializer`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.BigDecimalSerializer)

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.BigDecimalSerializer)

        val obj = BigDecimals(BigDecimal.TEN, BigDecimal.TEN)
        val objCopy = serdes(obj, factory, factory2)
        assertEquals(objCopy.a, objCopy.b)
    }

    data class BigIntegers(val a: BigInteger, val b: BigInteger)

    @Test(timeout=300_000)
	fun `test BigInteger custom serializer`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.BigIntegerSerializer)

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.BigIntegerSerializer)

        val obj = BigIntegers(BigInteger.TEN, BigInteger.TEN)
        val objCopy = serdes(obj, factory, factory2)
        assertEquals(objCopy.a, objCopy.b)
    }

    private fun emptyCrl(): X509CRL {
        val builder = X509v2CRLBuilder(X500Name("CN=Corda Root CA, O=R3 HoldCo LLC, L=New York, C=US"), Date())
        val provider = BouncyCastleProvider()
        val crlHolder = builder.build(ContentSignerBuilder.build(Crypto.RSA_SHA256, Crypto.generateKeyPair(Crypto.RSA_SHA256).private, provider))
        return JcaX509CRLConverter().setProvider(provider).getCRL(crlHolder)
    }

    @Test(timeout=300_000)
	fun `test X509CRL custom serializer`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.X509CRLSerializer)

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.X509CRLSerializer)

        val obj = emptyCrl()
        serdes(obj, factory, factory2)
    }

    class ByteArrays(val a: ByteArray, val b: ByteArray)

    @Test(timeout=300_000)
	fun `test byte arrays not reference counted`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.BigDecimalSerializer)

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.BigDecimalSerializer)

        val bytes = ByteArray(1)
        val obj = ByteArrays(bytes, bytes)
        val objCopy = serdes(obj, factory, factory2, false, false)
        assertNotSame(objCopy.a, objCopy.b)
    }

    @Test(timeout=300_000)
	fun `test StringBuffer serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.StringBufferSerializer)

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.StringBufferSerializer)

        val obj = StringBuffer("Bob")
        val obj2 = serdes(obj, factory, factory2, false, false)
        assertEquals(obj.toString(), obj2.toString())
    }

    @Test(timeout=300_000)
	fun `test SimpleString serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.SimpleStringSerializer)

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.SimpleStringSerializer)

        val obj = SimpleString("Bob")
        serdes(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test kotlin Unit serialize`() {
        val obj = Unit
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test kotlin Pair serialize`() {
        val obj = Pair("a", 3)
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test InputStream serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.InputStreamSerializer)

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.InputStreamSerializer)
        val bytes = ByteArray(10) { it.toByte() }
        val obj = bytes.inputStream()
        val obj2 = serdes(obj, factory, factory2, expectedEqual = false, expectDeserializedEqual = false, reproducableInput = false)
        val obj3 = bytes.inputStream()  // Can't use original since the stream pointer has moved.
        assertEquals(obj3.available(), obj2.available())
        assertEquals(obj3.read(), obj2.read())
    }

    @Test(timeout=300_000)
	fun `test EnumSet serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.EnumSetSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.EnumSetSerializer(factory2))

        val obj = EnumSet.of(Month.APRIL, Month.AUGUST)
        serdes(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test BitSet serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.BitSetSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.BitSetSerializer(factory2))

        val obj = BitSet.valueOf(kotlin.ByteArray(16) { it.toByte() }).get(0, 123)
        serdes(obj, factory, factory2)
    }

    @Test(timeout=300_000)
	fun `test EnumMap serialize`() {
        val obj = EnumMap<Month, Int>(Month::class.java)
        obj[Month.APRIL] = Month.APRIL.value
        obj[Month.AUGUST] = Month.AUGUST.value
        serdes(obj)
    }

    @Test(timeout=300_000)
	fun `test contract attachment serialize`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.ContractAttachmentSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.ContractAttachmentSerializer(factory2))

        val obj = ContractAttachment(GeneratedAttachment("test".toByteArray(), "test"), DummyContract.PROGRAM_ID)
        val obj2 = serdes(obj, factory, factory2, expectedEqual = false, expectDeserializedEqual = false)
        assertEquals(obj.id, obj2.attachment.id)
        assertEquals(obj.contract, obj2.contract)
        assertEquals(obj.additionalContracts, obj2.additionalContracts)
        assertArrayEquals(obj.open().readBytes(), obj2.open().readBytes())
    }

    @Test(timeout=300_000)
	fun `test contract attachment throws if missing attachment`() {
        val factory = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory.register(net.corda.serialization.internal.amqp.custom.ContractAttachmentSerializer(factory))

        val factory2 = SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        )
        factory2.register(net.corda.serialization.internal.amqp.custom.ContractAttachmentSerializer(factory2))

        val obj = ContractAttachment(object : AbstractAttachment({ throw Exception() }, "test") {
            override val id = SecureHash.zeroHash
        }, DummyContract.PROGRAM_ID)

        assertThatThrownBy {
            serdes(obj, factory, factory2, expectedEqual = false, expectDeserializedEqual = false)
        }.isInstanceOf(MissingAttachmentsException::class.java)
    }

    //
    // Example stacktrace that this test is trying to reproduce
    //
    // java.lang.IllegalArgumentException:
    //      net.corda.core.contracts.TransactionState ->
    //      data(net.corda.core.contracts.ContractState) ->
    //      net.corda.finance.contracts.asset.Cash$State ->
    //      amount(net.corda.core.contracts.Amount<net.corda.core.contracts.Issued<java.util.Currency>>) ->
    //      net.corda.core.contracts.Amount<net.corda.core.contracts.Issued<java.util.Currency>> ->
    //      displayTokenSize(java.math.BigDecimal) ->
    //      wrong number of arguments
    //
    // So the actual problem was objects with multiple getters. The code wasn't looking for one with zero
    // properties, just taking the first one it found with with the most applicable type, and the reflection
    // ordering of the methods was random, thus occasionally we select the wrong one
    //
    @Test(timeout=300_000)
	fun reproduceWrongNumberOfArguments() {
        data class C(val a: Amount<Currency>)

        val factory = testDefaultFactoryNoEvolution()
        factory.register(net.corda.serialization.internal.amqp.custom.BigDecimalSerializer)
        factory.register(net.corda.serialization.internal.amqp.custom.CurrencySerializer)

        val c = C(Amount(100, BigDecimal("1.5"), Currency.getInstance("USD")))

        // were the issue not fixed we'd blow up here
        SerializationOutput(factory).serialize(c, compression)
    }

    @Test(timeout=300_000)
	fun `compression has the desired effect`() {
        compression ?: return
        val factory = defaultFactory()
        val data = ByteArray(12345).also { Random(0).nextBytes(it) }.let { it + it }
        val compressed = SerializationOutput(factory).serialize(data, compression)
        assertEquals(.5, compressed.size.toDouble() / data.size, .03)
        assertArrayEquals(data, DeserializationInput(factory).deserialize(compressed, testSerializationContext.withEncodingWhitelist(encodingWhitelist)))
    }

    @Test(timeout=300_000)
	fun `a particular encoding can be banned for deserialization`() {
        compression ?: return
        val factory = defaultFactory()
        doReturn(false).whenever(encodingWhitelist).acceptEncoding(compression)
        val compressed = SerializationOutput(factory).serialize("whatever", compression)
        val input = DeserializationInput(factory)
        catchThrowable { input.deserialize(compressed, testSerializationContext.withEncodingWhitelist(encodingWhitelist)) }.run {
            assertSame(NotSerializableException::class.java, javaClass)
            assertEquals(encodingNotPermittedFormat.format(compression), message)
        }
    }

    @Test(timeout=300_000)
	fun nestedObjects() {
        // The "test" is that this doesn't throw, anything else is a success
        AckWrapper.serialize()
    }

    @Test(timeout=300_000)
	fun privateNestedObjects() {
        // The "test" is that this doesn't throw, anything else is a success
        PrivateAckWrapper.serialize()
    }

    @Test(timeout=300_000)
	fun throwable() {
        class TestException(message: String?, cause: Throwable?) : CordaException(message, cause)

        val testExcp = TestException("hello", Throwable().apply { stackTrace = Thread.currentThread().stackTrace })
        val factory = testDefaultFactoryNoEvolution()
        SerializationOutput(factory).serialize(testExcp)
    }

    @Test(timeout=300_000)
	fun nestedInner() {
        class C(val a: Int) {
            inner class D(val b: Int)

            fun serialize() {
                val factory = testDefaultFactoryNoEvolution()
                SerializationOutput(factory).serialize(D(4))
            }
        }

        // By the time we escape the serializer we should just have a general
        // NotSerializable Exception
        assertThatExceptionOfType(NotSerializableException::class.java).isThrownBy {
            C(12).serialize()
        }.withMessageContaining("has synthetic fields and is likely a nested inner class")
    }

    @Test(timeout=300_000)
	fun nestedNestedInner() {
        class C(val a: Int) {
            inner class D(val b: Int) {
                inner class E(val c: Int)

                fun serialize() {
                    val factory = testDefaultFactoryNoEvolution()
                    SerializationOutput(factory).serialize(E(4))
                }
            }

            fun serializeD() {
                val factory = testDefaultFactoryNoEvolution()
                SerializationOutput(factory).serialize(D(4))
            }

            fun serializeE() {
                D(1).serialize()
            }
        }

        // By the time we escape the serializer we should just have a general
        // NotSerializable Exception
        assertThatExceptionOfType(NotSerializableException::class.java).isThrownBy {
            C(12).serializeD()
        }.withMessageContaining("has synthetic fields and is likely a nested inner class")

        assertThatExceptionOfType(NotSerializableException::class.java).isThrownBy {
            C(12).serializeE()
        }.withMessageContaining("has synthetic fields and is likely a nested inner class")
    }

    @Test(timeout=300_000)
	fun multiNestedInner() {
        class C(val a: Int) {
            inner class D(val b: Int)
            inner class E(val c: Int)

            fun serializeD() {
                val factory = testDefaultFactoryNoEvolution()
                SerializationOutput(factory).serialize(D(4))
            }

            fun serializeE() {
                val factory = testDefaultFactoryNoEvolution()
                SerializationOutput(factory).serialize(E(4))
            }
        }

        // By the time we escape the serializer we should just have a general
        // NotSerializable Exception
        assertThatExceptionOfType(NotSerializableException::class.java).isThrownBy {
            C(12).serializeD()
        }.withMessageContaining("has synthetic fields and is likely a nested inner class")

        assertThatExceptionOfType(NotSerializableException::class.java).isThrownBy {
            C(12).serializeE()
        }.withMessageContaining("has synthetic fields and is likely a nested inner class")
    }

    interface DataClassByInterface<V> {
        val v: V
    }

    @Test(timeout=300_000)
	fun dataClassBy() {
        data class C(val s: String) : DataClassByInterface<String> {
            override val v: String = "-- $s"
        }

        data class Inner<T>(val wrapped: DataClassByInterface<T>) : DataClassByInterface<T> by wrapped {
            override val v = wrapped.v
        }

        val i = Inner(C("hello"))

        val bytes = SerializationOutput(testDefaultFactory()).serialize(i)

        try {
            DeserializationInput(testDefaultFactory()).deserialize(bytes)
        } catch (e: NotSerializableException) {
            throw Error("Deserializing serialized \$C should not throw")
        }
    }

    @Test(timeout=300_000)
	fun `compression reduces number of bytes significantly`() {
        val ser = SerializationOutput(SerializerFactoryBuilder.build(AllWhitelist,
                ClassCarpenterImpl(AllWhitelist, ClassLoader.getSystemClassLoader())
        ))
        val obj = ByteArray(20000)
        val uncompressedSize = ser.serialize(obj).bytes.size
        val compressedSize = ser.serialize(obj, CordaSerializationEncoding.SNAPPY).bytes.size
        // Ordinarily this might be considered high maintenance, but we promised wire compatibility, so they'd better not change!
        assertEquals(20059, uncompressedSize)
        assertEquals(1018, compressedSize)
    }

    // JDK11: backwards compatibility function to deal with StacktraceElement comparison pre-JPMS
    private fun deepEquals(a: Any?, b: Any?): Boolean {
        return if (a === b)
            true
        else if (a == null || b == null)
            false
        else {
            if (a is Exception && b is Exception)
                (a.cause == b.cause && a.localizedMessage == b.localizedMessage && a.message == b.message) &&
                        Objects.deepEquals(a.stackTrace.toStackTraceBasic, b.stackTrace.toStackTraceBasic)
            else
                Objects.deepEquals(a, b)
        }
    }

    private val <T> Array<T>.toStackTraceBasic: Unit
        get() {
            this.map { StackTraceElementBasic(it as StackTraceElement) }
        }

    // JPMS adds additional fields that are not equal according to classloader/module hierarchy
    data class StackTraceElementBasic(val ste: StackTraceElement) {
        override fun equals(other: Any?): Boolean {
            return if (other is StackTraceElementBasic)
                (ste.className == other.ste.className) &&
                        (ste.methodName == other.ste.methodName) &&
                        (ste.fileName == other.ste.fileName) &&
                        (ste.lineNumber == other.ste.lineNumber)
            else false
        }
    }
}