package net.corda.serialization.internal.amqp

import net.corda.core.serialization.ConstructorForDeserialization
import net.corda.core.serialization.SerializeForCarpenter
import net.corda.serialization.internal.amqp.testutils.deserialize
import net.corda.serialization.internal.amqp.testutils.serialize
import net.corda.serialization.internal.amqp.testutils.testDefaultFactoryNoEvolution
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.Test

class RoundTripTests {
    @Test
    fun mutableBecomesImmutable() {
        data class C(val l: MutableList<String>)

        val factory = testDefaultFactoryNoEvolution()
        val bytes = SerializationOutput(factory).serialize(C(mutableListOf("a", "b", "c")))
        val newC = DeserializationInput(factory).deserialize(bytes)

        assertThatThrownBy {
            newC.l.add("d")
        }.isInstanceOf(UnsupportedOperationException::class.java)
    }

    @Test
    fun mutableStillMutable() {
        class C(l: MutableList<String>) {
            val l: MutableList<String> = l.toMutableList()
        }

        val factory = testDefaultFactoryNoEvolution()
        val bytes = SerializationOutput(factory).serialize(C(mutableListOf("a", "b", "c")))
        val newC = DeserializationInput(factory).deserialize(bytes)

        newC.l.add("d")
        assertThat(newC.l).containsExactly("a", "b", "c", "d")
    }

    @Test
    fun mutableStillMutable2() {
        data class C(val l: MutableList<String>) {
            @ConstructorForDeserialization
            @Suppress("Unused")
            constructor (l: Collection<String>) : this(l.toMutableList())
        }

        val factory = testDefaultFactoryNoEvolution()
        val bytes = SerializationOutput(factory).serialize(C(mutableListOf("a", "b", "c")))
        val newC = DeserializationInput(factory).deserialize(bytes)

        newC.l.add("d")
        assertThat(newC.l).containsExactly("a", "b", "c", "d")
    }

    @Test
    fun mutableBecomesImmutable4() {
        data class C(val l: List<String>)

        val factory = testDefaultFactoryNoEvolution()
        val bytes = SerializationOutput(factory).serialize(C(listOf("a", "b", "c")))
        val newC = DeserializationInput(factory).deserialize(bytes)
        newC.copy(l = (newC.l + "d"))
    }

    @Test
    fun calculatedValues() {
        data class C(val i: Int) {
            @get:SerializeForCarpenter
            val squared = i * i
        }

        val factory = testDefaultFactoryNoEvolution()
        val bytes = SerializationOutput(factory).serialize(C(2))
        val deserialized = DeserializationInput(factory).deserialize(bytes)
        assertThat(deserialized.squared).isEqualTo(4)
    }

    @Test
    fun calculatedFunction() {
        class C {
            var i: Int = 0
            @SerializeForCarpenter
            fun getSquared() = i * i
        }

        val instance = C().apply { i = 2 }
        val factory = testDefaultFactoryNoEvolution()
        val bytes = SerializationOutput(factory).serialize(instance)
        val deserialized = DeserializationInput(factory).deserialize(bytes)
        assertThat(deserialized.getSquared()).isEqualTo(4)
    }
}
