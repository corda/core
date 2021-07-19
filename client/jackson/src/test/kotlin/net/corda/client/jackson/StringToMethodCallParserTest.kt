package net.corda.client.jackson

import net.corda.core.crypto.SecureHash
import org.junit.Assert.assertArrayEquals
import org.junit.Test
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class StringToMethodCallParserTest {
    @Suppress("UNUSED")
    class Target {
        fun simple() = "simple"
        fun string(noteTextWord: String) = noteTextWord
        fun twoStrings(a: String, b: String) = a + b
        fun simpleObject(hash: SecureHash) = hash.toString()
        fun complexObject(pair: Pair<Int, String>) = pair
        fun complexNestedObject(pairs: Pair<Int, Deque<Char>>) = pairs

        fun overload(a: String) = a
        fun overload(a: String, b: String) = a + b
    }

    val randomHash = "361170110f61086f77ff2c5b7ab36513705da1a3ebabf14dbe5cc9c982c45401"
    val tests = mapOf(
            "simple" to "simple",
            "string noteTextWord: A test of barewords" to "A test of barewords",
            "twoStrings a: Some words, b: ' and some words, like, Kirk, would, speak'" to "Some words and some words, like, Kirk, would, speak",
            "simpleObject hash: $randomHash" to randomHash.toUpperCase(),
            "complexObject pair: { first: 12, second: Word up brother }" to Pair(12, "Word up brother"),
            "overload a: A" to "A",
            "overload a: A, b: B" to "AB"
    )

    @Test(timeout=300_000)
	fun calls() {
        val parser = StringToMethodCallParser(Target::class)
        val target = Target()
        for ((input, output) in tests) {
            assertEquals(output, parser.parse(target, input).invoke())
        }
    }

    /*
     * It would be unreasonable to expect "[ A, B, C ]" to deserialise as "Deque<Char>" by default.
     * Deque is chosen as we still expect it to preserve the order of its elements.
     */
    @Test(timeout=300_000)
	fun complexNestedGenericMethod() {
        val parser = StringToMethodCallParser(Target::class)
        val result = parser.parse(Target(), "complexNestedObject pairs: { first: 101, second: [ A, B, C ] }").invoke()

        assertTrue(result is Pair<*,*>)
        result as Pair<*,*>

        assertEquals(101, result.first)

        assertTrue(result.second is Deque<*>)
        val deque = result.second as Deque<*>
        assertArrayEquals(arrayOf('A', 'B', 'C'), deque.toTypedArray())
    }

    @Suppress("UNUSED")
    class ConstructorTarget(val someWord: String, val aDifferentThing: Int) {
        constructor(alternativeWord: String) : this(alternativeWord, 0)
        constructor(numbers: List<Long>) : this(numbers.map(Long::toString).joinToString("+"), numbers.size)
    }

    @Test(timeout=300_000)
	fun ctor1() {
        val clazz = ConstructorTarget::class.java
        val parser = StringToMethodCallParser(clazz)
        val ctor = clazz.getDeclaredConstructor(String::class.java, Int::class.java)
        val names: List<String> = parser.paramNamesFromConstructor(ctor)
        assertEquals(listOf("someWord", "aDifferentThing"), names)
        val args: Array<Any?> = parser.parseArguments(clazz.name, names.zip(ctor.parameterTypes), "someWord: Blah blah blah, aDifferentThing: 12")
        assertArrayEquals(arrayOf("Blah blah blah", 12), args)
    }

    @Test(timeout=300_000)
	fun ctor2() {
        val clazz = ConstructorTarget::class.java
        val parser = StringToMethodCallParser(clazz)
        val ctor = clazz.getDeclaredConstructor(String::class.java)
        val names: List<String> = parser.paramNamesFromConstructor(ctor)
        assertEquals(listOf("alternativeWord"), names)
        val args: Array<Any?> = parser.parseArguments(clazz.name, names.zip(ctor.parameterTypes), "alternativeWord: Foo bar!")
        assertArrayEquals(arrayOf("Foo bar!"), args)
    }

    @Test(timeout=300_000)
	fun constructorWithGenericArgs() {
        val clazz = ConstructorTarget::class.java
        val ctor = clazz.getDeclaredConstructor(List::class.java)
        StringToMethodCallParser(clazz).apply {
            val names = paramNamesFromConstructor(ctor)
            assertEquals(listOf("numbers"), names)

            val commandLine = "numbers: [ 1, 2, 3 ]"

            val args = parseArguments(clazz.name, names.zip(ctor.parameterTypes), commandLine)
            assertArrayEquals(arrayOf(listOf(1, 2, 3)), args)

            val trueArgs = parseArguments(clazz.name, names.zip(ctor.genericParameterTypes), commandLine)
            assertArrayEquals(arrayOf(listOf(1L, 2L, 3L)), trueArgs)
        }
    }
}
