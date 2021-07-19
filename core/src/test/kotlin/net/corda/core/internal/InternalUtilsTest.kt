package net.corda.core.internal

import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import net.corda.core.contracts.TimeWindow
import net.corda.core.crypto.SecureHash
import org.assertj.core.api.Assertions.assertThat
import org.junit.Assert.assertArrayEquals
import org.junit.Test
import org.slf4j.Logger
import rx.subjects.PublishSubject
import java.util.*
import java.util.stream.IntStream
import java.util.stream.Stream
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

open class InternalUtilsTest {
    @Test(timeout=300_000)
	fun `noneOrSingle on an empty collection`() {
        val collection = emptyList<Int>()
        assertThat(collection.noneOrSingle()).isNull()
        assertThat(collection.noneOrSingle { it == 1 }).isNull()
    }

    @Test(timeout=300_000)
	fun `noneOrSingle on a singleton collection`() {
        val collection = listOf(1)
        assertThat(collection.noneOrSingle()).isEqualTo(1)
        assertThat(collection.noneOrSingle { it == 1 }).isEqualTo(1)
        assertThat(collection.noneOrSingle { it == 2 }).isNull()
    }

    @Test(timeout=300_000)
	fun `noneOrSingle on a collection with two items`() {
        val collection = listOf(1, 2)
        assertFailsWith<IllegalArgumentException> { collection.noneOrSingle() }
        assertThat(collection.noneOrSingle { it == 1 }).isEqualTo(1)
        assertThat(collection.noneOrSingle { it == 2 }).isEqualTo(2)
        assertThat(collection.noneOrSingle { it == 3 }).isNull()
        assertFailsWith<IllegalArgumentException> { collection.noneOrSingle { it > 0 } }
    }

    @Test(timeout=300_000)
	fun `noneOrSingle on a collection with items 1, 2, 1`() {
        val collection = listOf(1, 2, 1)
        assertFailsWith<IllegalArgumentException> { collection.noneOrSingle() }
        assertFailsWith<IllegalArgumentException> { collection.noneOrSingle { it == 1 } }
        assertThat(collection.noneOrSingle { it == 2 }).isEqualTo(2)
    }

    @Test(timeout=300_000)
	fun `indexOfOrThrow returns index of the given item`() {
        val collection = listOf(1, 2)
        assertEquals(collection.indexOfOrThrow(1), 0)
        assertEquals(collection.indexOfOrThrow(2), 1)
    }

    @Test(timeout=300_000)
	fun `indexOfOrThrow throws if the given item is not found`() {
        val collection = listOf(1)
        assertFailsWith<IllegalArgumentException> { collection.indexOfOrThrow(2) }
    }

    @Test(timeout=300_000)
	fun `IntProgression stream works`() {
        assertArrayEquals(intArrayOf(1, 2, 3, 4), (1..4).stream().toArray())
        assertArrayEquals(intArrayOf(1, 2, 3, 4), (1 until 5).stream().toArray())
        assertArrayEquals(intArrayOf(1, 3), (1..4 step 2).stream().toArray())
        assertArrayEquals(intArrayOf(1, 3), (1..3 step 2).stream().toArray())
        @Suppress("EmptyRange") // It's supposed to be empty.
        assertArrayEquals(intArrayOf(), (1..0).stream().toArray())
        assertArrayEquals(intArrayOf(1, 0), (1 downTo 0).stream().toArray())
        assertArrayEquals(intArrayOf(3, 1), (3 downTo 0 step 2).stream().toArray())
        assertArrayEquals(intArrayOf(3, 1), (3 downTo 1 step 2).stream().toArray())
    }

    @Test(timeout=300_000)
	fun `IntProgression spliterator characteristics and comparator`() {
        val rangeCharacteristics = IntStream.range(0, 2).spliterator().characteristics()
        val forward = (0..9 step 3).stream().spliterator()
        assertEquals(rangeCharacteristics, forward.characteristics())
        assertEquals(null, forward.comparator)
        val reverse = (9 downTo 0 step 3).stream().spliterator()
        assertEquals(rangeCharacteristics, reverse.characteristics())
        assertEquals(Comparator.reverseOrder(), reverse.comparator)
    }

    @Test(timeout=300_000)
	fun `Stream toTypedArray works`() {
        val a: Array<String> = Stream.of("one", "two").toTypedArray()
        assertEquals(Array<String>::class.java, a.javaClass)
        assertArrayEquals(arrayOf("one", "two"), a)
        val b: Array<String?> = Stream.of("one", "two", null).toTypedArray()
        assertEquals(Array<String?>::class.java, b.javaClass)
        assertArrayEquals(arrayOf("one", "two", null), b)
    }

    @Test(timeout=300_000)
	fun kotlinObjectInstance() {
        assertThat(PublicObject::class.java.kotlinObjectInstance).isSameAs(PublicObject)
        assertThat(PrivateObject::class.java.kotlinObjectInstance).isSameAs(PrivateObject)
        assertThat(ProtectedObject::class.java.kotlinObjectInstance).isSameAs(ProtectedObject)
        assertThat(TimeWindow::class.java.kotlinObjectInstance).isNull()
        assertThat(PrivateClass::class.java.kotlinObjectInstance).isNull()
    }

    @Test(timeout=300_000)
	fun `bufferUntilSubscribed delays emission until the first subscription`() {
        val sourceSubject: PublishSubject<Int> = PublishSubject.create<Int>()
        val bufferedObservable: rx.Observable<Int> = uncheckedCast(sourceSubject.bufferUntilSubscribed())

        sourceSubject.onNext(1)

        val itemsFromBufferedObservable = mutableSetOf<Int>()
        bufferedObservable.subscribe{itemsFromBufferedObservable.add(it)}

        val itemsFromNonBufferedObservable = mutableSetOf<Int>()
        sourceSubject.subscribe{itemsFromNonBufferedObservable.add(it)}

        assertThat(itemsFromBufferedObservable.contains(1))
        assertThat(itemsFromNonBufferedObservable).doesNotContain(1)
    }

    @Test(timeout=300_000)
	fun `test SHA-256 hash for InputStream`() {
        val contents = arrayOfJunk(DEFAULT_BUFFER_SIZE * 2 + DEFAULT_BUFFER_SIZE / 2)
        assertThat(contents.inputStream().hash())
            .isEqualTo(SecureHash.create("A4759E7AA20338328866A2EA17EAF8C7FE4EC6BBE3BB71CEE7DF7C0461B3C22F"))
    }

    @Test(timeout=300_000)
	fun `warnOnce works, but the backing cache grows only to a maximum size`() {
        val MAX_SIZE = 100

        val logger = mock<Logger>()
        logger.warnOnce("a")
        logger.warnOnce("b")
        logger.warnOnce("b")

        // This should cause the eviction of "a".
        (1..MAX_SIZE).forEach { logger.warnOnce("$it") }
        logger.warnOnce("a")

        // "a" should be logged twice because it was evicted.
        verify(logger, times(2)).warn("a")

        // "b" should be logged only once because there was no eviction.
        verify(logger, times(1)).warn("b")
    }

    private fun arrayOfJunk(size: Int) = ByteArray(size).apply {
        for (i in 0 until size) {
            this[i] = (i and 0xFF).toByte()
        }
    }

    object PublicObject
    private object PrivateObject
    protected object ProtectedObject

    private class PrivateClass
}
