package net.corda.node

import org.apache.commons.lang3.RandomStringUtils
import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.CoreMatchers.equalTo
import org.junit.Assert
import org.junit.Test
import java.util.*

class CircularOverwritingBufferTest {

    @Test(expected = IllegalStateException::class)
    fun `should not allow incorrectly sized buffer`() {
        CircularOverwritingBuffer(19, allowNonPowerOfTwoSizedBuffer = false)
    }

    @Test
    fun `should handle inserts before wrapping`() {
        val size = 32
        val buffer = CircularOverwritingBuffer(size, allowNonPowerOfTwoSizedBuffer = false)
        buffer.add("1")
        Assert.assertThat(buffer.emitInOrder().toList(), `is`(equalTo(listOf("1"))))
        buffer.add("2")
        Assert.assertThat(buffer.emitInOrder().toList(), `is`(equalTo(listOf("1", "2"))))
        buffer.add("3")
        Assert.assertThat(buffer.emitInOrder().toList(), `is`(equalTo(listOf("1", "2", "3"))))
        buffer.add("1")
        buffer.add("1")
        buffer.add("1")
        Assert.assertThat(buffer.emitInOrder().toList(), `is`(equalTo(listOf("1", "2", "3", "1", "1", "1"))))
    }

    @Test
    fun `should handle inserts with wrapping`() {
        val size = 100
        val buffer = CircularOverwritingBuffer(size, allowNonPowerOfTwoSizedBuffer = true)
        val queue = LinkedList<String>()
        repeat((0..1000).count()) {
            val toInsert = RandomStringUtils.randomAlphabetic(46)
            buffer.add(toInsert)
            queue.offer(toInsert)
            if (queue.size > size) {
                queue.remove()
            }
        }

        Assert.assertThat(buffer.emitInOrder().toList(), `is`(equalTo(queue as List<String>)))
    }

    @Test
    fun `should handle inserts with wrapping and truncation`() {
        val slots = 100
        val slotSize = 64
        val buffer = CircularOverwritingBuffer(slots, maxItemSize = slotSize, allowNonPowerOfTwoSizedBuffer = true)
        val queue = LinkedList<String>()
        repeat((0..1000).count()) {
            val toInsert = RandomStringUtils.randomAlphabetic(128)
            val toInsertTruncated = toInsert.toByteArray(Charsets.UTF_8).copyOfRange(0, slotSize).toString(Charsets.UTF_8)
            buffer.add(toInsert)
            queue.offer(toInsertTruncated)
            if (queue.size > slots) {
                queue.remove()
            }
        }

        Assert.assertThat(buffer.emitInOrder().toList().joinToString("\n"), `is`(equalTo((queue as List<String>).joinToString("\n"))))
    }

    @Test
    fun `should allow selection of newest elements without wrapping`() {
        val size = 10
        val buffer = CircularOverwritingBuffer(size, allowNonPowerOfTwoSizedBuffer = true)
        buffer.add("1")
        Assert.assertThat(buffer.emitInOrder().toList(), `is`(equalTo(listOf("1"))))
        buffer.add("2")
        Assert.assertThat(buffer.emitInOrder(1).toList(), `is`(equalTo(listOf("2"))))
        buffer.add("3")
        Assert.assertThat(buffer.emitInOrder(2).toList(), `is`(equalTo(listOf("2", "3"))))
        buffer.add("1")
        buffer.add("1")
        buffer.add("1")
        Assert.assertThat(buffer.emitInOrder(5).toList(), `is`(equalTo(listOf("2", "3", "1", "1", "1"))))
        Assert.assertThat(buffer.emitInOrder(100).toList(), `is`(equalTo(listOf("1", "2", "3", "1", "1", "1"))))
    }

    @Test
    fun `should allow selection of newest elements with wrapping`() {
        val elementsToGet = 100
        val buffer = CircularOverwritingBuffer(elementsToGet * 4, allowNonPowerOfTwoSizedBuffer = true)
        val queue = LinkedList<String>()
        repeat((0..1000).count()) {
            val toInsert = it.toString()
            buffer.add(toInsert)
            queue.offer(toInsert)
            if (queue.size > elementsToGet) {
                queue.remove()
            }
        }

        Assert.assertThat(buffer.emitInOrder(elementsToGet).toList().joinToString("\n"), `is`(equalTo(queue.joinToString("\n"))))
    }

    @Test
    fun `should not throw error if more elements requested than buffer can hold`(){
        val size = 10
        val buffer = CircularOverwritingBuffer(size, allowNonPowerOfTwoSizedBuffer = true)
        buffer.add("1")
        Assert.assertThat(buffer.emitInOrder(100).toList(), `is`(equalTo(listOf("1"))))
    }

    @Test
    fun `rigourously`() {
        val r = Random()
        repeat((0..1000).count()) {
            val bufferSize = r.nextInt(1024).coerceAtLeast(1)
            val elementsToFetch = r.nextInt(bufferSize).coerceAtLeast(1)
            val buffer = CircularOverwritingBuffer(bufferSize, allowNonPowerOfTwoSizedBuffer = true)
            val queue = LinkedList<String>()
            println("Testing with buffer size = ${bufferSize}, elementsToFetch = $elementsToFetch")
            repeat((0..(bufferSize * r.nextInt(6)).coerceAtLeast(bufferSize)).count()) {
                val toInsert = it.toString()
                buffer.add(toInsert)
                queue.offer(toInsert)
                if (queue.size > elementsToFetch) {
                    queue.remove()
                }
            }
            Assert.assertThat(buffer.emitInOrder(elementsToFetch).toList().joinToString("\n"), `is`(equalTo(queue.joinToString("\n"))))
        }
    }
}