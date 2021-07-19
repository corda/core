package net.corda.node

import com.google.common.collect.Iterators
import org.apache.logging.log4j.core.Appender
import org.apache.logging.log4j.core.Core
import org.apache.logging.log4j.core.Filter
import org.apache.logging.log4j.core.LogEvent
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.plugins.Plugin
import org.apache.logging.log4j.core.config.plugins.PluginAttribute
import org.apache.logging.log4j.core.config.plugins.PluginElement
import org.apache.logging.log4j.core.config.plugins.PluginFactory
import java.io.RandomAccessFile
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicInteger

@Plugin(name = "MemoryAppender",
        category = Core.CATEGORY_NAME,
        elementType = Appender.ELEMENT_TYPE)
class InMemoryAppender(name: String,
                       filter: Filter?) : AbstractAppender(name, filter, null) {

    companion object {
        @PluginFactory
        @JvmStatic
        fun createAppender(
                @PluginAttribute("name") name: String,
                @PluginElement("Filter") filter: Filter?): InMemoryAppender {
            return InMemoryAppender(name, filter)
        }
    }

    private val buffer = CircularOverwritingBuffer(8192)

    override fun append(event: LogEvent?) {
        event?.let {
            buffer.add("[${event.level}] ${DateTimeFormatter.ISO_INSTANT.format(Instant.now())} ${event.message.formattedMessage}")
        }
    }

    fun inOrder(): Iterator<String> {
        return buffer.emitInOrder().iterator()
    }
}

class CircularOverwritingBuffer(val slots: Int,
                                val maxItemSize: Int = 512,
                                val allowNonPowerOfTwoSizedBuffer: Boolean = false,
                                val deleteBackingFileOnExit: Boolean = true) {

    init {

        if (!allowNonPowerOfTwoSizedBuffer && slots and (slots - 1) != 0) {
            throw IllegalStateException("Buffer size must be a power of 2")
        }
    }

    private val tempFile = Files.createTempFile("rotating-buffer", ".bin").also {
        if (deleteBackingFileOnExit) {
            it.toFile().deleteOnExit()
        }
    }
    private val randomAccessFile = RandomAccessFile(tempFile.toFile(), "rw")
    private val slotSize = maxItemSize + 8
    private val sizeInBytes: Long = (slotSize * this.slots).toLong()
    private val writeBuffer: MappedByteBuffer = randomAccessFile.channel.map(FileChannel.MapMode.READ_WRITE, 0, sizeInBytes).also {
        it.position(0)
    }

    private val nextSlot = AtomicInteger(0)

    fun add(item: String) {
        synchronized(this) {
            val ourSlot = nextSlot.getAndUpdate {
                (it + 1) % slots
            }
            writeBuffer.position(ourSlot * slotSize)
            val toByteArray = item.toByteArray(Charsets.UTF_8)
            val sizeOfBytesToWrite = toByteArray.size.coerceAtMost(maxItemSize)
            writeBuffer.putInt(ourSlot * slotSize, sizeOfBytesToWrite)
            writeBuffer.put(toByteArray, 0, sizeOfBytesToWrite)
        }
    }

    fun nextSlot(): Int {
        return nextSlot.get();
    }

    fun emitInOrder(maxElements: Int = Int.MAX_VALUE): Iterable<String> {
        val readBuffer = writeBuffer.asReadOnlyBuffer()
        val nextSlotAtTimeOfEmit = nextSlot.get()
        readBuffer.position(nextSlotAtTimeOfEmit * slotSize)
        val sizeOfValueInNextSlot = readBuffer.int
        val fromZeroToNextSlotIterator = object : Iterator<String> {
            val available = nextSlotAtTimeOfEmit - 0
            val offset = (available - maxElements.coerceAtMost(available))
            var currentSlot = offset
            override fun hasNext(): Boolean {
                return currentSlot < nextSlotAtTimeOfEmit
            }

            override fun next(): String {
                readBuffer.position(currentSlot * slotSize)
                val sizeOfString = readBuffer.int
                val toReturn = ByteArray(sizeOfString)
                readBuffer.get(toReturn)
                currentSlot += 1
                return toReturn.toString(Charsets.UTF_8)
            }
        }
        val iteratorToReturn = if (sizeOfValueInNextSlot == 0) {
            fromZeroToNextSlotIterator
        } else {
            val totalToConsume = slots.coerceAtMost(maxElements)
            val availableInFirstSection = slots - nextSlotAtTimeOfEmit
            val availableInSecondSection = nextSlotAtTimeOfEmit
            //newest elements will always be in the "second" section
            //if there are total 5 to consume, and there are 3 in second section we should only take 2 from first section
            val numberToAcceptFromFirstSection = 0.coerceAtLeast(totalToConsume - availableInSecondSection)
            val firstSectionOffset = availableInFirstSection - numberToAcceptFromFirstSection
            val numberToAcceptFromSecondSection = (totalToConsume - numberToAcceptFromFirstSection)
            val secondSectionOffset = availableInSecondSection - numberToAcceptFromSecondSection

            val fromNextSlotToEnd = object : Iterator<String> {
                var currentSlot = nextSlotAtTimeOfEmit + firstSectionOffset
                override fun hasNext(): Boolean {
                    return currentSlot < slots
                }

                override fun next(): String {
                    readBuffer.position(currentSlot * slotSize)
                    val sizeOfString = readBuffer.int
                    val toReturn = ByteArray(sizeOfString)
                    readBuffer.get(toReturn)
                    currentSlot += 1
                    return toReturn.toString(Charsets.UTF_8)
                }
            }
            val secondSection = object : Iterator<String> {
                var currentSlot = 0 + secondSectionOffset
                override fun hasNext(): Boolean {
                    return currentSlot < nextSlotAtTimeOfEmit
                }

                override fun next(): String {
                    readBuffer.position(currentSlot * slotSize)
                    val sizeOfString = readBuffer.int
                    val toReturn = ByteArray(sizeOfString)
                    readBuffer.get(toReturn)
                    currentSlot += 1
                    return toReturn.toString(Charsets.UTF_8)
                }
            }
            Iterators.concat(fromNextSlotToEnd, secondSection)
        }

        return object : Iterable<String> {
            override fun iterator(): Iterator<String> {
                return iteratorToReturn
            }
        }
    }

    override fun toString(): String {
        val byteArray = ByteArray(sizeInBytes.toInt())
        val asReadOnlyBuffer = writeBuffer.asReadOnlyBuffer()
        asReadOnlyBuffer.position(0)
        asReadOnlyBuffer.get(byteArray)
        return byteArray.toString(Charsets.UTF_8)
    }
}