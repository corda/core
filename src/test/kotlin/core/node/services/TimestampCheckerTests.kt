package core.node.services

import core.TimestampCommand
import core.seconds
import org.junit.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class TimestampCheckerTests {
    val clock = Clock.fixed(Instant.now(), ZoneId.systemDefault())
    val timestampChecker = TimestampChecker(clock, tolerance = 30.seconds)

    @Test
    fun `should return true for valid timestamp`() {
        val now = clock.instant()
        val timestampPast = TimestampCommand(now - 60.seconds, now - 29.seconds)
        val timestampFuture = TimestampCommand(now + 29.seconds, now + 60.seconds)
        assertTrue { timestampChecker.isValid(timestampPast) }
        assertTrue { timestampChecker.isValid(timestampFuture) }
    }

    @Test
    fun `should return false for invalid timestamp`() {
        val now = clock.instant()
        val timestampPast = TimestampCommand(now - 60.seconds, now - 31.seconds)
        val timestampFuture = TimestampCommand(now + 31.seconds, now + 60.seconds)
        assertFalse { timestampChecker.isValid(timestampPast) }
        assertFalse { timestampChecker.isValid(timestampFuture) }
    }
}