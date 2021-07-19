package net.corda.coretests.flows

import co.paralleluniverse.fibers.Fiber
import co.paralleluniverse.fibers.VerifyInstrumentationException
import co.paralleluniverse.strands.SuspendableCallable
import org.junit.Test

/*
Test that the Suspendable annotation is needed for functions and lambdas.
I have commented out where the @Suspendable annotation would be needed.
If ever in the future Suspendable is not needed then the test will fail.
The test assumes runtime instrumentation using the Java agent.
 */
class SuspendableTest {

    private class FiberTask : SuspendableCallable<Unit> {
        // @Suspendable
        override fun run(): Unit {
            Fiber.sleep(1)
        }
    }

    @Test(expected = VerifyInstrumentationException::class)
    fun `test Suspendable annotation is needed in Kotlin when using a function`() {
        val fiber: Fiber<Unit> = Fiber(FiberTask())
        fiber.start().get()
    }

    @Test(expected = VerifyInstrumentationException::class)
    fun `test Suspendable annotation is needed in Kotlin when using a lamda`() {
        val fiber: Fiber<Boolean> = Fiber /* @Suspendable */ { Fiber.sleep(1)}
        fiber.start().get()
    }
}