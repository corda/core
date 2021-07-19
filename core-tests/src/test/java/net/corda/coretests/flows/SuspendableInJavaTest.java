package net.corda.coretests.flows;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.SuspendableCallable;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

/*
Test that the Suspendable annotation is needed for functions but not for Java lambdas.
I have commented out where the @Suspendable annotation would be needed.
If ever in the future Suspendable is not needed then the test will fail.
The test assumes runtime instrumentation using the Java agent.
 */
public class SuspendableInJavaTest {

    private class FiberTask implements SuspendableCallable<Void> {

        /* @Suspendable */
        @Override
        public Void run() {
            try {
                Fiber.sleep(1);
                return null;
            }
            catch (InterruptedException | SuspendExecution ex) {
                throw new IllegalStateException(ex);
            }
        }
    }

    @Test(expected = ExecutionException.class)
    public void testSuspendableAnnotationIsNeededInJavaForAFunction() throws Exception {
        Fiber<Void> f = new Fiber<>(new FiberTask());
        f.start().get();
    }

    @Test
    public void testSuspendableAnnotationNotNeededForJavaWhenUsingALamda() throws ExecutionException, InterruptedException {
        Fiber<Void> fiber = new Fiber<> (() -> Fiber.sleep(100));
        fiber.start().join();
    }
}