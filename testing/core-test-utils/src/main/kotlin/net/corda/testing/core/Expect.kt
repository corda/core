package net.corda.testing.core

import com.google.common.util.concurrent.SettableFuture
import net.corda.core.DoNotImplement
import net.corda.core.internal.uncheckedCast
import net.corda.core.utilities.getOrThrow
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import rx.Observable
import rx.Subscription

/**
 * This file defines a simple DSL for testing non-deterministic sequence of events arriving on an [Observable].
 *
 * [sequence] is used to impose ordering invariants on the stream, whereas [parallel] allows events to arrive in any order.
 *
 * The only restriction on [parallel] is that we should be able to discriminate which branch to take based on the
 * arrived event's type and optionally custom matching logic. If this is ambiguous the first matching piece of DSL will
 * be run.
 *
 * [sequence]s and [parallel]s can be nested arbitrarily
 *
 * Example usage:
 *
 * val stream: Observable<SomeEvent> = (..)
 * stream.expectEvents {
 *   sequence(
 *     expect { event: SomeEvent.A -> require(event.isOk()) },
 *     parallel(
 *       expect { event.SomeEvent.B -> },
 *       expect { event.SomeEvent.C -> }
 *     )
 *   )
 * }
 *
 * The above will test our expectation that the stream should first emit an A, and then a B and C in unspecified order.
 */

private val log: Logger = LoggerFactory.getLogger("Expect")

/**
 * Expect an event of type [T] and run [expectClosure] on it
 *
 * @param klass The [Class] to use for checking the incoming event's type
 * @param match Optional additional matching logic
 * @param expectClosure The closure to run on the event
 */
fun <E : Any> expect(klass: Class<E>, match: (E) -> Boolean, expectClosure: (E) -> Unit): ExpectCompose<E> {
    return ExpectCompose.Single(Expect(klass, match, expectClosure))
}

/**
 * Convenience variant of [expect] reifying the [Class] parameter
 */
inline fun <reified E : Any> expect(
        noinline match: (E) -> Boolean = { true },
        noinline expectClosure: (E) -> Unit
): ExpectCompose<E> = expect(E::class.java, match, expectClosure)

/**
 * Convenience variant of [expect] that only matches events that are strictly equal to [event]
 */
inline fun <reified E : Any> expect(
        event: E,
        noinline expectClosure: (E) -> Unit = {}
): ExpectCompose<E> = expect(match = { event == it }, expectClosure = expectClosure)

/**
 * Tests that events arrive in the specified order.
 *
 * @param expectations The pieces of DSL that should run sequentially when events arrive.
 */
fun <E> sequence(vararg expectations: ExpectCompose<E>): ExpectCompose<E> = ExpectCompose.Sequential(listOf(*expectations))

fun <E> sequence(expectations: List<ExpectCompose<E>>): ExpectCompose<E> = ExpectCompose.Sequential(expectations)

/**
 * Tests that events arrive in unspecified order.
 *
 * @param expectations The pieces of DSL all of which should run but in an unspecified order depending on what sequence events arrive.
 */
fun <E> parallel(vararg expectations: ExpectCompose<E>): ExpectCompose<E> = ExpectCompose.Parallel(listOf(*expectations))

/**
 * Tests that events arrive in unspecified order.
 *
 * @param expectations The pieces of DSL all of which should run but in an unspecified order depending on what sequence events arrive.
 */
fun <E> parallel(expectations: List<ExpectCompose<E>>): ExpectCompose<E> = ExpectCompose.Parallel(expectations)

/**
 * Tests that N events of the same type arrive
 *
 * @param number The number of events expected.
 * @param expectation The piece of DSL to run on each event, with the index of the event passed in.
 */
inline fun <E> replicate(number: Int, expectation: (Int) -> ExpectCompose<E>): ExpectCompose<E> =
        sequence(*Array(number) { expectation(it) })

/**
 * Run the specified DSL against the event [Observable].
 *
 * @param isStrict If false non-matched events are disregarded (so the DSL will only check a subsequence of events).
 * @param expectCompose The DSL we expect to match against the stream of events.
 */
fun <E : Any> Observable<E>.expectEvents(isStrict: Boolean = true, expectCompose: () -> ExpectCompose<E>) {
    var subscription: Subscription? = null

    try {
        serialize().genericExpectEvents(
                isStrict = isStrict,
                stream = { action: (E) -> Unit ->
                    val lock = object {}
                    subscription = subscribe { event ->
                        synchronized(lock) {
                            action(event)
                        }
                    }
                },
                expectCompose = expectCompose
        )
    } finally {
        subscription?.unsubscribe()
    }
}

/**
 * Run the specified DSL against the event [Iterable].
 *
 * @param isStrict If false non-matched events are disregarded (so the DSL will only check a subsequence of events).
 * @param expectCompose The DSL we expect to match against the stream of events.
 */
fun <E : Any> Iterable<E>.expectEvents(isStrict: Boolean = true, expectCompose: () -> ExpectCompose<E>) =
        genericExpectEvents(
                isStrict = isStrict,
                stream = { action: (E) -> Unit ->
                    forEach(action)
                },
                expectCompose = expectCompose
        )

/**
 * Run the specified DSL against the generic event [S]tream
 *
 * @param isStrict If false non-matched events are disregarded (so the DSL will only check a subsequence of events).
 * @param stream A function that extracts events from the stream.
 * @param expectCompose The DSL we expect to match against the stream of events.
 */
fun <S, E : Any> S.genericExpectEvents(
        isStrict: Boolean = true,
        stream: S.((E) -> Unit) -> Unit,
        expectCompose: () -> ExpectCompose<E>
) {
    val finishFuture = SettableFuture.create<Unit>()
    /**
     * Internally we create a "lazy" state automaton. The outgoing edges are state.getExpectedEvents() modulo additional
     * matching logic. When an event comes we extract the first edge that matches using state.nextState(event), which
     * returns the next state and the piece of dsl to be run on the event. If nextState() returns null it means the event
     * didn't match at all, in this case we either fail (if isStrict=true) or carry on with the same state (if isStrict=false)
     *
     * TODO Think about pre-compiling the state automaton, possibly introducing regexp constructs. This requires some
     * thinking, as the [parallel] construct blows up the state space factorially, so we need some clever lazy expansion
     * of states.
     */
    var state = ExpectComposeState.fromExpectCompose(expectCompose())
    stream { event ->
        if (state is ExpectComposeState.Finished) {
            if (isStrict) {
                log.warn("Got event $event, but was expecting no further events")
            }
            return@stream
        }
        val next = state.nextState(event)
        val expectedStates = state.getExpectedEvents()
        log.debug("$event :: ${expectedStates.map { it.simpleName }} -> ${next?.second?.getExpectedEvents()?.map { it.simpleName }}")
        if (next == null) {
            val message = "Got $event, did not match any expectations of type ${expectedStates.map { it.simpleName }}"
            if (isStrict) {
                finishFuture.setException(Exception(message))
                state = ExpectComposeState.Finished()
            } else {
                log.debug("$message, discarding event as isStrict=false")
            }
        } else {
            state = next.second
            val expectClosure = next.first
            // Now run the matching piece of dsl
            try {
                expectClosure()
            } catch (exception: Throwable) {
                finishFuture.setException(exception)
            }
            if (state is ExpectComposeState.Finished) {
                finishFuture.set(Unit)
            }
        }
    }
    finishFuture.getOrThrow()
}

/**
 * Part of the Expectation DSL
 */
@DoNotImplement
sealed class ExpectCompose<out E> {
    internal class Single<out E, T : E>(val expect: Expect<E, T>) : ExpectCompose<E>()
    internal class Sequential<out E>(val sequence: List<ExpectCompose<E>>) : ExpectCompose<E>()
    internal class Parallel<out E>(val parallel: List<ExpectCompose<E>>) : ExpectCompose<E>()
}

internal data class Expect<out E, T : E>(
        val clazz: Class<T>,
        val match: (T) -> Boolean,
        val expectClosure: (T) -> Unit
)

private sealed class ExpectComposeState<E : Any> {

    abstract fun nextState(event: E): Pair<() -> Unit, ExpectComposeState<E>>?
    abstract fun getExpectedEvents(): List<Class<out E>>

    class Finished<E : Any> : ExpectComposeState<E>() {
        override fun nextState(event: E) = null
        override fun getExpectedEvents(): List<Class<E>> = listOf()
    }

    class Single<E : Any, T : E>(val single: ExpectCompose.Single<E, T>) : ExpectComposeState<E>() {
        override fun nextState(event: E): Pair<() -> Unit, ExpectComposeState<E>>? =
                if (single.expect.clazz.isAssignableFrom(event.javaClass)) {
                    val coercedEvent: T = uncheckedCast(event)
                    if (single.expect.match(coercedEvent)) {
                        Pair({ single.expect.expectClosure(coercedEvent) }, Finished())
                    } else {
                        null
                    }
                } else {
                    null
                }

        override fun getExpectedEvents() = listOf(single.expect.clazz)
    }

    class Sequential<E : Any>(
            val sequential: ExpectCompose.Sequential<E>,
            val index: Int,
            val state: ExpectComposeState<E>
    ) : ExpectComposeState<E>() {
        override fun nextState(event: E): Pair<() -> Unit, ExpectComposeState<E>>? {
            val next = state.nextState(event)
            return if (next == null) {
                null
            } else if (next.second is Finished) {
                if (index == sequential.sequence.size - 1) {
                    Pair(next.first, Finished())
                } else {
                    val nextState = fromExpectCompose(sequential.sequence[index + 1])
                    if (nextState is Finished) {
                        Pair(next.first, Finished<E>())
                    } else {
                        Pair(next.first, Sequential(sequential, index + 1, nextState))
                    }
                }
            } else {
                Pair(next.first, Sequential(sequential, index, next.second))
            }
        }

        override fun getExpectedEvents() = state.getExpectedEvents()
    }

    class Parallel<E : Any>(
            val parallel: ExpectCompose.Parallel<E>,
            val states: List<ExpectComposeState<E>>
    ) : ExpectComposeState<E>() {
        override fun nextState(event: E): Pair<() -> Unit, ExpectComposeState<E>>? {
            states.forEachIndexed { stateIndex, state ->
                val next = state.nextState(event)
                if (next != null) {
                    val nextStates = states.mapIndexed { i, expectComposeState ->
                        if (i == stateIndex) next.second else expectComposeState
                    }
                    return if (nextStates.all { it is Finished }) {
                        Pair(next.first, Finished())
                    } else {
                        Pair(next.first, Parallel(parallel, nextStates))
                    }
                }
            }
            return null
        }

        override fun getExpectedEvents() = states.flatMap { it.getExpectedEvents() }
    }

    companion object {
        fun <E : Any> fromExpectCompose(expectCompose: ExpectCompose<E>): ExpectComposeState<E> {
            return when (expectCompose) {
                is ExpectCompose.Single<E, *> -> {
                    // This coercion should not be needed but kotlin can't reason about existential type variables(T)
                    // so here we're coercing T into E (even though T is invariant).
                    Single(uncheckedCast(expectCompose))
                }
                is ExpectCompose.Sequential -> {
                    if (expectCompose.sequence.isNotEmpty()) {
                        Sequential(expectCompose, 0, fromExpectCompose(expectCompose.sequence[0]))
                    } else {
                        Finished()
                    }
                }
                is ExpectCompose.Parallel -> {
                    if (expectCompose.parallel.isNotEmpty()) {
                        Parallel(expectCompose, expectCompose.parallel.map { fromExpectCompose(it) })
                    } else {
                        Finished()
                    }
                }
            }
        }
    }
}
