package net.corda.core.internal

import net.corda.core.DeleteForDJVM
import net.corda.core.concurrent.CordaFuture
import net.corda.core.contracts.StateRef
import net.corda.core.internal.concurrent.asCordaFuture
import net.corda.core.node.ServiceHub
import net.corda.core.utilities.contextLogger
import java.util.concurrent.CompletableFuture

/**
 * An [FlowAsyncOperation] which suspends a flow until the provided [StateRef]s have been updated.
 *
 * WARNING! Remember that if the node isn't aware of the supplied StateRefs or if the StateRefs are never updated, then
 * the calling flow will remain suspended.
 *
 * @property stateRefs the StateRefs which will be updated.
 * @property services a ServiceHub instance
 */
@DeleteForDJVM
class WaitForStateConsumption(val stateRefs: Set<StateRef>, val services: ServiceHub) : FlowAsyncOperation<Unit> {

    companion object {
        val logger = contextLogger()
    }

    override fun execute(deduplicationId: String): CordaFuture<Unit> {
        val futures = stateRefs.map { services.vaultService.whenConsumed(it).toCompletableFuture() }
        val completedFutures = futures.filter { it.isDone }

        when {
            completedFutures.isEmpty() ->
                logger.info("All StateRefs $stateRefs have yet to be consumed. Suspending...")
            futures == completedFutures ->
                logger.info("All StateRefs $stateRefs have already been consumed. No need to suspend.")
            else -> {
                val updatedStateRefs = completedFutures.flatMap { it.get().consumed.map { it.ref } }
                val notUpdatedStateRefs = stateRefs - updatedStateRefs
                logger.info("StateRefs $notUpdatedStateRefs have yet to be consumed. Suspending...")
            }
        }

        return CompletableFuture.allOf(*futures.toTypedArray()).thenApply { Unit }.asCordaFuture()
    }
}
