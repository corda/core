package net.corda.node.services.statemachine

import net.corda.core.CordaRuntimeException
import net.corda.core.serialization.ConstructorForDeserialization

// CORDA-3353 - These exceptions should not be propagated up to rpc as they suppress the real exceptions

class StateTransitionException(
    val transitionAction: Action?,
    val transitionEvent: Event?,
    val exception: Exception
) : CordaRuntimeException(exception.message, exception) {

    @ConstructorForDeserialization
    constructor(exception: Exception): this(null, null, exception)
}

class AsyncOperationTransitionException(exception: Exception) : CordaRuntimeException(exception.message, exception)

class ErrorStateTransitionException(val exception: Exception, val event: Event) : CordaRuntimeException(exception.message, exception)

class ReloadFlowFromCheckpointException(cause: Exception) : CordaRuntimeException(
    "Could not reload flow from checkpoint. This is likely due to a discrepancy " +
            "between the serialization and deserialization of an object in the flow's checkpoint", cause
)