package net.corda.node.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.StartableByRPC
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.startFlow
import net.corda.core.schemas.MappedSchema
import net.corda.core.serialization.CordaSerializable
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.seconds
import net.corda.node.services.statemachine.StaffedFlowHospital
import net.corda.testing.internal.StateMachineTest
import java.util.concurrent.Semaphore
import javax.persistence.Column
import javax.persistence.Entity
import javax.persistence.Id
import javax.persistence.Table
import kotlin.test.assertEquals

abstract class AbstractFlowEntityManagerTest : StateMachineTest() {

    protected companion object {

        const val TABLE_NAME = "entity_manager_custom_table"

        val entityWithIdOne = CustomTableEntity(1, "Dan", "This won't work")
        val anotherEntityWithIdOne = CustomTableEntity(1, "Rick", "I'm pretty sure this will work")
        val entityWithIdTwo = CustomTableEntity(2, "Ivan", "This will break existing CorDapps")
        val entityWithIdThree = CustomTableEntity(3, "Some other guy", "What am I doing here?")
    }

    @CordaSerializable
    enum class CommitStatus { INTERMEDIATE_COMMIT, NO_INTERMEDIATE_COMMIT }

    protected inline fun <reified R : FlowLogic<Any>> CordaRPCOps.expectFlowFailureAndAssertCreatedEntities(
        crossinline flow: (CommitStatus) -> R,
        commitStatus: CommitStatus,
        numberOfDischarges: Int,
        numberOfExpectedEntities: Int
    ): Int {
        var counter = 0
        StaffedFlowHospital.onFlowDischarged.add { _, _ -> ++counter }
        val lock = Semaphore(0)
        StaffedFlowHospital.onFlowKeptForOvernightObservation.add { _, _ -> lock.release() }
        startFlow(flow, commitStatus)
        lock.acquire()
        assertEquals(
            numberOfDischarges,
            counter,
            "[$commitStatus] expected the flow to be discharged from hospital $numberOfDischarges time(s)"
        )
        val numberOfEntities = startFlow(::GetCustomEntities).returnValue.getOrThrow().size
        assertEquals(
            numberOfExpectedEntities,
            numberOfEntities,
            "[$commitStatus] expected $numberOfExpectedEntities to be saved"
        )
        startFlow(::DeleteCustomEntities).returnValue.getOrThrow(30.seconds)
        return numberOfEntities
    }

    protected inline fun <reified R : FlowLogic<Any>> CordaRPCOps.expectFlowSuccessAndAssertCreatedEntities(
        crossinline flow: (CommitStatus) -> R,
        commitStatus: CommitStatus,
        numberOfDischarges: Int,
        numberOfExpectedEntities: Int
    ): Int {
        var counter = 0
        StaffedFlowHospital.onFlowDischarged.add { _, _ -> ++counter }
        startFlow(flow, commitStatus).returnValue.getOrThrow(30.seconds)
        assertEquals(
            numberOfDischarges,
            counter,
            "[$commitStatus] expected the flow to be discharged from hospital $numberOfDischarges time(s)"
        )
        val numberOfEntities = startFlow(::GetCustomEntities).returnValue.getOrThrow().size
        assertEquals(
            numberOfExpectedEntities,
            numberOfEntities,
            "[$commitStatus] expected $numberOfExpectedEntities to be saved"
        )
        startFlow(::DeleteCustomEntities).returnValue.getOrThrow(30.seconds)
        return numberOfEntities
    }

    @StartableByRPC
    class GetCustomEntities : FlowLogic<List<CustomTableEntity>>() {
        @Suspendable
        override fun call(): List<CustomTableEntity> {
            return serviceHub.withEntityManager {
                val criteria = criteriaBuilder.createQuery(CustomTableEntity::class.java)
                criteria.select(criteria.from(CustomTableEntity::class.java))
                createQuery(criteria).resultList
            }
        }
    }

    @StartableByRPC
    class DeleteCustomEntities : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            serviceHub.withEntityManager {
                val delete = criteriaBuilder.createCriteriaDelete(CustomTableEntity::class.java)
                delete.from(CustomTableEntity::class.java)
                createQuery(delete).executeUpdate()
            }
        }
    }

    @Entity
    @Table(name = TABLE_NAME)
    @CordaSerializable
    data class CustomTableEntity constructor(
        @Id
        @Column(name = "id", nullable = false)
        var id: Int,
        @Column(name = "name", nullable = false)
        var name: String,
        @Column(name = "quote", nullable = false)
        var quote: String
    )

    object CustomSchema

    object CustomMappedSchema : MappedSchema(CustomSchema::class.java, 1, listOf(CustomTableEntity::class.java))
}