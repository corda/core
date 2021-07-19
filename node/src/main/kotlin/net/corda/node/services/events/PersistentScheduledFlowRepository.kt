package net.corda.node.services.events

import net.corda.core.contracts.ScheduledStateRef
import net.corda.core.contracts.StateRef
import net.corda.core.crypto.SecureHash
import net.corda.core.schemas.PersistentStateRef
import net.corda.nodeapi.internal.persistence.CordaPersistence

interface ScheduledFlowRepository {
    fun delete(key: StateRef): Boolean
    fun merge(value: ScheduledStateRef): Boolean
    fun getLatest(lookahead: Int): List<Pair<StateRef, ScheduledStateRef>>
}

class PersistentScheduledFlowRepository(val database: CordaPersistence) : ScheduledFlowRepository {
    private fun toPersistentEntityKey(stateRef: StateRef): PersistentStateRef {
        return PersistentStateRef(stateRef.txhash.toString(), stateRef.index)
    }

    private fun toPersistentEntity(key: StateRef, value: ScheduledStateRef): NodeSchedulerService.PersistentScheduledState {
        val output = PersistentStateRef(key.txhash.toString(), key.index)
        return NodeSchedulerService.PersistentScheduledState(output).apply {
            scheduledAt = value.scheduledAt
        }
    }

    private fun fromPersistentEntity(scheduledStateRecord: NodeSchedulerService.PersistentScheduledState): Pair<StateRef, ScheduledStateRef> {
        val txId = scheduledStateRecord.output.txId
        val index = scheduledStateRecord.output.index
        return Pair(StateRef(SecureHash.create(txId), index), ScheduledStateRef(StateRef(SecureHash.create(txId), index), scheduledStateRecord.scheduledAt))
    }

    override fun delete(key: StateRef): Boolean {
        return database.transaction {
            val elem = session.find(NodeSchedulerService.PersistentScheduledState::class.java, toPersistentEntityKey(key))
            if (elem != null) {
                session.remove(elem)
                true
            } else {
                false
            }
        }
    }

    override fun merge(value: ScheduledStateRef): Boolean {
        return database.transaction {
            val existingEntry = session.find(NodeSchedulerService.PersistentScheduledState::class.java, toPersistentEntityKey(value.ref))
            if (existingEntry != null) {
                session.merge(toPersistentEntity(value.ref, value))
                true
            } else {
                session.save(toPersistentEntity(value.ref, value))
                false
            }
        }
    }

    override fun getLatest(lookahead: Int): List<Pair<StateRef, ScheduledStateRef>> {
        return database.transaction {
            val criteriaQuery = session.criteriaBuilder.createQuery(NodeSchedulerService.PersistentScheduledState::class.java)
            val shed = criteriaQuery.from(NodeSchedulerService.PersistentScheduledState::class.java)
            criteriaQuery.select(shed)
            criteriaQuery.orderBy(session.criteriaBuilder.asc(shed.get<NodeSchedulerService.PersistentScheduledState>("scheduledAt")))
            session.createQuery(criteriaQuery).setFirstResult(0).setMaxResults(lookahead)
                    .resultList.map { e -> fromPersistentEntity(e as NodeSchedulerService.PersistentScheduledState) }
        }
    }
}