package net.corda.node.services.persistence

import net.corda.core.crypto.SecureHash
import net.corda.core.flows.StateMachineRunId
import net.corda.core.internal.bufferUntilSubscribed
import net.corda.core.messaging.DataFeed
import net.corda.core.messaging.StateMachineTransactionMapping
import net.corda.node.services.api.StateMachineRecordedTransactionMappingStorage
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.nodeapi.internal.persistence.bufferUntilDatabaseCommit
import net.corda.nodeapi.internal.persistence.currentDBSession
import net.corda.nodeapi.internal.persistence.wrapWithDatabaseTransaction
import rx.subjects.PublishSubject
import java.util.*
import javax.annotation.concurrent.ThreadSafe

/**
 * Database storage of a txhash -> state machine id mapping.
 *
 * Mappings are added as transactions are persisted by [ServiceHub.recordTransaction], and never deleted.  Used in the
 * RPC API to correlate transaction creation with flows.
 */
@ThreadSafe
class DBTransactionMappingStorage(private val database: CordaPersistence) : StateMachineRecordedTransactionMappingStorage {
    val updates: PublishSubject<StateMachineTransactionMapping> = PublishSubject.create()

    override fun addMapping(stateMachineRunId: StateMachineRunId, transactionId: SecureHash) {
        database.transaction {
            updates.bufferUntilDatabaseCommit().onNext(StateMachineTransactionMapping(stateMachineRunId, transactionId))
        }
    }

    override fun track(): DataFeed<List<StateMachineTransactionMapping>, StateMachineTransactionMapping> = database.transaction {
        val session = currentDBSession()
        val cb = session.criteriaBuilder
        val cq = cb.createTupleQuery()
        val from = cq.from(DBTransactionStorage.DBTransaction::class.java)
        cq.multiselect(from.get<String>(DBTransactionStorage.DBTransaction::stateMachineRunId.name), from.get<String>(DBTransactionStorage.DBTransaction::txId.name))
        cq.where(cb.isNotNull(from.get<String>(DBTransactionStorage.DBTransaction::stateMachineRunId.name)))
        val flowIds = session.createQuery(cq).resultList.map { StateMachineTransactionMapping(StateMachineRunId(UUID.fromString(it[0] as String)), SecureHash.create(it[1] as String)) }
        DataFeed(flowIds, updates.bufferUntilSubscribed().wrapWithDatabaseTransaction())
    }
}
