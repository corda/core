package net.corda.nodeapi.internal.persistence

import co.paralleluniverse.strands.Strand
import com.zaxxer.hikari.HikariDataSource
import com.zaxxer.hikari.pool.HikariPool
import com.zaxxer.hikari.util.ConcurrentBag
import net.corda.common.logging.errorReporting.ErrorCode
import net.corda.core.flows.HospitalizeFlowException
import net.corda.core.internal.NamedCacheFactory
import net.corda.core.schemas.MappedSchema
import net.corda.core.utilities.contextLogger
import net.corda.common.logging.errorReporting.NodeDatabaseErrors
import org.hibernate.tool.schema.spi.SchemaManagementException
import rx.Observable
import rx.Subscriber
import rx.subjects.UnicastSubject
import java.io.Closeable
import java.lang.reflect.Field
import java.sql.Connection
import java.sql.SQLException
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import javax.persistence.AttributeConverter
import javax.persistence.PersistenceException
import javax.sql.DataSource

/**
 * Table prefix for all tables owned by the node module.
 */
const val NODE_DATABASE_PREFIX = "node_"

enum class SchemaInitializationType{
    NONE,
    VALIDATE,
    UPDATE
}

// This class forms part of the node config and so any changes to it must be handled with care
data class DatabaseConfig(
        val initialiseSchema: Boolean = Defaults.initialiseSchema,
        val initialiseAppSchema: SchemaInitializationType = Defaults.initialiseAppSchema,
        val transactionIsolationLevel: TransactionIsolationLevel = Defaults.transactionIsolationLevel,
        val exportHibernateJMXStatistics: Boolean = Defaults.exportHibernateJMXStatistics,
        val mappedSchemaCacheSize: Long = Defaults.mappedSchemaCacheSize
) {
    object Defaults {
        val initialiseSchema = true
        val initialiseAppSchema = SchemaInitializationType.UPDATE
        val transactionIsolationLevel = TransactionIsolationLevel.REPEATABLE_READ
        val exportHibernateJMXStatistics = false
        val mappedSchemaCacheSize = 100L
        val mappedSchemaCacheSizeLowMemoryMode = 25L
    }
}

// This class forms part of the node config and so any changes to it must be handled with care
enum class TransactionIsolationLevel {
    NONE,
    READ_UNCOMMITTED,
    READ_COMMITTED,
    REPEATABLE_READ,
    SERIALIZABLE;

    /**
     * The JDBC constant value of the same name but prefixed with TRANSACTION_ defined in [java.sql.Connection].
     */
    val jdbcString = "TRANSACTION_$name"
    val jdbcValue: Int = java.sql.Connection::class.java.getField(jdbcString).get(null) as Int
}

internal val _prohibitDatabaseAccess = ThreadLocal.withInitial { false }

private val _contextDatabase = InheritableThreadLocal<CordaPersistence>()
var contextDatabase: CordaPersistence
    get() {
        require(_prohibitDatabaseAccess.get() != true) { "Database access is disabled in this context." }
        return _contextDatabase.get() ?: error("Was expecting to find CordaPersistence set on current thread: ${Strand.currentStrand()}")
    }
    set(database) = _contextDatabase.set(database)

/**
 * The logic in the [block] will be prevented from opening a database transaction.
 * Also will not be able to access database resources ( Like the context transaction or the [contextDatabase] ).
 */
fun <T> withoutDatabaseAccess(block: () -> T): T {
    val oldValue = _prohibitDatabaseAccess.get()
    _prohibitDatabaseAccess.set(true)
    try {
        return block()
    } finally {
        _prohibitDatabaseAccess.set(oldValue)
    }
}

val contextDatabaseOrNull: CordaPersistence? get() = _contextDatabase.get()

class CordaPersistence(
        databaseConfig: DatabaseConfig,
        schemas: Set<MappedSchema>,
        val jdbcUrl: String,
        cacheFactory: NamedCacheFactory,
        attributeConverters: Collection<AttributeConverter<*, *>> = emptySet(),
        customClassLoader: ClassLoader? = null,
        val closeConnection: Boolean = true,
        val errorHandler: DatabaseTransaction.(e: Exception) -> Unit = {}
) : Closeable {
    companion object {
        private val log = contextLogger()
    }

    private val defaultIsolationLevel = databaseConfig.transactionIsolationLevel
    val hibernateConfig: HibernateConfiguration by lazy {
        transaction {
            try {
                HibernateConfiguration(schemas, databaseConfig, attributeConverters, jdbcUrl, cacheFactory, customClassLoader)
            } catch (e: Exception) {
                when (e) {
                    is SchemaManagementException -> throw HibernateSchemaChangeException("Incompatible schema change detected. Please run the node with database.initialiseSchema=true. Reason: ${e.message}", e)
                    else -> throw HibernateConfigException("Could not create Hibernate configuration: ${e.message}", e)
                }
            }
        }
    }

    val entityManagerFactory get() = hibernateConfig.sessionFactoryForRegisteredSchemas

    data class Boundary(val txId: UUID, val success: Boolean)

    private var _dataSource: DataSource? = null
    val dataSource: DataSource get() = checkNotNull(_dataSource) { "CordaPersistence not started" }

    fun start(dataSource: DataSource) {
        _dataSource = dataSource
        // Found a unit test that was forgetting to close the database transactions.  When you close() on the top level
        // database transaction it will reset the threadLocalTx back to null, so if it isn't then there is still a
        // database transaction open.  The [transaction] helper above handles this in a finally clause for you
        // but any manual database transaction management is liable to have this problem.
        contextTransactionOrNull?.let {
            error("Was not expecting to find existing database transaction on current strand when setting database: ${Strand.currentStrand()}, $it")
        }
        _contextDatabase.set(this)
        // Check not in read-only mode.
        transaction {
            check(!connection.metaData.isReadOnly) { "Database should not be readonly." }
        }
    }

    fun currentOrNew(isolation: TransactionIsolationLevel = defaultIsolationLevel): DatabaseTransaction {
        return contextTransactionOrNull ?: newTransaction(isolation)
    }

    private val liveTransactions = ConcurrentHashMap<UUID, DatabaseTransaction>()

    fun newTransaction(isolation: TransactionIsolationLevel = defaultIsolationLevel): DatabaseTransaction {
        if (_prohibitDatabaseAccess.get()) {
            throw IllegalAccessException("Database access is not allowed in the current context.")
        }
        val outerTransaction = contextTransactionOrNull
        return DatabaseTransaction(isolation.jdbcValue, contextTransactionOrNull, this).also {
            contextTransactionOrNull = it
            // Outer transaction only exists in a controlled scenario we can ignore.
            if (outerTransaction == null) {
                liveTransactions.put(it.id, it)
                it.onClose { liveTransactions.remove(it.id) }
            }
        }
    }

    fun onAllOpenTransactionsClosed(callback: () -> Unit) {
        // Does not use kotlin toList() as that is not safe to use on concurrent collections.
        val allOpen = ArrayList(liveTransactions.values)
        if (allOpen.isEmpty()) {
            callback()
        } else {
            val counter = AtomicInteger(allOpen.size)
            allOpen.forEach {
                it.onClose {
                    if (counter.decrementAndGet() == 0) {
                        callback()
                    }
                }
            }
        }
    }

    /**
     * Creates an instance of [DatabaseTransaction], with the given transaction isolation level.
     */
    fun createTransaction(isolationLevel: TransactionIsolationLevel = defaultIsolationLevel): DatabaseTransaction {
        // We need to set the database for the current [Thread] or [Fiber] here as some tests share threads across databases.
        _contextDatabase.set(this)
        return currentOrNew(isolationLevel)
    }

    fun createSession(): Connection {
        // We need to set the database for the current [Thread] or [Fiber] here as some tests share threads across databases.
        _contextDatabase.set(this)
        val transaction = contextTransaction
        try {
            transaction.session.flush()
            return transaction.connection
        } catch (e: Exception) {
            if (e is SQLException || e is PersistenceException) {
                transaction.errorHandler(e)
            }
            throw e
        }
    }

    /**
     * Executes given statement in the scope of transaction, with the given isolation level.
     * @param isolationLevel isolation level for the transaction.
     * @param statement to be executed in the scope of this transaction.
     */
    fun <T> transaction(isolationLevel: TransactionIsolationLevel, useErrorHandler: Boolean, statement: DatabaseTransaction.() -> T): T =
            transaction(isolationLevel, 2, false, useErrorHandler, statement)

    /**
     * Executes given statement in the scope of transaction with the transaction level specified at the creation time.
     * @param statement to be executed in the scope of this transaction.
     */
    @JvmOverloads
    fun <T> transaction(useErrorHandler: Boolean = true, statement: DatabaseTransaction.() -> T): T = transaction(defaultIsolationLevel, useErrorHandler, statement)

    /**
     * Executes given statement in the scope of transaction, with the given isolation level.
     * @param isolationLevel isolation level for the transaction.
     * @param recoverableFailureTolerance number of transaction commit retries for SQL while SQL exception is encountered.
     * @param recoverAnyNestedSQLException retry transaction on any SQL Exception wrapped as a cause of [Throwable].
     * @param statement to be executed in the scope of this transaction.
     */
    fun <T> transaction(isolationLevel: TransactionIsolationLevel, recoverableFailureTolerance: Int,
                        recoverAnyNestedSQLException: Boolean, useErrorHandler: Boolean, statement: DatabaseTransaction.() -> T): T {
        _contextDatabase.set(this)
        val outer = contextTransactionOrNull
        return if (outer != null) {
            // we only need to handle errors coming out of inner transactions because,
            // a. whenever this code is being executed within the flow state machine, a top level transaction should have
            // previously been created by the flow state machine in ActionExecutorImpl#executeCreateTransaction
            // b. exceptions coming out from top level transactions are already being handled in CordaPersistence#inTopLevelTransaction
            // i.e. roll back and close the transaction
            if(useErrorHandler) {
                outer.withErrorHandler(statement)
            } else {
                outer.statement()
            }
        } else {
            inTopLevelTransaction(isolationLevel, recoverableFailureTolerance, recoverAnyNestedSQLException, statement)
        }
    }

    private fun <T> DatabaseTransaction.withErrorHandler(statement: DatabaseTransaction.() -> T): T {
        return try {
            statement()
        } catch (e: Exception) {
            if ((e is SQLException || e is PersistenceException || e is HospitalizeFlowException)) {
                errorHandler(e)
            }
            throw e
        }
    }

    /**
     * Executes given statement in the scope of transaction with the transaction level specified at the creation time.
     * @param statement to be executed in the scope of this transaction.
     * @param recoverableFailureTolerance number of transaction commit retries for SQL while SQL exception is encountered.
     */
    fun <T> transaction(recoverableFailureTolerance: Int, statement: DatabaseTransaction.() -> T): T {
        return transaction(defaultIsolationLevel, recoverableFailureTolerance, false, false, statement)
    }

    private fun <T> inTopLevelTransaction(isolationLevel: TransactionIsolationLevel, recoverableFailureTolerance: Int,
                                          recoverAnyNestedSQLException: Boolean, statement: DatabaseTransaction.() -> T): T {
        var recoverableFailureCount = 0
        fun <T> quietly(task: () -> T) = try {
            task()
        } catch (e: Exception) {
            log.warn("Cleanup task failed:", e)
        }
        while (true) {
            val transaction = contextDatabase.currentOrNew(isolationLevel) // XXX: Does this code really support statement changing the contextDatabase?
            try {
                val answer = transaction.statement()
                transaction.commit()
                return answer
            } catch (e: Exception) {
                quietly(transaction::rollback)
                if (e is SQLException || (recoverAnyNestedSQLException && e.hasSQLExceptionCause())) {
                    if (++recoverableFailureCount > recoverableFailureTolerance) throw e
                    log.warn("Caught failure, will retry $recoverableFailureCount/$recoverableFailureTolerance:", e)
                } else {
                    throw e
                }
            } finally {
                quietly(transaction::close)
            }
        }
    }

    override fun close() {
        // DataSource doesn't implement AutoCloseable so we just have to hope that the implementation does so that we can close it
        val mayBeAutoClosableDataSource = _dataSource as? AutoCloseable
        if(mayBeAutoClosableDataSource != null) {
            log.info("Closing $mayBeAutoClosableDataSource")
            mayBeAutoClosableDataSource.close()
        } else {
            log.warn("$_dataSource has not been properly closed")
        }
    }

    @Suppress("PLATFORM_CLASS_MAPPED_TO_KOTLIN")
    val hikariPoolThreadLocal: ThreadLocal<List<Object>>? by lazy(LazyThreadSafetyMode.PUBLICATION) {
        val hikariDataSource = dataSource as? HikariDataSource
        if (hikariDataSource == null) {
            null
        } else {
            val poolField: Field = HikariDataSource::class.java.getDeclaredField("pool")
            poolField.isAccessible = true
            val pool: HikariPool = poolField.get(hikariDataSource) as HikariPool
            val connectionBagField: Field = HikariPool::class.java.getDeclaredField("connectionBag")
            connectionBagField.isAccessible = true
            @Suppress("UNCHECKED_CAST")
            val connectionBag: ConcurrentBag<ConcurrentBag.IConcurrentBagEntry> = connectionBagField.get(pool) as ConcurrentBag<ConcurrentBag.IConcurrentBagEntry>
            val threadListField: Field = ConcurrentBag::class.java.getDeclaredField("threadList")
            threadListField.isAccessible = true
            @Suppress("UNCHECKED_CAST")
            val threadList: ThreadLocal<List<Object>> = threadListField.get(connectionBag) as ThreadLocal<List<Object>>
            threadList
        }
    }
}

/**
 * Buffer observations until after the current database transaction has been closed.  Observations are never
 * dropped, simply delayed.
 *
 * Primarily for use by component authors to publish observations during database transactions without racing against
 * closing the database transaction.
 *
 * For examples, see the call hierarchy of this function.
 */
fun <T : Any> rx.Observer<T>.bufferUntilDatabaseCommit(propagateRollbackAsError: Boolean = false): rx.Observer<T> {
    val currentTx = contextTransaction
    val subject = UnicastSubject.create<T>()
    val databaseTxBoundary: Observable<CordaPersistence.Boundary> = currentTx.boundary.filter { it.success }
    if (propagateRollbackAsError) {
        currentTx.boundary.filter { !it.success }.subscribe { this.onError(DatabaseTransactionRolledBackException(it.txId)) }
    }
    subject.delaySubscription(databaseTxBoundary).subscribe(this)
    return subject
}

class DatabaseTransactionRolledBackException(txId: UUID) : Exception("Database transaction $txId was rolled back")

// A subscriber that delegates to multiple others, wrapping a database transaction around the combination.
private class DatabaseTransactionWrappingSubscriber<U>(private val db: CordaPersistence?) : Subscriber<U>() {
    // Some unsubscribes happen inside onNext() so need something that supports concurrent modification.
    val delegates = CopyOnWriteArrayList<Subscriber<in U>>()

    fun forEachSubscriberWithDbTx(block: Subscriber<in U>.() -> Unit) {
        (db ?: contextDatabase).transaction {
            delegates.filter { !it.isUnsubscribed }.forEach {
                it.block()
            }
        }
    }

    override fun onCompleted() = forEachSubscriberWithDbTx { onCompleted() }

    override fun onError(e: Throwable?) = forEachSubscriberWithDbTx { onError(e) }

    override fun onNext(s: U) = forEachSubscriberWithDbTx { onNext(s) }

    override fun onStart() = forEachSubscriberWithDbTx { onStart() }

    fun cleanUp() {
        if (delegates.removeIf { it.isUnsubscribed }) {
            if (delegates.isEmpty()) {
                unsubscribe()
            }
        }
    }
}

// A subscriber that wraps another but does not pass on observations to it.
private class NoOpSubscriber<U>(t: Subscriber<in U>) : Subscriber<U>(t) {
    override fun onCompleted() {}
    override fun onError(e: Throwable?) {}
    override fun onNext(s: U) {}
}

/**
 * Wrap delivery of observations in a database transaction.  Multiple subscribers will receive the observations inside
 * the same database transaction.  This also lazily subscribes to the source [rx.Observable] to preserve any buffering
 * that might be in place.
 */
fun <T : Any> rx.Observable<T>.wrapWithDatabaseTransaction(db: CordaPersistence? = null): rx.Observable<T> {
    var wrappingSubscriber = DatabaseTransactionWrappingSubscriber<T>(db)
    // Use lift to add subscribers to a special subscriber that wraps a database transaction around observations.
    // Each subscriber will be passed to this lambda when they subscribe, at which point we add them to wrapping subscriber.
    return this.lift { toBeWrappedInDbTx: Subscriber<in T> ->
        // Add the subscriber to the wrapping subscriber, which will invoke the original subscribers together inside a database transaction.
        wrappingSubscriber.delegates.add(toBeWrappedInDbTx)
        // If we are the first subscriber, return the shared subscriber, otherwise return a subscriber that does nothing.
        if (wrappingSubscriber.delegates.size == 1) wrappingSubscriber else NoOpSubscriber(toBeWrappedInDbTx)
        // Clean up the shared list of subscribers when they unsubscribe.
    }.doOnUnsubscribe {
        wrappingSubscriber.cleanUp()
        // If cleanup removed the last subscriber reset the system, as future subscribers might need the stream again
        if (wrappingSubscriber.delegates.isEmpty()) {
            wrappingSubscriber = DatabaseTransactionWrappingSubscriber(db)
        }
    }
}

/** Check if any nested cause is of [SQLException] type. */
private fun Throwable.hasSQLExceptionCause(): Boolean =
        when (cause) {
            null -> false
            is SQLException -> true
            else -> cause?.hasSQLExceptionCause() ?: false
        }

class CouldNotCreateDataSourceException(override val message: String?,
                                        override val code: NodeDatabaseErrors,
                                        override val parameters: List<Any> = listOf(),
                                        override val cause: Throwable? = null) : ErrorCode<NodeDatabaseErrors>, Exception()

class HibernateSchemaChangeException(override val message: String?, override val cause: Throwable? = null): Exception()

class HibernateConfigException(override val message: String?, override val cause: Throwable? = null): Exception()
