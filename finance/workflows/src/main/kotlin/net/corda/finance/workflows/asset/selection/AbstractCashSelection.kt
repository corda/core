package net.corda.finance.workflows.asset.selection

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.contracts.Amount
import net.corda.core.contracts.StateAndRef
import net.corda.core.contracts.StateRef
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.FlowLogic
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.Party
import net.corda.core.internal.uncheckedCast
import net.corda.core.node.ServiceHub
import net.corda.core.node.services.StatesNotAvailableException
import net.corda.core.utilities.*
import net.corda.finance.contracts.asset.Cash
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet
import java.util.*
import java.util.concurrent.atomic.AtomicReference

/**
 * Pluggable interface to allow for different cash selection provider implementations
 * Default implementation in finance workflow module uses H2 database and a custom function within H2 to perform aggregation.
 * Custom implementations must implement this interface and declare their implementation in
 * `META-INF/services/net.corda.finance.workflows.asset.selection.AbstractCashSelection`.
 */
// TODO: make parameters configurable when we get CorDapp configuration.
abstract class AbstractCashSelection(private val maxRetries : Int = 8, private val retrySleep : Int = 100,
                                     private val retryCap : Int = 2000) {
    companion object {
        val instance = AtomicReference<AbstractCashSelection>()

        fun getInstance(metadata: () -> java.sql.DatabaseMetaData): AbstractCashSelection {
            return instance.get() ?: {
                val metadataLocal = metadata()
                val cashSelectionAlgos = ServiceLoader.load(AbstractCashSelection::class.java, this::class.java.classLoader).toList()
                val cashSelectionAlgo = cashSelectionAlgos.firstOrNull { it.isCompatible(metadataLocal) }
                cashSelectionAlgo?.let {
                    instance.set(cashSelectionAlgo)
                    cashSelectionAlgo
                } ?: throw ClassNotFoundException("\nUnable to load compatible cash selection algorithm implementation for JDBC driver name '${metadataLocal.driverName}'." +
                        "\nPlease specify an implementation in META-INF/services/${AbstractCashSelection::class.qualifiedName}." +
                        "\nAvailable implementations: $cashSelectionAlgos")
            }.invoke()
        }

        private val log = contextLogger()
    }

    /**
     * Upon dynamically loading configured Cash Selection algorithms declared in META-INF/services
     * this method determines whether the loaded implementation is compatible and usable with the currently
     * loaded JDBC driver.
     * Note: the first loaded implementation to pass this check will be used at run-time.
     */
    protected abstract fun isCompatible(metadata: DatabaseMetaData): Boolean

    /**
     * A vendor specific query(ies) to gather Cash states that are available.
     * @param amount The amount of currency desired (ignoring issues, but specifying the currency)
     * @param lockId The FlowLogic.runId.uuid of the flow, which is used to soft reserve the states.
     * Also, previous outputs of the flow will be eligible as they are implicitly locked with this id until the flow completes.
     * @param notary If null the notary source is ignored, if specified then only states marked
     * with this notary are included.
     * @param onlyFromIssuerParties Optional issuer parties to match against.
     * @param withIssuerRefs Optional issuer references to match against.
     * @param withResultSet Function that contains the business logic. The JDBC ResultSet with the matching states that were found. If sufficient funds were found these will be locked,
     * otherwise what is available is returned unlocked for informational purposes.
     * @return The result of the withResultSet function
     */
    protected abstract fun executeQuery(connection: Connection, amount: Amount<Currency>, lockId: UUID, notary: Party?,
                              onlyFromIssuerParties: Set<AbstractParty>, withIssuerRefs: Set<OpaqueBytes>, withResultSet: (ResultSet) -> Boolean): Boolean

    abstract override fun toString(): String

    /**
     * Query to gather Cash states that are available and retry if they are temporarily unavailable.
     * @param services The service hub to allow access to the database session
     * @param amount The amount of currency desired (ignoring issues, but specifying the currency)
     * @param onlyFromIssuerParties If empty the operation ignores the specifics of the issuer,
     * otherwise the set of eligible states wil be filtered to only include those from these issuers.
     * @param notary If null the notary source is ignored, if specified then only states marked
     * with this notary are included.
     * @param lockId The FlowLogic.runId.uuid of the flow, which is used to soft reserve the states.
     * Also, previous outputs of the flow will be eligible as they are implicitly locked with this id until the flow completes.
     * @param withIssuerRefs If not empty the specific set of issuer references to match against.
     * @return The matching states that were found. If sufficient funds were found these will be locked,
     * otherwise what is available is returned unlocked for informational purposes.
     */
    @Suspendable
    fun unconsumedCashStatesForSpending(services: ServiceHub,
                                        amount: Amount<Currency>,
                                        onlyFromIssuerParties: Set<AbstractParty> = emptySet(),
                                        notary: Party? = null,
                                        lockId: UUID,
                                        withIssuerRefs: Set<OpaqueBytes> = emptySet()): List<StateAndRef<Cash.State>> {
        val stateAndRefs = mutableListOf<StateAndRef<Cash.State>>()

        // DOCSTART CASHSELECT 1
        for (retryCount in 1..maxRetries) {
            if (!attemptSpend(services, amount, lockId, notary, onlyFromIssuerParties, withIssuerRefs, stateAndRefs)) {
                log.warn("Coin selection failed on attempt $retryCount")
                // TODO: revisit the back off strategy for contended spending.
                if (retryCount != maxRetries) {
                    stateAndRefs.clear()
                    val durationMillis = (minOf(retrySleep.shl(retryCount), retryCap / 2) * (1.0 + Math.random())).toInt()
                    FlowLogic.sleep(durationMillis.millis)
                } else {
                    log.warn("Insufficient spendable states identified for $amount")
                }
            } else {
                break
            }
        }
        // DOCEND CASHSELECT 1
        return stateAndRefs
    }

    private fun attemptSpend(services: ServiceHub, amount: Amount<Currency>, lockId: UUID, notary: Party?, onlyFromIssuerParties: Set<AbstractParty>, withIssuerRefs: Set<OpaqueBytes>, stateAndRefs: MutableList<StateAndRef<Cash.State>>): Boolean {
        val connection = services.jdbcSession()
        try {
            // we select spendable states irrespective of lock but prioritised by unlocked ones (Eg. null)
            // the softLockReserve update will detect whether we try to lock states locked by others
            return executeQuery(connection, amount, lockId, notary, onlyFromIssuerParties, withIssuerRefs) { rs ->
                stateAndRefs.clear()

                var totalPennies = 0L
                val stateRefs = mutableSetOf<StateRef>()
                while (rs.next()) {
                    val txHash = SecureHash.create(rs.getString(1))
                    val index = rs.getInt(2)
                    val pennies = rs.getLong(3)
                    totalPennies = rs.getLong(4)
                    val rowLockId = rs.getString(5)
                    stateRefs.add(StateRef(txHash, index))
                    log.trace { "ROW: $rowLockId ($lockId): ${StateRef(txHash, index)} : $pennies ($totalPennies)" }
                }

                if (stateRefs.isNotEmpty()) {
                    // TODO: future implementation to retrieve contract states from a Vault BLOB store
                    stateAndRefs.addAll(uncheckedCast(services.loadStates(stateRefs)))
                }

                val success = stateAndRefs.isNotEmpty() && totalPennies >= amount.quantity
                if (success) {
                    // we should have a minimum number of states to satisfy our selection `amount` criteria
                    log.trace("Coin selection for $amount retrieved ${stateAndRefs.count()} states totalling $totalPennies pennies: $stateAndRefs")

                    // With the current single threaded state machine available states are guaranteed to lock.
                    // TODO However, we will have to revisit these methods in the future multi-threaded.
                    services.vaultService.softLockReserve(lockId, (stateAndRefs.map { it.ref }).toNonEmptySet())
                } else {
                    log.trace("Coin selection requested $amount but retrieved $totalPennies pennies with state refs: ${stateAndRefs.map { it.ref }}")
                }
                success
            }

            // retry as more states may become available
        } catch (e: StatesNotAvailableException) { // Should never happen with single threaded state machine
            log.warn(e.message)
            // retry only if there are locked states that may become available again (or consumed with change)
        }
        return false
    }
}
