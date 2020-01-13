package net.corda.finance.workflows.asset.selection

import net.corda.core.contracts.Amount
import net.corda.core.crypto.toStringShort
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.Party
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.debug
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet
import java.util.*

class CashSelectionPostgreSQLImpl : AbstractCashSelection() {

    companion object {
        const val JDBC_DRIVER_NAME = "PostgreSQL JDBC Driver"
        private val log = contextLogger()
    }

    override fun isCompatible(metadata: DatabaseMetaData): Boolean {
        return metadata.driverName == JDBC_DRIVER_NAME
    }

    override fun toString() = "${this::class.qualifiedName} for '$JDBC_DRIVER_NAME'"

    //       This is using PostgreSQL window functions for selecting a minimum set of rows that match a request amount of coins:
    //       1) This may also be possible with user-defined functions (e.g. using PL/pgSQL)
    //       2) The window function accumulated column (`total`) does not include the current row (starts from 0) and cannot
    //          appear in the WHERE clause, hence restricting row selection and adjusting the returned total in the outer query.
    //       3) Currently (version 9.6), FOR UPDATE cannot be specified with window functions
    override fun executeQuery(connection: Connection, amount: Amount<Currency>, lockId: UUID, notary: Party?, onlyFromIssuerParties: Set<AbstractParty>, withIssuerRefs: Set<OpaqueBytes>, maxVersion: Int, withResultSet: (ResultSet) -> Boolean): Boolean {
        // state_status = 0 -> UNCONSUMED.
        // is_relevant = 0 -> RELEVANT.
        val selectJoin = """SELECT nested.transaction_id, nested.output_index, nested.pennies,
                        nested.total+nested.pennies as total_pennies, nested.lock_id
                       FROM
                       (SELECT vs.transaction_id, vs.output_index, ccs.pennies,
                       coalesce((SUM(ccs.pennies) OVER (PARTITION BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)), 0)
                       AS total, vs.lock_id
                        FROM vault_states AS vs, contract_cash_states AS ccs
                        WHERE vs.transaction_id = ccs.transaction_id AND vs.output_index = ccs.output_index
                        AND vs.state_status = 0
                        AND vs.relevancy_status = 0
                        AND ccs.ccy_code = ?
                        AND (vs.lock_id = ? OR vs.lock_id is null)
                        """ +
                (if (notary != null)
                    " AND vs.notary_name = ?" else "") +
                (if (onlyFromIssuerParties.isNotEmpty())
                    " AND ccs.issuer_key_hash = ANY (?)" else "") +
                (if (withIssuerRefs.isNotEmpty()) {
                    val repeats = generateSequence { "?" }
                            .take(withIssuerRefs.size)
                            .joinToString(",")
                    " AND ccs.issuer_ref IN ($repeats)"
                } else "") +
                """)
                        nested WHERE nested.total < ?
                     """

        connection.prepareStatement(selectJoin).use { statement ->
            statement.setString(1, amount.token.toString())
            statement.setString(2, lockId.toString())
            var paramOffset = 0
            if (notary != null) {
                statement.setString(3, notary.name.toString())
                paramOffset += 1
            }
            if (onlyFromIssuerParties.isNotEmpty()) {
                val issuerKeys = connection.createArrayOf("VARCHAR", onlyFromIssuerParties.map
                { it.owningKey.toStringShort() }.toTypedArray())
                statement.setArray(3 + paramOffset, issuerKeys)
                paramOffset += 1
            }
            withIssuerRefs.map { it.bytes }.forEach {
                statement.setBytes(3 + paramOffset, it)
                paramOffset += 1
            }
            statement.setLong(3 + paramOffset, amount.quantity)
            log.debug { statement.toString() }

            statement.executeQuery().use { rs ->
                return withResultSet(rs)
            }
        }
    }
}
