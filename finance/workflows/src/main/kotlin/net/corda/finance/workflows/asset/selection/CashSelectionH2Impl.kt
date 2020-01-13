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

class CashSelectionH2Impl : AbstractCashSelection() {
    companion object {
        const val JDBC_DRIVER_NAME = "H2 JDBC Driver"
        private val log = contextLogger()
    }

    override fun isCompatible(metadata: DatabaseMetaData): Boolean {
        return metadata.driverName == JDBC_DRIVER_NAME
    }

    override fun toString() = "${this::class.qualifiedName} for '$JDBC_DRIVER_NAME'"

    //       We are using an H2 specific means of selecting a minimum set of rows that match a request amount of coins:
    //       1) There is no standard SQL mechanism of calculating a cumulative total on a field and restricting row selection on the
    //          running total of such an accumulator
    //       2) H2 uses session variables to perform this accumulator function:
    //          http://www.h2database.com/html/functions.html#set
    //       3) H2 does not support JOIN's in FOR UPDATE (hence we are forced to execute 2 queries)
    override fun executeQuery(connection: Connection, amount: Amount<Currency>, lockId: UUID, notary: Party?, onlyFromIssuerParties: Set<AbstractParty>, withIssuerRefs: Set<OpaqueBytes>, maxVersion: Int, withResultSet: (ResultSet) -> Boolean): Boolean {
        connection.createStatement().use { it.execute("CALL SET(@t, CAST(0 AS BIGINT));") }

        // state_status = 0 -> UNCONSUMED.
        // is_relevant = 0 -> RELEVANT.
        val selectJoin = """
                    SELECT vs.transaction_id, vs.output_index, ccs.pennies, SET(@t, ifnull(@t,0)+ccs.pennies) total_pennies, vs.lock_id, vs.tx_version
                    FROM vault_states AS vs, contract_cash_states AS ccs
                    WHERE vs.transaction_id = ccs.transaction_id AND vs.output_index = ccs.output_index
                    AND vs.state_status = 0
                    AND vs.relevancy_status = 0
                    AND ccs.ccy_code = ? and @t < ?
                    AND (vs.lock_id = ? OR vs.lock_id is null)
                    """ +
                (if (notary != null)
                    " AND vs.notary_name = ?" else "") +
                (if (onlyFromIssuerParties.isNotEmpty()) {
                    val repeats = generateSequence { "?" }.take(onlyFromIssuerParties.size).joinToString(",")
                    " AND ccs.issuer_key_hash IN ($repeats)"
                } else "") +
                (if (withIssuerRefs.isNotEmpty()) {
                    val repeats = generateSequence { "?" }.take(withIssuerRefs.size).joinToString(",")
                    " AND ccs.issuer_ref IN ($repeats)"
                } else "") +
                (if (maxVersion > 0) {
                    """ AND vs.tx_version <= ? """
                } else {
                    ""
                })

        // Use prepared statement for protection against SQL Injection (http://www.h2database.com/html/advanced.html#sql_injection)
        connection.prepareStatement(selectJoin).use { psSelectJoin ->
            var pIndex = 0
            psSelectJoin.setString(++pIndex, amount.token.currencyCode)
            psSelectJoin.setLong(++pIndex, amount.quantity)
            psSelectJoin.setString(++pIndex, lockId.toString())
            if (notary != null)
                psSelectJoin.setString(++pIndex, notary.name.toString())
            onlyFromIssuerParties.forEach {
                psSelectJoin.setString(++pIndex, it.owningKey.toStringShort())
            }
            withIssuerRefs.forEach {
                psSelectJoin.setBytes(++pIndex, it.bytes)
            }
            if (maxVersion > 0) {
                psSelectJoin.setInt(++pIndex, maxVersion)
            }
            log.debug { psSelectJoin.toString() }

            psSelectJoin.executeQuery().use { rs ->
                return withResultSet(rs)
            }
        }
    }
}