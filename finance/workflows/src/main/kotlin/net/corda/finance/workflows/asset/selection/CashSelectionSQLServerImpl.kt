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

class CashSelectionSQLServerImpl : AbstractCashSelection() {
    companion object {
        val JDBC_DRIVER_NAME_REGEX = """Microsoft JDBC Driver (\w+.\w+) for SQL Server""".toRegex()
        private val log = contextLogger()
    }

    override fun isCompatible(metadata: DatabaseMetaData): Boolean {
        return  JDBC_DRIVER_NAME_REGEX.matches(metadata.driverName)
    }

    override fun toString() = "${this::class.qualifiedName} for '$JDBC_DRIVER_NAME_REGEX'"

    //      This is one MSSQL implementation of the query to select just enough cash states to meet the desired amount.
    //      We select the cash states with smaller amounts first so that as the result, we minimize the numbers of
    //      unspent cash states remaining in the vault.
    //
    //      If there is not enough cash, the query will return an empty resultset, which should signal to the caller
    //      of an exception, since the desired amount is assumed to always > 0.
    //      NOTE: The other two implementations, H2 and PostgresSQL, behave differently in this case - they return
    //      all in the vault instead of nothing. That seems to give the caller an extra burden to verify total returned
    //      >= amount.
    //      In addition, extra data fetched results in unnecessary I/O.
    //      Nevertheless, if so desired, we can achieve the same by changing the last FROM clause to
    //          FROM CTE LEFT JOIN Boundary AS B ON 1 = 1
    //          WHERE B.seqNo IS NULL OR CTE.seqNo <= B.seqNo
    //
    //      Common Table Expression and Windowed functions help make the query more readable.
    //      Query plan does index scan on pennies_idx, which may be unavoidable due to the nature of the query.
    override fun executeQuery(connection: Connection, amount: Amount<Currency>, lockId: UUID, notary: Party?, onlyFromIssuerParties: Set<AbstractParty>, withIssuerRefs: Set<OpaqueBytes>, maxVersion: Int, withResultSet: (ResultSet) -> Boolean): Boolean {
        val sb = StringBuilder()
        // state_status = 0 -> UNCONSUMED.
        // is_relevant = 0 -> RELEVANT.
        sb.append( """
            ;WITH CTE AS
            (
            SELECT
              vs.transaction_id,
              vs.output_index,
              ccs.pennies,
              vs.lock_id,
              total_pennies = SUM(ccs.pennies) OVER (ORDER BY ccs.pennies),
              seqNo = ROW_NUMBER() OVER (ORDER BY ccs.pennies)
            FROM vault_states AS vs INNER JOIN contract_cash_states AS ccs
                ON vs.transaction_id = ccs.transaction_id AND vs.output_index = ccs.output_index
            WHERE
              vs.state_status = 0
              AND vs.relevancy_status = 0
              AND ccs.ccy_code = ?
              AND (vs.lock_id = ? OR vs.lock_id IS NULL)
            """
        )
        if (notary != null)
            sb.append("""
              AND vs.notary_name = ?
            """)
        if (onlyFromIssuerParties.isNotEmpty()) {
            val repeats = generateSequence { "?" }.take(onlyFromIssuerParties.size).joinToString(",")
            sb.append("""
              AND ccs.issuer_key_hash IN ($repeats)
            """)
        }
        if (withIssuerRefs.isNotEmpty()) {
            val repeats = generateSequence { "?" }.take(withIssuerRefs.size).joinToString(",")
            sb.append("""
              AND ccs.issuer_ref IN ($repeats)
            """)
        }
        sb.append(
            """
            ),
            Boundary AS
            (
              SELECT TOP (1) * FROM  CTE WHERE total_pennies >= ? ORDER BY seqNo
            )
            SELECT CTE.transaction_id, CTE.output_index, CTE.pennies, CTE.total_pennies, CTE.lock_id
              FROM CTE INNER JOIN Boundary AS B ON CTE.seqNo <= B.seqNo
            ;
            """
        )
        val selectJoin = sb.toString()
        log.debug { selectJoin }
        connection.prepareStatement(selectJoin).use { psSelectJoin ->
            var pIndex = 0
            psSelectJoin.setString(++pIndex, amount.token.currencyCode)
            psSelectJoin.setString(++pIndex, lockId.toString())
            if (notary != null)
                psSelectJoin.setString(++pIndex, notary.name.toString())
            onlyFromIssuerParties.forEach {
                psSelectJoin.setString(++pIndex, it.owningKey.toStringShort())
            }
            withIssuerRefs.forEach {
                psSelectJoin.setBytes(++pIndex, it.bytes)
            }
            psSelectJoin.setLong(++pIndex, amount.quantity)

            log.debug { psSelectJoin.toString() }

            psSelectJoin.executeQuery().use { rs ->
                return withResultSet(rs)
            }
        }
    }
}