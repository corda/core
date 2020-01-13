package net.corda.finance.workflows.asset

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.contracts.*
import net.corda.core.contracts.Amount.Companion.sumOrThrow
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.node.ServiceHub
import net.corda.core.transactions.TransactionBuilder
import net.corda.finance.contracts.asset.Cash
import net.corda.finance.contracts.asset.OnLedgerAsset
import net.corda.finance.contracts.asset.PartyAndAmount
import net.corda.finance.workflows.asset.selection.AbstractCashSelection
import java.security.PublicKey
import java.util.*

object CashUtils {
    /**
     * Generate a transaction that moves an amount of currency to the given party, and sends any change back to
     * sole identity of the calling node. Fails for nodes with multiple identities.
     *
     * Note: an [Amount] of [Currency] is only fungible for a given Issuer Party within a [FungibleAsset]
     *
     * @param services The [ServiceHub] to provide access to the database session.
     * @param tx A builder, which may contain inputs, outputs and commands already. The relevant components needed
     *           to move the cash will be added on top.
     * @param amount How much currency to send.
     * @param to the recipient party.
     * @param onlyFromParties if non-null, the asset states will be filtered to only include those issued by the set
     *                        of given parties. This can be useful if the party you're trying to pay has expectations
     *                        about which type of asset claims they are willing to accept.
     * @return A [Pair] of the same transaction builder passed in as [tx], and the list of keys that need to sign
     *         the resulting transaction for it to be valid.
     * @throws InsufficientBalanceException when a cash spending transaction fails because
     *         there is insufficient quantity for a given currency (and optionally set of Issuer Parties).
     */
    @JvmStatic
    @Throws(InsufficientBalanceException::class)
    @Suspendable
    @Deprecated("Our identity should be specified", replaceWith = ReplaceWith("generateSpend(services, tx, amount, to, ourIdentity, onlyFromParties)"))
    fun generateSpend(services: ServiceHub,
                      tx: TransactionBuilder,
                      amount: Amount<Currency>,
                      to: AbstractParty,
                      onlyFromParties: Set<AbstractParty> = emptySet()): Pair<TransactionBuilder, List<PublicKey>> {
        return generateSpend(services, tx, listOf(PartyAndAmount(to, amount)), services.myInfo.legalIdentitiesAndCerts.single(), onlyFromParties)
    }

    /**
     * Generate a transaction that moves an amount of currency to the given party.
     *
     * Note: an [Amount] of [Currency] is only fungible for a given Issuer Party within a [FungibleAsset]
     *
     * @param services The [ServiceHub] to provide access to the database session.
     * @param tx A builder, which may contain inputs, outputs and commands already. The relevant components needed
     *           to move the cash will be added on top.
     * @param amount How much currency to send.
     * @param to the recipient party.
     * @param ourIdentity ourIdentity is used to determine the where the change will be sent.
     *                    If anonymous is true then an anonymous identity will be generated from this and the change
     *                    will be spent to that, otherwise ourIdentity will be used as is.
     * @param onlyFromParties if non-null, the asset states will be filtered to only include those issued by the set
     *                        of given parties. This can be useful if the party you're trying to pay has expectations
     *                        about which type of asset claims they are willing to accept.
     * @param anonymous whether or not to use CI to send the change to
     * @return A [Pair] of the same transaction builder passed in as [tx], and the list of keys that need to sign
     *         the resulting transaction for it to be valid.
     * @throws InsufficientBalanceException when a cash spending transaction fails because
     *         there is insufficient quantity for a given currency (and optionally set of Issuer Parties).
     */
    @JvmStatic
    @JvmOverloads
    @Throws(InsufficientBalanceException::class)
    @Suspendable
    fun generateSpend(services: ServiceHub,
                      tx: TransactionBuilder,
                      amount: Amount<Currency>,
                      ourIdentity: PartyAndCertificate,
                      to: AbstractParty,
                      onlyFromParties: Set<AbstractParty> = emptySet(),
                      anonymous: Boolean = true,
                      maxVersion: Int = 0): Pair<TransactionBuilder, List<PublicKey>> {
        return generateSpend(services, tx, listOf(PartyAndAmount(to, amount)), ourIdentity, onlyFromParties, anonymous, maxVersion)
    }

    /**
     * Generate a transaction that moves money of the given amounts to the recipients specified, and sends any change
     * back to sole identity of the calling node. Fails for nodes with multiple identities.
     *
     * Note: an [Amount] of [Currency] is only fungible for a given Issuer Party within a [FungibleAsset]
     *
     * @param services The [ServiceHub] to provide access to the database session.
     * @param tx A builder, which may contain inputs, outputs and commands already. The relevant components needed
     *           to move the cash will be added on top.
     * @param payments A list of amounts to pay, and the party to send the payment to.
     * @param onlyFromParties if non-null, the asset states will be filtered to only include those issued by the set
     *                        of given parties. This can be useful if the party you're trying to pay has expectations
     *                        about which type of asset claims they are willing to accept.
     * @return A [Pair] of the same transaction builder passed in as [tx], and the list of keys that need to sign
     *         the resulting transaction for it to be valid.
     * @throws InsufficientBalanceException when a cash spending transaction fails because
     *         there is insufficient quantity for a given currency (and optionally set of Issuer Parties).
     */
    @JvmStatic
    @Throws(InsufficientBalanceException::class)
    @Suspendable
    @Deprecated("Our identity should be specified", replaceWith = ReplaceWith("generateSpend(services, tx, amount, to, ourIdentity, onlyFromParties)"))
    fun generateSpend(services: ServiceHub,
                      tx: TransactionBuilder,
                      payments: List<PartyAndAmount<Currency>>,
                      onlyFromParties: Set<AbstractParty> = emptySet()): Pair<TransactionBuilder, List<PublicKey>> {
        return generateSpend(services, tx, payments, services.myInfo.legalIdentitiesAndCerts.single(), onlyFromParties)
    }

    /**
     * Generate a transaction that moves money of the given amounts to the recipients specified.
     *
     * Note: an [Amount] of [Currency] is only fungible for a given Issuer Party within a [FungibleAsset]
     *
     * @param services The [ServiceHub] to provide access to the database session.
     * @param tx A builder, which may contain inputs, outputs and commands already. The relevant components needed
     *           to move the cash will be added on top.
     * @param payments A list of amounts to pay, and the party to send the payment to.
     * @param ourIdentity ourIdentity is used to determine the where the change will be sent.
     *                    If anonymous is true then an anonymous identity will be generated from this and the change
     *                    will be spent to that, otherwise ourIdentity will be used as is.
     * @param onlyFromParties if non-null, the asset states will be filtered to only include those issued by the set
     *                        of given parties. This can be useful if the party you're trying to pay has expectations
     *                        about which type of asset claims they are willing to accept.
     * @param anonymous whether or not to use CI to send the change to
     * @return A [Pair] of the same transaction builder passed in as [tx], and the list of keys that need to sign
     *         the resulting transaction for it to be valid.
     * @throws InsufficientBalanceException when a cash spending transaction fails because
     *         there is insufficient quantity for a given currency (and optionally set of Issuer Parties).
     */
    @JvmStatic
    @JvmOverloads
    @Throws(InsufficientBalanceException::class)
    @Suspendable
    fun generateSpend(services: ServiceHub,
                      tx: TransactionBuilder,
                      payments: List<PartyAndAmount<Currency>>,
                      ourIdentity: PartyAndCertificate,
                      onlyFromParties: Set<AbstractParty> = emptySet(),
                      anonymous: Boolean = true,
                      maxVersion: Int = 0): Pair<TransactionBuilder, List<PublicKey>> {

        fun deriveState(txState: TransactionState<Cash.State>, amt: Amount<Issued<Currency>>, owner: AbstractParty): TransactionState<Cash.State> {
            return txState.copy(data = txState.data.copy(amount = amt, owner = owner))
        }

        // Retrieve unspent and unlocked cash states that meet our spending criteria.
        val totalAmount = payments.map { it.amount }.sumOrThrow()
        val cashSelection = AbstractCashSelection.getInstance { services.jdbcSession().metaData }
        val acceptableCoins = cashSelection.unconsumedCashStatesForSpending(services, totalAmount, onlyFromParties, tx.notary, tx.lockId, maxVersion = maxVersion)
        val revocationEnabled = false // Revocation is currently unsupported
        // If anonymous is true, generate a new identity that change will be sent to for confidentiality purposes. This means that a
        // third party with a copy of the transaction (such as the notary) cannot identify who the change was
        // sent to
        val changeIdentity: AbstractParty = if (anonymous) services.keyManagementService.freshKeyAndCert(ourIdentity, revocationEnabled).party.anonymise() else ourIdentity.party
        return OnLedgerAsset.generateSpend(
                tx,
                payments,
                acceptableCoins,
                changeIdentity,
                ::deriveState,
                Cash()::generateMoveCommand
        )
    }
}

