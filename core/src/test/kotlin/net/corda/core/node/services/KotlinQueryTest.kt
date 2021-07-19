package net.corda.core.node.services

import net.corda.core.contracts.ContractState
import net.corda.core.contracts.StateAndRef
import net.corda.core.contracts.StateRef
import net.corda.core.contracts.TransactionState
import net.corda.core.crypto.Crypto
import net.corda.core.crypto.SecureHash
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.node.services.vault.DEFAULT_PAGE_SIZE
import net.corda.core.node.services.vault.PageSpecification
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(Parameterized::class)
class KotlinQueryTest(numberOfResults : Int, private val pageSize : Int) {

    companion object {
        @JvmStatic
        @Parameterized.Parameters(name = "Number of results: {0}, page size: {1}")
        fun data(): Collection<Array<Any>> {
            return listOf(
                arrayOf<Any>(123, DEFAULT_PAGE_SIZE),
                arrayOf<Any>(123, 100),
                arrayOf<Any>(123, 10),
                arrayOf<Any>(123, 1),
                arrayOf<Any>(10, DEFAULT_PAGE_SIZE),
                arrayOf<Any>(10, 100),
                arrayOf<Any>(10, 10),
                arrayOf<Any>(10, 1),
                arrayOf<Any>(1, DEFAULT_PAGE_SIZE),
                arrayOf<Any>(1, 100),
                arrayOf<Any>(1, 10),
                arrayOf<Any>(1, 1),
                arrayOf<Any>(0, DEFAULT_PAGE_SIZE),
                arrayOf<Any>(0, 100),
                arrayOf<Any>(0, 10),
                arrayOf<Any>(0, 1)
            )
        }
    }

    private data class DummyState(val id : Int, override val participants: List<AbstractParty> = emptyList()) : ContractState

    private val testNotary = Party(CordaX500Name("testNotary", "London", "GB"),
            Crypto.generateKeyPair(Crypto.DEFAULT_SIGNATURE_SCHEME).public)
    private val states = (0 until numberOfResults).map { DummyState(it) }.toList()

    var queryByInvocations = 0
    private fun queryFunction(pageSpecification: PageSpecification): Vault.Page<DummyState> {
        ++queryByInvocations
        return Vault.Page(
                states = states.asSequence()
                        .take(pageSpecification.pageNumber * pageSpecification.pageSize)
                        .drop((pageSpecification.pageNumber - 1) * pageSpecification.pageSize)
                        .map {
                            StateAndRef(
                                    TransactionState(it, "", notary = testNotary, encumbrance = null),
                                    StateRef(SecureHash.zeroHash, 0)
                            )
                        }.toList(),
                statesMetadata = emptyList(),
                totalStatesAvailable = states.size.toLong(),
                stateTypes = Vault.StateStatus.ALL,
                otherResults = emptyList()
        )
    }

    /**
     * check that all available results are returned and the minimum possible number of calls to the database is performed
     */
    @Test
    fun test() {
        var counter = 0
        queryLazy(pageSize, ::queryFunction).asSequence().forEach {
            Assert.assertTrue("Expected state number ${counter}, but got state ${it.state.data.id}", (states[counter]) === it.state.data)
            ++counter
        }
        Assert.assertEquals(states.size, counter)
        //At least one invocation is necessary to get total number of results
        Assert.assertEquals(Math.max(1, (states.size + pageSize - 1) / pageSize), queryByInvocations)
    }
}