package net.corda.avalanche

import picocli.CommandLine
import java.io.File
import java.util.*
import kotlin.collections.LinkedHashMap

fun main(args: Array<String>) {

    val parameters = Parameters()
    CommandLine(parameters).parseArgs(*args)
    if (parameters.helpRequested) {
        CommandLine.usage(Parameters(), System.out)
        return
    }

    val network = Network(parameters)
    val n1 = network.nodes[0]
    val c1 = mutableListOf<Transaction>()
    val c2 = mutableListOf<Transaction>()

    repeat(parameters.nrTransactions) {
        val n = network.nodes.shuffled(network.rng).first()
        c1.add(n.onGenerateTx(it))
        if (network.rng.nextDouble() < parameters.doubleSpendRatio) {
            val d = network.rng.nextInt(it)
            println("double spend of $d")
            val n2 = network.nodes.shuffled(network.rng).first()
            c2.add(n2.onGenerateTx(d))
        }

        network.run()

        if (parameters.dumpDags) {
            n1.dumpDag(File("node-0-${String.format("%03d", it)}.dot"))
        }
        println("$it: " + String.format("%.3f", fractionAccepted(n1)))
    }

    val conflictSets = (c1 + c2).groupBy { it.data }.filterValues { it.size > 1 }
    conflictSets.forEach { v, txs ->
        val acceptance = txs.map { t -> network.nodes.map { it.isAccepted(t) }.any { it } }
        require(acceptance.filter { it }.size < 2) { "More than one transaction of the conflict set of $v got accepted." }
    }
}

fun fractionAccepted(n: Node): Double {
    val accepted = n.transactions.values.filter { n.isAccepted(it) }.size
    return accepted.toDouble() / n.transactions.size
}

data class Transaction(
        val id: UUID,
        val data: Int,
        val parents: List<UUID>,
        var chit: Int = 0,
        var confidence: Int = 0) {
    override fun toString(): String {
        return "T(id=${id.toString().take(5)}, data=$data, parents=[${parents.map {it.toString().take(5) }}, chit=$chit, confidence=$confidence)"
    }
}

data class ConflictSet(
        var pref: Transaction,
        var last: Transaction,
        var count: Int,
        var size: Int
)

class Network(val parameters: Parameters) {
    val rng = Random(parameters.seed)
    val tx = Transaction(UUID.randomUUID(), -1, emptyList(), 1)
    val nodes = (0..parameters.nrNodes).map { Node(it, parameters, tx.copy(),this, rng) }
    fun run() {
        nodes.forEach { it.avalancheLoop() }
    }
}

class Node(val id: Int, parameters: Parameters, val genesisTx: Transaction, val network: Network, val rng: Random) {

    val alpha = parameters.alpha
    val k  = parameters.k
    val beta1 = parameters.beta1
    val beta2 = parameters.beta2

    val transactions = LinkedHashMap<UUID, Transaction>(mapOf(genesisTx.id to genesisTx))
    val queried = mutableSetOf<UUID>(genesisTx.id)
    val conflicts = mutableMapOf<Int, ConflictSet>(genesisTx.data to ConflictSet(genesisTx, genesisTx, 0, 1))

    val accepted = mutableSetOf<UUID>(genesisTx.id)
    val parentSets = mutableMapOf<UUID, Set<Transaction>>()

    fun onGenerateTx(data: Int): Transaction {
       val edges = parentSelection()
       val t = Transaction(UUID.randomUUID(), data, edges.map { it.id })
        onReceiveTx(this, t)
        return t
    }

    fun onReceiveTx(sender: Node, tx: Transaction) {
        if (transactions.contains(tx.id)) return
        tx.chit = 0
        tx.confidence = 0

        tx.parents.forEach {
            if (!transactions.contains(it)) {
                val t = sender.onSendTx(it)
                onReceiveTx(sender, t)
            }
        }

        if (!conflicts.contains(tx.data)) {
            conflicts[tx.data] = ConflictSet(tx, tx, 0, 1)
        } else {
            conflicts[tx.data]!!.size++
        }

        transactions[tx.id] = tx
    }

    fun onSendTx(id: UUID): Transaction {
        return transactions[id]!!.copy()
    }

    fun onQuery(sender: Node, tx: Transaction): Int {
        onReceiveTx(sender, tx)
        return if (isStronglyPreferred(tx)) 1
               else 0
    }

    fun avalancheLoop() {
        val txs = transactions.values.filterNot { queried.contains(it.id) }
        txs.forEach { tx ->
            val sample = network.nodes.filterNot { it == this }.shuffled(rng).take(k)
            val res = sample.map {
                val txCopy = tx.copy()
                it.onQuery(this, txCopy)
            }.sum()
            if (res >= alpha * k) {
                tx.chit = 1
                // Update the preference for ancestors.
                parentSet(tx).forEach { p ->
                    p.confidence += 1
                }
                parentSet(tx).forEach { p->
                    val cs = conflicts[p.data]!!
                    if (p.confidence > cs.pref.confidence) {
                       cs.pref = p
                    }
                    if (p != cs.last) {
                        cs.last = p
                        cs.count = 0
                    } else {
                        cs.count++
                    }
                }
            }
            queried.add(tx.id)
        }
    }

    fun isPreferred(tx: Transaction): Boolean {
        return conflicts[tx.data]!!.pref == tx
    }

    fun isStronglyPreferred(tx: Transaction): Boolean {
        return parentSet(tx).map { isPreferred(it) }.all { it }
    }

    fun isAccepted(tx: Transaction): Boolean {
        if (accepted.contains(tx.id)) return true
        if (!queried.contains(tx.id)) return false

        val cs = conflicts[tx.data]!!
        val parentsAccepted = tx.parents.map { accepted.contains(it) }.all { it }
        val isAccepted = (parentsAccepted && cs.size == 1 && tx.confidence > beta1) ||
                (cs.pref == tx && cs.count > beta2)
        if (isAccepted) accepted.add(tx.id)
        return isAccepted
    }

    fun parentSet(tx: Transaction): Set<Transaction> {

        if (parentSets.contains(tx.id)) return parentSets[tx.id]!!

        val parents = mutableSetOf<Transaction>()
        var ps = tx.parents.toSet()
        while (ps.isNotEmpty()) {
            ps.forEach {
                if (transactions.contains(it)) parents.add(transactions[it]!!)
            }
            ps = ps.flatMap {
                if (transactions.contains(it)) {
                    transactions[it]!!.parents
                } else {
                    emptyList()
                }
            }.toSet()
        }
        parentSets[tx.id] = parents
        return parents
    }

    fun parentSelection(): List<Transaction> {
        val eps0 = transactions.values.filter { isStronglyPreferred(it) }
        val eps1 = eps0.filter { conflicts[it.data]!!.size == 1 || it.confidence > 0 }
        val parents = eps1.flatMap { parentSet(it) }.toSet().filterNot { eps1.contains(it) }
        val fallback = if (transactions.size == 1) listOf(genesisTx)
                        else transactions.values.reversed().take(10).filter { !isAccepted(it) && conflicts[it.data]!!.size == 1 }.shuffled(network.rng).take(3)
        require(parents.isNotEmpty() || fallback.isNotEmpty()) { "Unable to select parents." }
        return if (parents.isEmpty()) return fallback else parents
    }

    fun dumpDag(f: File) {
        f.printWriter().use { out ->
            out.println("digraph G {")
            transactions.values.forEach {
                val color = if (isAccepted(it)) "color=lightblue; style=filled;" else ""
                val pref = if (conflicts[it.data]!!.size > 1 && isPreferred(it)) "*" else ""
                val chit = if (queried.contains(it.id)) it.chit.toString() else "?"
                out.println("\"${it.id}\" [$color label=\"${it.data}$pref, $chit, ${it.confidence}\"];")
            }
            transactions.values.forEach {
                it.parents.forEach { p->
                    out.println("\"${it.id}\" -> \"$p\";")
                }
            }
            out.println("}")
        }
    }
}
