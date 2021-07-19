package net.corda.core.crypto

import net.corda.core.KeepForDJVM
import java.util.*

/**
 * Creation and verification of a Merkle tree for a [WireTransaction].
 *
 * See: https://en.wikipedia.org/wiki/Merkle_tree
 *
 * Transaction is split into following blocks: inputs, attachments' refs, outputs, commands, notary,
 * signers, tx type, time-window. Merkle Tree is kept in a recursive data structure. Building is done bottom up,
 * from all leaves' hashes. If number of leaves is not a power of two, the tree is padded with zero hashes.
 */
sealed class MerkleTree {
    abstract val hash: SecureHash

    @KeepForDJVM data class Leaf(override val hash: SecureHash) : MerkleTree()
    @KeepForDJVM data class Node(override val hash: SecureHash, val left: MerkleTree, val right: MerkleTree) : MerkleTree()

    companion object {
        private fun isPow2(num: Int): Boolean = num and (num - 1) == 0

        /**
         * Merkle tree building using hashes, with zero hash padding to full power of 2.
         */
        @Throws(MerkleTreeException::class)
        fun getMerkleTree(allLeavesHashes: List<SecureHash>): MerkleTree {
            if (allLeavesHashes.isEmpty())
                throw MerkleTreeException("Cannot calculate Merkle root on empty hash list.")
            val algorithms = allLeavesHashes.mapTo(HashSet(), SecureHash::algorithm)
            require(algorithms.size == 1) {
                "Cannot build Merkle tree with multiple hash algorithms: $algorithms"
            }
            val leaves = padWithZeros(allLeavesHashes).map { Leaf(it) }
            return buildMerkleTree(leaves)
        }

        // If number of leaves in the tree is not a power of 2, we need to pad it with zero hashes.
        private fun padWithZeros(allLeavesHashes: List<SecureHash>): List<SecureHash> {
            var n = allLeavesHashes.size
            if (isPow2(n)) return allLeavesHashes
            val paddedHashes = ArrayList(allLeavesHashes)
            val zeroHash = SecureHash.zeroHashFor(paddedHashes[0].algorithm)
            while (!isPow2(n++)) {
                paddedHashes.add(zeroHash)
            }
            return paddedHashes
        }

        /**
         * Tailrecursive function for building a tree bottom up.
         * @param lastNodesList MerkleTree nodes from previous level.
         * @return Tree root.
         */
        private tailrec fun buildMerkleTree(lastNodesList: List<MerkleTree>): MerkleTree {
            return if (lastNodesList.size == 1) {
                lastNodesList[0] // Root reached.
            } else {
                val newLevelHashes: MutableList<MerkleTree> = ArrayList()
                val n = lastNodesList.size
                require((n and 1) == 0) { "Sanity check: number of nodes should be even." }
                for (i in 0..n - 2 step 2) {
                    val left = lastNodesList[i]
                    val right = lastNodesList[i + 1]
                    val newHash = left.hash.concatenate(right.hash)
                    val combined = Node(newHash, left, right)
                    newLevelHashes.add(combined)
                }
                buildMerkleTree(newLevelHashes)
            }
        }
    }
}