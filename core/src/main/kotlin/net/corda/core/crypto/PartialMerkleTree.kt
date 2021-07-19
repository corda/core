package net.corda.core.crypto

import net.corda.core.CordaException
import net.corda.core.CordaInternal
import net.corda.core.KeepForDJVM
import net.corda.core.serialization.CordaSerializable
import java.util.*

@KeepForDJVM
@CordaSerializable
class MerkleTreeException(val reason: String) : CordaException("Partial Merkle Tree exception. Reason: $reason")

/**
 * Building and verification of Partial Merkle Tree.
 * Partial Merkle Tree is a minimal tree needed to check that a given set of leaves belongs to a full Merkle Tree.
 *
 * Example of Merkle tree with 5 leaves.
 *
 *             h15
 *          /       \
 *         h14       h55
 *        /  \      /  \
 *      h12  h34   h50 h00
 *     / \   / \   / \  / \
 *    l1 l2 l3 l4 l5 0 0  0
 *
 * l* denote hashes of leaves, h* - hashes of nodes below. 0 denotes zero hash, we use it to pad not full binary trees,
 * so the number of leaves is always a power of 2.
 *
 * Example of Partial tree based on the tree above.
 *
 *             ___
 *          /       \
 *          _        _
 *        /  \      /  \
 *      h12   _     _   h00
 *           / \   / \
 *          I3 l4 I5 0
 *
 * We want to check l3 and l5 - now turned into IncudedLeaf (I3 and I5 above). To verify that these two leaves belong to
 * the tree with a hash root h15 we need to provide a Merkle branch (or partial tree). In our case we need hashes:
 * h12, l4, 0 and h00. Verification is done by hashing the partial tree to obtain the root and checking it against
 * the obtained h15 hash. Additionally we store included hashes used in calculation and compare them to leaves hashes we got
 * (there can be a difference in obtained leaves ordering - that's why it's a set comparison not hashing leaves into a tree).
 * If both equalities hold, we can assume that l3 and l5 belong to the transaction with root h15.
 */
@KeepForDJVM
@CordaSerializable
class PartialMerkleTree(val root: PartialTree) {
    /**
     * The structure is a little different than that of Merkle Tree.
     * Partial Tree might not be a full binary tree. Leaves represent either original Merkle tree leaves
     * or cut subtree node with stored hash. We differentiate between the leaves that are included in a filtered
     * transaction and leaves that just keep hashes needed for calculation. Reason for this approach: during verification
     * it's easier to extract hashes used as a base for this tree.
     */
    @CordaSerializable
    sealed class PartialTree {
        @KeepForDJVM data class IncludedLeaf(val hash: SecureHash) : PartialTree()
        @KeepForDJVM data class Leaf(val hash: SecureHash) : PartialTree()
        @KeepForDJVM data class Node(val left: PartialTree, val right: PartialTree) : PartialTree()
    }

    companion object {
        /**
         * @param merkleRoot Root of full Merkle tree.
         * @param includeHashes Hashes that should be included in a partial tree.
         * @return Partial Merkle tree root.
         */
        @Throws(IllegalArgumentException::class, MerkleTreeException::class)
        fun build(merkleRoot: MerkleTree, includeHashes: List<SecureHash>): PartialMerkleTree {
            require(includeHashes.none(SecureHash::isZero)) { "Zero hashes shouldn't be included in partial tree." }
            checkFull(merkleRoot) // Throws MerkleTreeException if it is not a full binary tree.
            val usedHashes = ArrayList<SecureHash>()
            val tree = buildPartialTree(merkleRoot, includeHashes, usedHashes)
            // Too many included hashes or different ones.
            if (includeHashes.size != usedHashes.size)
                throw MerkleTreeException("Some of the provided hashes are not in the tree.")
            return PartialMerkleTree(tree.second)
        }

        // Check if a MerkleTree is full binary tree. Returns the height of the tree if full, otherwise throws exception.
        private fun checkFull(tree: MerkleTree, level: Int = 0): Int {
            return when (tree) {
                is MerkleTree.Leaf -> level
                is MerkleTree.Node -> {
                    val l1 = checkFull(tree.left, level + 1)
                    val l2 = checkFull(tree.right, level + 1)
                    if (l1 != l2) throw MerkleTreeException("Got not full binary tree.")
                    l1
                }
            }
        }

        /**
         * @param root Root of full Merkle tree which is a base for a partial one.
         * @param includeHashes Hashes of leaves to be included in this partial tree.
         * @param usedHashes Hashes actually used to build this partial tree.
         * @return Pair, first element indicates if in a subtree there is a leaf that is included in that partial tree.
         * Second element refers to that subtree.
         */
        private fun buildPartialTree(
                root: MerkleTree,
                includeHashes: List<SecureHash>,
                usedHashes: MutableList<SecureHash>
        ): Pair<Boolean, PartialTree> {
            return when (root) {
                is MerkleTree.Leaf ->
                    if (root.hash in includeHashes) {
                        usedHashes.add(root.hash)
                        Pair(true, PartialTree.IncludedLeaf(root.hash))
                    } else Pair(false, PartialTree.Leaf(root.hash))
                is MerkleTree.Node -> {
                    val leftNode = buildPartialTree(root.left, includeHashes, usedHashes)
                    val rightNode = buildPartialTree(root.right, includeHashes, usedHashes)
                    if (leftNode.first or rightNode.first) {
                        // This node is on a path to some included leaves. Don't store hash.
                        val newTree = PartialTree.Node(leftNode.second, rightNode.second)
                        Pair(true, newTree)
                    } else {
                        // This node has no included leaves below. Cut the tree here and store a hash as a Leaf.
                        val newTree = PartialTree.Leaf(root.hash)
                        Pair(false, newTree)
                    }
                }
            }
        }

        /**
         * Recursive calculation of root of this partial tree.
         * Modifies usedHashes to later check for inclusion with hashes provided.
         * @param node the partial Merkle tree for which we want to calculate the Merkle root.
         * @param usedHashes a mutable list that at the end of this recursive algorithm, it will consist of the included leaves (hashes of the visible components).
         * @return the root [SecureHash] of this partial Merkle tree.
         */
        fun rootAndUsedHashes(node: PartialTree, usedHashes: MutableList<SecureHash>): SecureHash {
            return when (node) {
                is PartialTree.IncludedLeaf -> {
                    usedHashes.add(node.hash)
                    node.hash
                }
                is PartialTree.Leaf -> node.hash
                is PartialTree.Node -> {
                    val leftHash = rootAndUsedHashes(node.left, usedHashes)
                    val rightHash = rootAndUsedHashes(node.right, usedHashes)
                    leftHash.concatenate(rightHash)
                }
            }
        }
    }

    /**
     * Function to verify a [PartialMerkleTree] against an input Merkle root and a list of leaves.
     * The tree should only contain the leaves defined in [hashesToCheck].
     * @param merkleRootHash Hash that should be checked for equality with root calculated from this partial tree.
     * @param hashesToCheck List of included leaves hashes that should be found in this partial tree.
     */
    fun verify(merkleRootHash: SecureHash, hashesToCheck: List<SecureHash>): Boolean {
        val usedHashes = ArrayList<SecureHash>()
        val verifyRoot = rootAndUsedHashes(root, usedHashes)
        return verifyRoot == merkleRootHash // Tree roots match.
                && hashesToCheck.size == usedHashes.size // Obtained the same number of hashes (leaves).
                && hashesToCheck.toSet().containsAll(usedHashes) // Lists contain the same elements.
    }

    /**
     * Method to return the index of the input leaf in the partial Merkle tree structure.
     * @param leaf the component hash to check.
     * @return leaf-index of this component (starting from zero).
     * @throws MerkleTreeException if the provided hash is not in the tree.
     */
    @CordaInternal
    @Throws(MerkleTreeException::class)
    fun leafIndex(leaf: SecureHash): Int {
        // Special handling if the tree consists of one node only.
        if (root is PartialTree.IncludedLeaf && root.hash == leaf) return 0
        val flagPath = mutableListOf<Boolean>()
        if (!leafIndexHelper(leaf, this.root, flagPath)) throw MerkleTreeException("The provided hash $leaf is not in the tree.")
        return indexFromFlagPath(flagPath)
    }

    // Helper function to compute the path. False means go to the left and True to the right.
    // Because the path is updated recursively, the path is returned in reverse order.
    private fun leafIndexHelper(leaf: SecureHash, node: PartialTree, path: MutableList<Boolean>): Boolean {
        if (node is PartialTree.IncludedLeaf) {
            return node.hash == leaf
        } else if (node is PartialTree.Node) {
            if (leafIndexHelper(leaf, node.left, path)) {
                path.add(false)
                return true
            }
            if (leafIndexHelper(leaf, node.right, path)) {
                path.add(true)
                return true
            }
        }
        return false
    }

    // Return the leaf index from the path boolean list.
    private fun indexFromFlagPath(pathList: List<Boolean>) =
            pathList.mapIndexed { index, value -> if (value) (1 shl index) else 0 }.sum()
}
