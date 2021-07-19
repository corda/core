package net.corda.testing.services

import net.corda.core.contracts.Attachment
import net.corda.core.contracts.ContractAttachment
import net.corda.core.contracts.ContractClassName
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.sha256
import net.corda.core.internal.AbstractAttachment
import net.corda.core.internal.TRUSTED_UPLOADERS
import net.corda.core.internal.UNKNOWN_UPLOADER
import net.corda.core.internal.cordapp.CordappImpl.Companion.DEFAULT_CORDAPP_VERSION
import net.corda.core.internal.readFully
import net.corda.core.node.services.AttachmentId
import net.corda.core.node.services.AttachmentStorage
import net.corda.core.node.services.vault.*
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.nodeapi.internal.withContractsInJar
import java.io.InputStream
import java.security.PublicKey
import java.util.*
import java.util.jar.Attributes
import java.util.jar.JarInputStream

/**
 * A mock implementation of [AttachmentStorage] for use within tests
 */
class MockAttachmentStorage : AttachmentStorage, SingletonSerializeAsToken() {

    private data class ContractAttachmentMetadata(val name: ContractClassName, val version: Int, val isSigned: Boolean, val signers: List<PublicKey>, val uploader: String)

    private val _files = HashMap<SecureHash, Pair<Attachment, ByteArray>>()
    private val _contractClasses = HashMap<ContractAttachmentMetadata, SecureHash>()
    /** A map of the currently stored files by their [SecureHash] */
    val files: Map<SecureHash, Pair<Attachment, ByteArray>> get() = _files

    @Suppress("OverridingDeprecatedMember")
    override fun importAttachment(jar: InputStream): AttachmentId = importAttachment(jar, UNKNOWN_UPLOADER, null)

    override fun importAttachment(jar: InputStream, uploader: String, filename: String?): AttachmentId {
        return withContractsInJar(jar) { contractClassNames, inputStream ->
            importAttachmentInternal(inputStream, uploader, contractClassNames)
        }
    }

    override fun openAttachment(id: SecureHash): Attachment? = files[id]?.first

    // This function only covers those possibilities currently used within tests. Each ColumnPredicate type can have multiple operators,
    // and not all predicate types are covered here.
    private fun <C> criteriaFilter(metadata: C, predicate: ColumnPredicate<C>?): Boolean {
        return when (predicate) {
            // real implementation allows an intersection of signers to return results from the query
            is ColumnPredicate.EqualityComparison -> {
                val rightLiteral = predicate.rightLiteral
                when (rightLiteral) {
                    is Collection<*> -> rightLiteral.any { value ->
                        (metadata as Collection<*>).contains(value)
                    }
                    else -> rightLiteral == metadata
                }
            }
            is ColumnPredicate.CollectionExpression -> predicate.rightLiteral.contains(metadata)
            else -> true
        }
    }

    override fun queryAttachments(criteria: AttachmentQueryCriteria, sorting: AttachmentSort?): List<AttachmentId> {
        criteria as AttachmentQueryCriteria.AttachmentsQueryCriteria
        val metadataFilter = { metadata: ContractAttachmentMetadata ->
            criteriaFilter(listOf(metadata.name), criteria.contractClassNamesCondition) &&
                    criteriaFilter(metadata.signers, criteria.signersCondition) &&
                    criteriaFilter(metadata.isSigned, criteria.isSignedCondition) &&
                    criteriaFilter(metadata.version, criteria.versionCondition) &&
                    criteriaFilter(metadata.uploader, criteria.uploaderCondition)
        }

        return _contractClasses.filterKeys { metadataFilter(it) }.values.toList()
    }

    override fun hasAttachment(attachmentId: AttachmentId) = files.containsKey(attachmentId)

    @Suppress("OverridingDeprecatedMember")
    override fun importOrGetAttachment(jar: InputStream): AttachmentId {
        return try {
            importAttachment(jar, UNKNOWN_UPLOADER, null)
        } catch (e: java.nio.file.FileAlreadyExistsException) {
            AttachmentId.create(e.message!!)
        }
    }

    @JvmOverloads
    fun importContractAttachment(contractClassNames: List<ContractClassName>, uploader: String, jar: InputStream, attachmentId: AttachmentId? = null,  signers: List<PublicKey> = emptyList()): AttachmentId = importAttachmentInternal(jar, uploader, contractClassNames, attachmentId, signers)

    fun importContractAttachment(attachmentId: AttachmentId, contractAttachment: ContractAttachment) {
        _files[attachmentId] = Pair(contractAttachment, ByteArray(1))
    }

    fun getAttachmentIdAndBytes(jar: InputStream): Pair<AttachmentId, ByteArray> = jar.readFully().let { bytes -> Pair(bytes.sha256(), bytes) }

    private class MockAttachment(dataLoader: () -> ByteArray, override val id: SecureHash, override val signerKeys: List<PublicKey>, uploader: String) : AbstractAttachment(dataLoader, uploader)

    private fun importAttachmentInternal(jar: InputStream, uploader: String, contractClassNames: List<ContractClassName>? = null, attachmentId: AttachmentId? = null, signers: List<PublicKey> = emptyList()): AttachmentId {
        // JIS makes read()/readBytes() return bytes of the current file, but we want to hash the entire container here.
        require(jar !is JarInputStream)

        val bytes = jar.readFully()

        val sha256 = attachmentId ?: bytes.sha256()
        if (sha256 !in files.keys) {
            val baseAttachment = MockAttachment({ bytes }, sha256, signers, uploader)
            val version = try { Integer.parseInt(baseAttachment.openAsJAR().manifest?.mainAttributes?.getValue(Attributes.Name.IMPLEMENTATION_VERSION)) } catch (e: Exception) { DEFAULT_CORDAPP_VERSION }
            val attachment =
                    if (contractClassNames == null || contractClassNames.isEmpty()) baseAttachment
                    else {
                        contractClassNames.map {contractClassName ->
                            val contractClassMetadata = ContractAttachmentMetadata(contractClassName, version, signers.isNotEmpty(), signers, uploader)
                            _contractClasses[contractClassMetadata] = sha256
                        }
                        ContractAttachment.create(baseAttachment, contractClassNames.first(), contractClassNames.toSet(), uploader, signers, version)
                    }
            _files[sha256] = Pair(attachment, bytes)
        }
        return sha256
    }

    override fun getLatestContractAttachments(contractClassName: String, minContractVersion: Int): List<AttachmentId> {
        val attachmentQueryCriteria = AttachmentQueryCriteria.AttachmentsQueryCriteria(contractClassNamesCondition = Builder.equal(listOf(contractClassName)),
                versionCondition = Builder.greaterThanOrEqual(minContractVersion), uploaderCondition = Builder.`in`(TRUSTED_UPLOADERS))
        val attachmentSort = AttachmentSort(listOf(AttachmentSort.AttachmentSortColumn(AttachmentSort.AttachmentSortAttribute.VERSION, Sort.Direction.DESC)))
        return queryAttachments(attachmentQueryCriteria, attachmentSort)
    }
}