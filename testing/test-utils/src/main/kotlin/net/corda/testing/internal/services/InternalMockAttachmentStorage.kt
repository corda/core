package net.corda.testing.internal.services

import net.corda.core.contracts.Attachment
import net.corda.core.node.services.AttachmentId
import net.corda.core.node.services.AttachmentStorage
import net.corda.core.node.services.vault.AttachmentQueryCriteria
import net.corda.node.services.persistence.AttachmentStorageInternal
import net.corda.testing.services.MockAttachmentStorage
import java.io.InputStream
import java.util.stream.Stream

/**
 * Internal version of [MockAttachmentStorage] that implements [AttachmentStorageInternal] for use
 * in internal tests where [AttachmentStorageInternal] functions are needed.
 */
class InternalMockAttachmentStorage(storage: MockAttachmentStorage) : AttachmentStorageInternal,
    AttachmentStorage by storage {

    override fun privilegedImportAttachment(
        jar: InputStream,
        uploader: String,
        filename: String?
    ): AttachmentId = importAttachment(jar, uploader, filename)

    override fun privilegedImportOrGetAttachment(
        jar: InputStream,
        uploader: String,
        filename: String?
    ): AttachmentId {
        return try {
            importAttachment(jar, uploader, filename)
        } catch (faee: java.nio.file.FileAlreadyExistsException) {
            AttachmentId.create(faee.message!!)
        }
    }

    override fun getAllAttachmentsByCriteria(criteria: AttachmentQueryCriteria): Stream<Pair<String?, Attachment>> {
        return queryAttachments(criteria)
            .map(this::openAttachment)
            .map { null as String? to it!! }
            .stream()
    }
}