package net.corda.node.internal.cordapp

import com.google.common.collect.HashBiMap
import net.corda.core.contracts.Attachment
import net.corda.core.contracts.ContractClassName
import net.corda.core.cordapp.Cordapp
import net.corda.core.cordapp.CordappContext
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.FlowLogic
import net.corda.core.internal.DEPLOYED_CORDAPP_UPLOADER
import net.corda.core.internal.cordapp.CordappImpl
import net.corda.core.internal.isUploaderTrusted
import net.corda.core.node.services.AttachmentFixup
import net.corda.core.node.services.AttachmentId
import net.corda.core.node.services.AttachmentStorage
import net.corda.core.serialization.MissingAttachmentsException
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.utilities.contextLogger
import net.corda.node.services.persistence.AttachmentStorageInternal
import net.corda.nodeapi.internal.cordapp.CordappLoader
import java.net.JarURLConnection
import java.net.URL
import java.util.concurrent.ConcurrentHashMap
import java.util.jar.JarFile

/**
 * Cordapp provider and store. For querying CorDapps for their attachment and vice versa.
 */
open class CordappProviderImpl(val cordappLoader: CordappLoader,
                               private val cordappConfigProvider: CordappConfigProvider,
                               private val attachmentStorage: AttachmentStorage) : SingletonSerializeAsToken(), CordappProviderInternal {
    companion object {
        const val COMMENT_MARKER = '#'
        private val log = contextLogger()
    }

    private val contextCache = ConcurrentHashMap<Cordapp, CordappContext>()
    private val cordappAttachments = HashBiMap.create<SecureHash, URL>()
    private val attachmentFixups = arrayListOf<AttachmentFixup>()

    /**
     * Current known CorDapps loaded on this node
     */
    override val cordapps: List<CordappImpl> get() = cordappLoader.cordapps

    fun start() {
        cordappAttachments.putAll(loadContractsIntoAttachmentStore())
        verifyInstalledCordapps()
        // Load the fix-ups after uploading any new contracts into attachment storage.
        attachmentFixups.addAll(loadAttachmentFixups())
    }

    private fun verifyInstalledCordapps() {
        // This will invoke the lazy flowCordappMap property, thus triggering the MultipleCordappsForFlow check.
        cordappLoader.flowCordappMap
    }

    override fun getAppContext(): CordappContext {
        // TODO: Use better supported APIs in Java 9
        Exception().stackTrace.forEach { stackFrame ->
            val cordapp = getCordappForClass(stackFrame.className)
            if (cordapp != null) {
                return getAppContext(cordapp)
            }
        }

        throw IllegalStateException("Not in an app context")
    }

    override fun getContractAttachmentID(contractClassName: ContractClassName): AttachmentId? {
        return getCordappForClass(contractClassName)?.let(this::getCordappAttachmentId)
    }

    /**
     * Gets the attachment ID of this CorDapp. Only CorDapps with contracts have an attachment ID
     *
     * @param cordapp The cordapp to get the attachment ID
     * @return An attachment ID if it exists, otherwise nothing
     */
    fun getCordappAttachmentId(cordapp: Cordapp): SecureHash? = cordappAttachments.inverse()[cordapp.jarPath]

    private fun loadContractsIntoAttachmentStore(): Map<SecureHash, URL> =
            cordapps.filter { it.contractClassNames.isNotEmpty() }.map { cordapp ->
                cordapp.jarPath.openStream().use { stream ->
                    try {
                        // This code can be reached by [MockNetwork] tests which uses [MockAttachmentStorage]
                        // [MockAttachmentStorage] cannot implement [AttachmentStorageInternal] because
                        // doing so results in internal functions being exposed in the public API.
                        if (attachmentStorage is AttachmentStorageInternal) {
                            attachmentStorage.privilegedImportAttachment(
                                stream,
                                DEPLOYED_CORDAPP_UPLOADER,
                                cordapp.info.shortName
                            )
                        } else {
                            attachmentStorage.importAttachment(
                                stream,
                                DEPLOYED_CORDAPP_UPLOADER,
                                cordapp.info.shortName
                            )
                        }
                    } catch (faee: java.nio.file.FileAlreadyExistsException) {
                        AttachmentId.create(faee.message!!)
                    }
                } to cordapp.jarPath
            }.toMap()

    /**
     * Loads the "fixup" rules from all META-INF/Corda-Fixups files.
     * These files have the following format:
     *     <AttachmentId>,<AttachmentId>...=><AttachmentId>,<AttachmentId>,...
     * where each <AttachmentId> is the SHA256 of a CorDapp JAR that
     * [net.corda.core.transactions.TransactionBuilder] will expect to find
     * inside [AttachmentStorage].
     *
     * These rules are for repairing broken CorDapps. A correctly written
     * CorDapp should not require them.
     */
    private fun loadAttachmentFixups(): List<AttachmentFixup> {
        return cordappLoader.appClassLoader.getResources("META-INF/Corda-Fixups").asSequence()
            .mapNotNull { fixup ->
                fixup.openConnection() as? JarURLConnection
            }.filter { fixupConnection ->
                isValidFixup(fixupConnection.jarFile)
            }.flatMapTo(ArrayList()) { fixupConnection ->
                fixupConnection.inputStream.bufferedReader().useLines { lines ->
                    lines.map { it.substringBefore(COMMENT_MARKER) }.map(String::trim).filterNot(String::isEmpty).map { line ->
                        val tokens = line.split("=>")
                        require(tokens.size == 2) {
                            "Invalid fix-up line '$line' in '${fixupConnection.jarFile.name}'"
                        }
                        val source = parseIds(tokens[0])
                        require(source.isNotEmpty()) {
                            "Forbidden empty list of source attachment IDs in '${fixupConnection.jarFile.name}'"
                        }
                        val target = parseIds(tokens[1])
                        Pair(source, target)
                    }.toList().asSequence()
                }
            }
    }

    private fun isValidFixup(jarFile: JarFile): Boolean {
        return jarFile.entries().asSequence().all { it.name.startsWith("META-INF/") }.also { isValid ->
            if (!isValid) {
                log.warn("FixUp '{}' contains files outside META-INF/ - IGNORING!", jarFile.name)
            }
        }
    }

    private fun parseIds(ids: String): Set<AttachmentId> {
        return ids.split(",").map(String::trim)
            .filterNot(String::isEmpty)
            .mapTo(LinkedHashSet(), SecureHash.Companion::parse)
    }

    /**
     * Apply this node's attachment fix-up rules to the given attachment IDs.
     *
     * @param attachmentIds A collection of [AttachmentId]s, e.g. as provided by a transaction.
     * @return The [attachmentIds] with the fix-up rules applied.
     */
    override fun fixupAttachmentIds(attachmentIds: Collection<AttachmentId>): Set<AttachmentId> {
        val replacementIds = LinkedHashSet(attachmentIds)
        attachmentFixups.forEach { (source, target) ->
            if (replacementIds.containsAll(source)) {
                replacementIds.removeAll(source)
                replacementIds.addAll(target)
            }
        }
        return replacementIds
    }

    /**
     * Apply this node's attachment fix-up rules to the given attachments.
     *
     * @param attachments A collection of [Attachment] objects, e.g. as provided by a transaction.
     * @return The [attachments] with the node's fix-up rules applied.
     */
    override fun fixupAttachments(attachments: Collection<Attachment>): Collection<Attachment> {
        val attachmentsById = attachments.associateByTo(LinkedHashMap(), Attachment::id)
        val replacementIds = fixupAttachmentIds(attachmentsById.keys)
        attachmentsById.keys.retainAll(replacementIds)
        (replacementIds - attachmentsById.keys).forEach { extraId ->
            val extraAttachment = attachmentStorage.openAttachment(extraId)
            if (extraAttachment == null || !extraAttachment.isUploaderTrusted()) {
                throw MissingAttachmentsException(listOf(extraId))
            }
            attachmentsById[extraId] = extraAttachment
        }
        return attachmentsById.values
    }

    /**
     * Get the current cordapp context for the given CorDapp
     *
     * @param cordapp The cordapp to get the context for
     * @return A cordapp context for the given CorDapp
     */
    fun getAppContext(cordapp: Cordapp): CordappContext {
        return contextCache.computeIfAbsent(cordapp) {
            CordappContext.create(
                    cordapp,
                    getCordappAttachmentId(cordapp),
                    cordappLoader.appClassLoader,
                    TypesafeCordappConfig(cordappConfigProvider.getConfigByName(cordapp.name))
            )
        }
    }

    /**
     * Resolves a cordapp for the provided class or null if there isn't one
     *
     * @param className The class name
     * @return cordapp A cordapp or null if no cordapp has the given class loaded
     */
    fun getCordappForClass(className: String): Cordapp? = cordapps.find { it.cordappClasses.contains(className) }

    override fun getCordappForFlow(flowLogic: FlowLogic<*>) = cordappLoader.flowCordappMap[flowLogic.javaClass]
}
