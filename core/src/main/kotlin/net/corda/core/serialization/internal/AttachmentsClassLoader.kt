package net.corda.core.serialization.internal

import net.corda.core.contracts.Attachment
import net.corda.core.contracts.ContractAttachment
import net.corda.core.contracts.TransactionVerificationException
import net.corda.core.contracts.TransactionVerificationException.OverlappingAttachmentsException
import net.corda.core.contracts.TransactionVerificationException.PackageOwnershipException
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.sha256
import net.corda.core.internal.*
import net.corda.core.internal.cordapp.targetPlatformVersion
import net.corda.core.node.NetworkParameters
import net.corda.core.serialization.*
import net.corda.core.serialization.internal.AttachmentURLStreamHandlerFactory.toUrl
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.debug
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.lang.ref.WeakReference
import java.net.*
import java.security.Permission
import java.util.*

/**
 * A custom ClassLoader that knows how to load classes from a set of attachments. The attachments themselves only
 * need to provide JAR streams, and so could be fetched from a database, local disk, etc. Constructing an
 * AttachmentsClassLoader is somewhat expensive, as every attachment is scanned to ensure that there are no overlapping
 * file paths. In addition, every JAR is scanned to ensure that it doesn't violate the package namespace ownership
 * rules.
 *
 * @property params The network parameters fetched from the transaction for which this classloader was built.
 * @property sampleTxId The transaction ID that triggered the creation of this classloader. Because classloaders are cached
 *           this tx may be stale, that is, classloading might be triggered by the verification of some other transaction
 *           if not all code is invoked every time, however we want a txid for errors in case of attachment bogusness.
 */
class AttachmentsClassLoader(attachments: List<Attachment>,
                             val params: NetworkParameters,
                             private val sampleTxId: SecureHash,
                             isAttachmentTrusted: (Attachment) -> Boolean,
                             parent: ClassLoader = ClassLoader.getSystemClassLoader()) :
        URLClassLoader(attachments.map(::toUrl).toTypedArray(), parent) {

    companion object {
        private val log = contextLogger()

        init {
            // Apply our own URLStreamHandlerFactory to resolve attachments
            setOrDecorateURLStreamHandlerFactory()
        }

        // Jolokia and Json-simple are dependencies that were bundled by mistake within contract jars.
        // In the AttachmentsClassLoader we just block any class in those 2 packages.
        private val ignoreDirectories = listOf("org/jolokia/", "org/json/simple/")
        private val ignorePackages = ignoreDirectories.map { it.replace("/", ".") }

        /**
         * Apply our custom factory either directly, if `URL.setURLStreamHandlerFactory` has not been called yet,
         * or use a decorator and reflection to bypass the single-call-per-JVM restriction otherwise.
         */
        private fun setOrDecorateURLStreamHandlerFactory() {
            // Retrieve the `URL.factory` field
            val factoryField = URL::class.java.getDeclaredField("factory")
            // Make it accessible
            factoryField.isAccessible = true

            // Check for preset factory, set directly if missing
            val existingFactory: URLStreamHandlerFactory? = factoryField.get(null) as URLStreamHandlerFactory?
            if (existingFactory == null) {
                URL.setURLStreamHandlerFactory(AttachmentURLStreamHandlerFactory)
            }
            // Otherwise, decorate the existing and replace via reflection
            // as calling `URL.setURLStreamHandlerFactory` again will throw an error
            else {
                log.warn("The URLStreamHandlerFactory was already set in the JVM. Please be aware that this is not recommended.")
                // Retrieve the field "streamHandlerLock" of the class URL that
                // is the lock used to synchronize access to the protocol handlers
                val lockField = URL::class.java.getDeclaredField("streamHandlerLock")
                // It is a private field so we need to make it accessible
                // Note: this will only work as-is in JDK8.
                lockField.isAccessible = true
                // Use the same lock to reset the factory
                synchronized(lockField.get(null)) {
                    // Reset the value to prevent Error due to a factory already defined
                    factoryField.set(null, null)
                    // Set our custom factory and wrap the current one into it
                    URL.setURLStreamHandlerFactory(
                            // Set the factory to a decorator
                            object : URLStreamHandlerFactory {
                                // route between our own and the pre-existing factory
                                override fun createURLStreamHandler(protocol: String): URLStreamHandler? {
                                    return AttachmentURLStreamHandlerFactory.createURLStreamHandler(protocol)
                                            ?: existingFactory.createURLStreamHandler(protocol)
                                }
                            }
                    )
                }
            }
        }
    }

    init {

        // Make some preliminary checks to ensure that we're not loading invalid attachments.

        // All attachments need to be valid JAR or ZIP files.
        for (attachment in attachments) {
            if (!isZipOrJar(attachment)) throw TransactionVerificationException.InvalidAttachmentException(sampleTxId, attachment.id)
        }

        // Until we have a sandbox to run untrusted code we need to make sure that any loaded class file was whitelisted by the node administrator.
        val untrusted = attachments
                .filter(::containsClasses)
                .filterNot(isAttachmentTrusted)
                .map(Attachment::id)

        if (untrusted.isNotEmpty()) {
            log.warn("Cannot verify transaction $sampleTxId as the following attachment IDs are untrusted: $untrusted." +
                    "You will need to manually install the CorDapp to whitelist it for use.")
            throw TransactionVerificationException.UntrustedAttachmentsException(sampleTxId, untrusted)
        }

        // Enforce the no-overlap and package ownership rules.
        checkAttachments(attachments)
    }

    private fun isZipOrJar(attachment: Attachment) = attachment.openAsJAR().use { jar ->
        jar.nextEntry != null
    }

    private fun containsClasses(attachment: Attachment): Boolean {
        attachment.openAsJAR().use { jar ->
            while (true) {
                val entry = jar.nextJarEntry ?: return false
                if (entry.name.endsWith(".class", ignoreCase = true)) return true
            }
        }
        return false
    }

    // This function attempts to strike a balance between security and usability when it comes to the no-overlap rule.
    // TODO - investigate potential exploits.
    private fun shouldCheckForNoOverlap(path: String, targetPlatformVersion: Int): Boolean {
        require(path.toLowerCase() == path)
        require(!path.contains("\\"))

        return when {
            path.endsWith("/") -> false                     // Directories (packages) can overlap.
            targetPlatformVersion < 4 && ignoreDirectories.any { path.startsWith(it) } -> false    // Ignore jolokia and json-simple for old cordapps.
            path.endsWith(".class") -> true                 // All class files need to be unique.
            !path.startsWith("meta-inf") -> true            // All files outside of META-INF need to be unique.
            (path == "meta-inf/services/net.corda.core.serialization.serializationwhitelist") -> false // Allow overlapping on the SerializationWhitelist.
            path.startsWith("meta-inf/services") -> true    // Services can't overlap to prevent a malicious party from injecting additional implementations of an interface used by a contract.
            else -> false                                          // This allows overlaps over any non-class files in "META-INF" - except 'services'.
        }
    }

    private fun checkAttachments(attachments: List<Attachment>) {
        require(attachments.isNotEmpty()) { "attachments list is empty" }

        // Here is where we enforce the no-overlap and package ownership rules.
        //
        // The no-overlap rule states that a transaction which has multiple attachments defining different files for
        // the same file path is invalid. It's an important part of the security model and blocks various sorts of
        // attacks.
        //
        // Consider the case of a transaction with two attachments, A and B. Attachment B satisfies the constraint
        // on the transaction's states, and thus should be bound by the logic imposed by the contract logic in that
        // attachment. But if attachment A were to supply a different class file with the same file name, then the
        // usual Java classpath semantics would apply and it'd end up being contract A that gets executed, not B.
        // This would prevent you from reasoning about the semantics and transitional logic applied to a state; in
        // effect the ledger would be open to arbitrary malicious changes.
        //
        // There are several variants of this attack that mean we must enforce the no-overlap rule on every file.
        // For instance the attacking attachment may override an inner class of the contract class, or a dependency.
        // However some files do normally overlap between JARs, like manifest files and others under META-INF. Those
        // do not affect code execution and are excluded.
        //
        // Package ownership rules are intended to avoid attacks in which the adversaries define classes in victim
        // namespaces. Whilst the constraints and attachments mechanism would keep these logically separated on the
        // ledger itself, once such states are serialised and deserialised again e.g. across RPC, to XML or JSON
        // then the origin of the code may be lost and only the fully qualified class name may remain. To avoid
        // attacks on externally connected systems that only consider type names, we allow people to formally
        // claim their parts of the Java package namespace via registration with the zone operator.

        val classLoaderEntries = mutableMapOf<String, SecureHash.SHA256>()
        for (attachment in attachments) {
            // We may have been given an attachment loaded from the database in which case, important info like
            // signers is already calculated.
            val signers = if (attachment is ContractAttachment) {
                attachment.signerKeys
            } else {
                // The call below reads the entire JAR and calculates all the public keys that signed the JAR.
                // It also verifies that there are no mismatches, like a JAR with two signers where some files
                // are signed by key A and others only by key B.
                //
                // The process of iterating every file of an attachment is important because JAR signature
                // checks are only applied during a file read. Merely opening a signed JAR does not imply
                // the files within it are correctly signed, but, we wish to verify package ownership
                // at this point during construction because otherwise we may conclude a JAR is properly
                // signed by the owners of the packages, even if it's not. We'd eventually discover that fact
                // when trying to read the class file to use it, but if we'd made any decisions based on
                // perceived correctness of the signatures or package ownership already, that would be too late.
                attachment.openAsJAR().use { JarSignatureCollector.collectSigners(it) }
            }
            // Now open it again to compute the overlap and package ownership data.
            attachment.openAsJAR().use { jar ->
                val targetPlatformVersion = jar.manifest?.targetPlatformVersion ?: 1
                while (true) {
                    val entry = jar.nextJarEntry ?: break
                    if (entry.isDirectory) continue

                    // We already verified that paths are not strange/game playing when we inserted the attachment
                    // into the storage service. So we don't need to repeat it here.
                    //
                    // We forbid files that differ only in case, or path separator to avoid issues for Windows/Mac developers where the
                    // filesystem tries to be case insensitive. This may break developers who attempt to use ProGuard.
                    //
                    // Also convert to Unix path separators as all resource/class lookups will expect this.
                    val path = entry.name.toLowerCase(Locale.US).replace('\\', '/')

                    // Namespace ownership. We only check class files: resources are loaded relative to a JAR anyway.
                    if (path.endsWith(".class")) {
                        // Get the package name from the file name. Inner classes separate their names with $ not /
                        // in file names so they are not a problem.
                        val pkgName = path
                                .dropLast(".class".length)
                                .replace('/', '.')
                                .split('.')
                                .dropLast(1)
                                .joinToString(".")
                        for ((namespace, pubkey) in params.packageOwnership) {
                            // Note that due to the toLowerCase() call above, we'll be comparing against a lowercased
                            // version of the ownership claim.
                            val ns = namespace.toLowerCase(Locale.US)
                            // We need an additional . to avoid matching com.foo.Widget against com.foobar.Zap
                            if (pkgName == ns || pkgName.startsWith("$ns.")) {
                                if (pubkey !in signers)
                                    throw PackageOwnershipException(sampleTxId, attachment.id, path, pkgName)
                            }
                        }
                    }

                    // Some files don't need overlap checking because they don't affect the way the code runs.
                    if (!shouldCheckForNoOverlap(path, targetPlatformVersion)) continue

                    // This calculates the hash of the current entry because the JarInputStream returns only the current entry.
                    fun entryHash() = ByteArrayOutputStream().use {
                        jar.copyTo(it)
                        it.toByteArray()
                    }.sha256()

                    // If 2 entries are identical, it means the same file is present in both attachments, so that is ok.
                    val currentHash = entryHash()
                    val previousFileHash = classLoaderEntries[path]
                    when {
                        previousFileHash == null -> {
                            log.debug { "Adding new entry for $path" }
                            classLoaderEntries[path] = currentHash
                        }
                        currentHash == previousFileHash -> log.debug { "Duplicate entry $path has same content hash $currentHash" }
                        else -> {
                            log.debug { "Content hash differs for $path" }
                            throw OverlappingAttachmentsException(sampleTxId, path)
                        }
                    }
                }
            }
            log.debug { "${classLoaderEntries.size} classloaded entries for $attachment" }
        }
    }

    /**
     * Required to prevent classes that were excluded from the no-overlap check from being loaded by contract code.
     * As it can lead to non-determinism.
     */
    override fun loadClass(name: String?): Class<*> {
        if (ignorePackages.any { name!!.startsWith(it) }) {
            throw ClassNotFoundException(name)
        }
        return super.loadClass(name)
    }
}

/**
 * This is just a factory that provides caches to optimise expensive construction/loading of classloaders, serializers,
 * whitelisted classes.
 */
@VisibleForTesting
object AttachmentsClassLoaderBuilder {
    private const val CACHE_SIZE = 1000

    // We use a set here because the ordering of attachments doesn't affect code execution, due to the no
    // overlap rule, and attachments don't have any particular ordering enforced by the builders. So we
    // can just do unordered comparisons here. But the same attachments run with different network parameters
    // may behave differently, so that has to be a part of the cache key.
    private data class Key(val hashes: Set<SecureHash>, val params: NetworkParameters)

    // This runs in the DJVM so it can't use caffeine.
    private val cache: MutableMap<Key, SerializationContext> = createSimpleCache<Key, SerializationContext>(CACHE_SIZE).toSynchronised()

    /**
     * Runs the given block with serialization execution context set up with a (possibly cached) attachments classloader.
     *
     * @param txId The transaction ID that triggered this request; it's unused except for error messages and exceptions that can occur during setup.
     */
    fun <T> withAttachmentsClassloaderContext(attachments: List<Attachment>,
                                              params: NetworkParameters,
                                              txId: SecureHash,
                                              isAttachmentTrusted: (Attachment) -> Boolean,
                                              lowMemoryMode: Boolean = false,
                                              parent: ClassLoader = ClassLoader.getSystemClassLoader(),
                                              block: (ClassLoader) -> T): T {
        val attachmentIds = attachments.map(Attachment::id).toSet()

        val serializationContext = cache.computeIfAbsent(Key(attachmentIds, params)) {
            // Create classloader and load serializers, whitelisted classes
            val transactionClassLoader = AttachmentsClassLoader(attachments, params, txId, isAttachmentTrusted, parent)
            val serializers = try {
                createInstancesOfClassesImplementing(transactionClassLoader, SerializationCustomSerializer::class.java,
                        JDK1_2_CLASS_FILE_FORMAT_MAJOR_VERSION..JDK8_CLASS_FILE_FORMAT_MAJOR_VERSION, lowMemoryMode)
                }
                catch(ex: UnsupportedClassVersionError) {
                    throw TransactionVerificationException.UnsupportedClassVersionError(txId, ex.message!!, ex)
                }
            val whitelistedClasses = ServiceLoader.load(SerializationWhitelist::class.java, transactionClassLoader)
                    .flatMap(SerializationWhitelist::whitelist)

            // Create a new serializationContext for the current transaction. In this context we will forbid
            // deserialization of objects from the future, i.e. disable forwards compatibility. This is to ensure
            // that app logic doesn't ignore newly added fields or accidentally downgrade data from newer state
            // schemas to older schemas by discarding fields.
            SerializationFactory.defaultFactory.defaultContext
                    .withPreventDataLoss()
                    .withClassLoader(transactionClassLoader)
                    .withWhitelist(whitelistedClasses)
                    .withCustomSerializers(serializers)
                    .withoutCarpenter()
        }

        // Deserialize all relevant classes in the transaction classloader.
        return SerializationFactory.defaultFactory.withCurrentContext(serializationContext) {
            block(serializationContext.deserializationClassLoader)
        }
    }
}

/**
 * Registers a new internal "attachment" protocol.
 * This will not be exposed as an API.
 */
object AttachmentURLStreamHandlerFactory : URLStreamHandlerFactory {
    internal const val attachmentScheme = "attachment"

    private val loadedAttachments: AttachmentsHolder = AttachmentsHolderImpl()

    override fun createURLStreamHandler(protocol: String): URLStreamHandler? {
        return if (attachmentScheme == protocol) {
            AttachmentURLStreamHandler
        } else null
    }

    @Synchronized
    fun toUrl(attachment: Attachment): URL {
        val proposedURL = URL(attachmentScheme, "", -1, attachment.id.toString(), AttachmentURLStreamHandler)
        val existingURL = loadedAttachments.getKey(proposedURL)
        return if (existingURL == null) {
            loadedAttachments[proposedURL] = attachment
            proposedURL
        } else {
            existingURL
        }
    }

    @VisibleForTesting
    fun loadedAttachmentsSize(): Int = loadedAttachments.size

    private object AttachmentURLStreamHandler : URLStreamHandler() {
        override fun openConnection(url: URL): URLConnection {
            if (url.protocol != attachmentScheme) throw IOException("Cannot handle protocol: ${url.protocol}")
            val attachment = loadedAttachments[url] ?: throw IOException("Could not load url: $url .")
            return AttachmentURLConnection(url, attachment)
        }

        override fun equals(attachmentUrl: URL, otherURL: URL?): Boolean {
            if (attachmentUrl.protocol != otherURL?.protocol) return false
            if (attachmentUrl.protocol != attachmentScheme) throw IllegalArgumentException("Cannot handle protocol: ${attachmentUrl.protocol}")
            return attachmentUrl.file == otherURL?.file
        }

        override fun hashCode(url: URL): Int {
            if (url.protocol != attachmentScheme) throw IllegalArgumentException("Cannot handle protocol: ${url.protocol}")
            return url.file.hashCode()
        }
    }
}

interface AttachmentsHolder {
    val size: Int
    fun getKey(key: URL): URL?
    operator fun get(key: URL): Attachment?
    operator fun set(key: URL, value: Attachment)
}

private class AttachmentsHolderImpl : AttachmentsHolder {
    private val attachments = WeakHashMap<URL, Pair<WeakReference<URL>, Attachment>>().toSynchronised()

    override val size: Int get() = attachments.size

    override fun getKey(key: URL): URL? {
        return attachments[key]?.first?.get()
    }

    override fun get(key: URL): Attachment? {
        return attachments[key]?.second
    }

    override fun set(key: URL, value: Attachment) {
        attachments[key] = WeakReference(key) to value
    }
}

private class AttachmentURLConnection(url: URL, private val attachment: Attachment) : URLConnection(url) {
    override fun getContentLengthLong(): Long = attachment.size.toLong()
    override fun getInputStream(): InputStream = attachment.open()
    /**
     * Define the permissions that [AttachmentsClassLoader] will need to
     * use this [URL]. The attachment is stored in memory, and so we
     * don't need any extra permissions here. But if we don't override
     * [getPermission] then [AttachmentsClassLoader] will assign the
     * default permission of ALL_PERMISSION to these classes'
     * [java.security.ProtectionDomain]. This would be a security hole!
     */
    override fun getPermission(): Permission? = null
    override fun connect() {
        connected = true
    }
}