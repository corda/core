package net.corda.node.internal.cordapp

import io.github.classgraph.ClassGraph
import io.github.classgraph.ClassInfo
import io.github.classgraph.ScanResult
import net.corda.common.logging.errorReporting.CordappErrors
import net.corda.common.logging.errorReporting.ErrorCode
import net.corda.core.CordaRuntimeException
import net.corda.core.cordapp.Cordapp
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.sha256
import net.corda.core.flows.*
import net.corda.core.internal.*
import net.corda.core.internal.cordapp.CordappImpl
import net.corda.core.internal.cordapp.CordappImpl.Companion.UNKNOWN_INFO
import net.corda.core.internal.cordapp.get
import net.corda.core.internal.notary.NotaryService
import net.corda.core.internal.notary.SinglePartyNotaryService
import net.corda.core.node.services.CordaService
import net.corda.core.schemas.MappedSchema
import net.corda.core.serialization.CheckpointCustomSerializer
import net.corda.core.serialization.SerializationCustomSerializer
import net.corda.core.serialization.SerializationWhitelist
import net.corda.core.serialization.SerializeAsToken
import net.corda.core.utilities.contextLogger
import net.corda.node.VersionInfo
import net.corda.nodeapi.internal.cordapp.CordappLoader
import net.corda.nodeapi.internal.coreContractClasses
import net.corda.serialization.internal.DefaultWhitelist
import java.lang.reflect.Modifier
import java.math.BigInteger
import java.net.URL
import java.net.URLClassLoader
import java.nio.file.Path
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.jar.JarInputStream
import java.util.jar.Manifest
import java.util.zip.ZipInputStream
import kotlin.collections.LinkedHashSet
import kotlin.reflect.KClass
import kotlin.streams.toList

/**
 * Handles CorDapp loading and classpath scanning of CorDapp JARs
 *
 * @property cordappJarPaths The classpath of cordapp JARs
 */
class JarScanningCordappLoader private constructor(private val cordappJarPaths: List<RestrictedURL>,
                                                   private val versionInfo: VersionInfo = VersionInfo.UNKNOWN,
                                                   extraCordapps: List<CordappImpl>,
                                                   private val signerKeyFingerprintBlacklist: List<SecureHash> = emptyList()) : CordappLoaderTemplate() {
    init {
        if (cordappJarPaths.isEmpty()) {
            logger.info("No CorDapp paths provided")
        } else {
            logger.info("Loading CorDapps from ${cordappJarPaths.joinToString()}")
        }
    }
    private val cordappClasses: ConcurrentHashMap<String, Set<Cordapp>> = ConcurrentHashMap()
    override val cordapps: List<CordappImpl> by lazy { loadCordapps() + extraCordapps }

    override val appClassLoader: URLClassLoader = URLClassLoader(cordappJarPaths.stream().map { it.url }.toTypedArray(), javaClass.classLoader)

    override fun close() = appClassLoader.close()

    companion object {
        private val logger = contextLogger()

        /**
         * Creates a CordappLoader from multiple directories.
         *
         * @param cordappDirs Directories used to scan for CorDapp JARs.
         */
        fun fromDirectories(cordappDirs: Collection<Path>,
                            versionInfo: VersionInfo = VersionInfo.UNKNOWN,
                            extraCordapps: List<CordappImpl> = emptyList(),
                            signerKeyFingerprintBlacklist: List<SecureHash> = emptyList()): JarScanningCordappLoader {
            logger.info("Looking for CorDapps in ${cordappDirs.distinct().joinToString(", ", "[", "]")}")
            val paths = cordappDirs.distinct().flatMap(this::jarUrlsInDirectory).map { it.restricted() }
            return JarScanningCordappLoader(paths, versionInfo, extraCordapps, signerKeyFingerprintBlacklist)
        }

        /**
         * Creates a CordappLoader loader out of a list of JAR URLs.
         *
         * @param scanJars Uses the JAR URLs provided for classpath scanning and Cordapp detection.
         */
        fun fromJarUrls(scanJars: List<URL>, versionInfo: VersionInfo = VersionInfo.UNKNOWN, extraCordapps: List<CordappImpl> = emptyList(),
                        cordappsSignerKeyFingerprintBlacklist: List<SecureHash> = emptyList()): JarScanningCordappLoader {
            val paths = scanJars.map { it.restricted() }
            return JarScanningCordappLoader(paths, versionInfo, extraCordapps, cordappsSignerKeyFingerprintBlacklist)
        }

        private fun URL.restricted(rootPackageName: String? = null) = RestrictedURL(this, rootPackageName)

        private fun jarUrlsInDirectory(directory: Path): List<URL> {
            return if (!directory.exists()) {
                emptyList()
            } else {
                directory.list { paths ->
                    // `toFile()` can't be used here...
                    paths.filter { it.toString().endsWith(".jar") }.map { it.toUri().toURL() }.toList()
                }
            }
        }
    }

    private fun loadCordapps(): List<CordappImpl> {
        val invalidCordapps = mutableMapOf<String, URL>()

        val cordapps = cordappJarPaths
                .map { url -> scanCordapp(url).use { it.toCordapp(url) } }
                .filter {
                    if (it.minimumPlatformVersion > versionInfo.platformVersion) {
                        logger.warn("Not loading CorDapp ${it.info.shortName} (${it.info.vendor}) as it requires minimum " +
                                "platform version ${it.minimumPlatformVersion} (This node is running version ${versionInfo.platformVersion}).")
                        invalidCordapps.put("CorDapp requires minimumPlatformVersion: ${it.minimumPlatformVersion}, but was: ${versionInfo.platformVersion}", it.jarPath)
                        false
                    } else {
                        true
                    }
                }
                .filter {
                    if (signerKeyFingerprintBlacklist.isEmpty()) {
                        true //Nothing blacklisted, no need to check
                    } else {
                        val certificates = it.jarPath.openStream().let(::JarInputStream).use(JarSignatureCollector::collectCertificates)
                        val blockedCertificates = certificates.filter { it.publicKey.hash.sha256() in signerKeyFingerprintBlacklist }
                        if (certificates.isEmpty() || (certificates - blockedCertificates).isNotEmpty())
                            true // Cordapp is not signed or it is signed by at least one non-blacklisted certificate
                        else {
                            logger.warn("Not loading CorDapp ${it.info.shortName} (${it.info.vendor}) as it is signed by blacklisted key(s) only (probably development key): " +
                                    "${blockedCertificates.map { it.publicKey }}.")
                            invalidCordapps.put("Corresponding contracts are signed by blacklisted key(s) only (probably development key),", it.jarPath)
                            false
                        }
                    }
                }

        if (invalidCordapps.isNotEmpty()) {
            throw InvalidCordappException("Invalid Cordapps found, that couldn't be loaded: " +
                    "${invalidCordapps.map { "Problem: ${it.key} in Cordapp ${it.value}" }}, ")
        }

        cordapps.forEach(::register)
        return cordapps
    }

    private fun register(cordapp: Cordapp) {
        val contractClasses = cordapp.contractClassNames.toSet()
        val existingClasses = cordappClasses.keys
        val classesToRegister = cordapp.cordappClasses.toSet()
        val notAlreadyRegisteredClasses = classesToRegister - existingClasses
        val alreadyRegistered= HashMap(cordappClasses).apply { keys.retainAll(classesToRegister) }

        notAlreadyRegisteredClasses.forEach { cordappClasses[it] = setOf(cordapp) }

        for ((registeredClassName, registeredCordapps) in alreadyRegistered) {
            val duplicateCordapps = registeredCordapps.filter { it.jarHash == cordapp.jarHash }.toSet()

            if (duplicateCordapps.isNotEmpty()) {
                throw DuplicateCordappsInstalledException(cordapp, duplicateCordapps)
            }
            if (registeredClassName in contractClasses) {
                throw IllegalStateException("More than one CorDapp installed on the node for contract $registeredClassName. " +
                        "Please remove the previous version when upgrading to a new version.")
            }
            cordappClasses[registeredClassName] = registeredCordapps + cordapp
        }
    }

    private fun RestrictedScanResult.toCordapp(url: RestrictedURL): CordappImpl {
        val manifest: Manifest? = url.url.openStream().use { JarInputStream(it).manifest }
        val info = parseCordappInfo(manifest, CordappImpl.jarName(url.url))
        val minPlatformVersion = manifest?.get(CordappImpl.MIN_PLATFORM_VERSION)?.toIntOrNull() ?: 1
        val targetPlatformVersion = manifest?.get(CordappImpl.TARGET_PLATFORM_VERSION)?.toIntOrNull() ?: minPlatformVersion
        validateContractStateClassVersion(this)
        validateWhitelistClassVersion(this)
        return CordappImpl(
                findContractClassNamesWithVersionCheck(this),
                findInitiatedFlows(this),
                findRPCFlows(this),
                findServiceFlows(this),
                findSchedulableFlows(this),
                findServices(this),
                findWhitelists(url),
                findSerializers(this),
                findCheckpointSerializers(this),
                findCustomSchemas(this),
                findAllFlows(this),
                url.url,
                info,
                getJarHash(url.url),
                minPlatformVersion,
                targetPlatformVersion,
                findNotaryService(this),
                explicitCordappClasses = findAllCordappClasses(this)
        )
    }

    private fun parseCordappInfo(manifest: Manifest?, defaultName: String): Cordapp.Info {
        if (manifest == null) return UNKNOWN_INFO

        /** new identifiers (Corda 4) */
        // is it a Contract Jar?
        val contractInfo = if (manifest[CordappImpl.CORDAPP_CONTRACT_NAME] != null) {
            Cordapp.Info.Contract(
                    shortName = manifest[CordappImpl.CORDAPP_CONTRACT_NAME] ?: defaultName,
                    vendor = manifest[CordappImpl.CORDAPP_CONTRACT_VENDOR] ?: CordappImpl.UNKNOWN_VALUE,
                    versionId = parseVersion(manifest[CordappImpl.CORDAPP_CONTRACT_VERSION], CordappImpl.CORDAPP_CONTRACT_VERSION),
                    licence = manifest[CordappImpl.CORDAPP_CONTRACT_LICENCE] ?: CordappImpl.UNKNOWN_VALUE
            )
        } else {
            null
        }

        // is it a Workflow (flows and services) Jar?
        val workflowInfo = if (manifest[CordappImpl.CORDAPP_WORKFLOW_NAME] != null) {
            Cordapp.Info.Workflow(
                    shortName = manifest[CordappImpl.CORDAPP_WORKFLOW_NAME] ?: defaultName,
                    vendor = manifest[CordappImpl.CORDAPP_WORKFLOW_VENDOR] ?: CordappImpl.UNKNOWN_VALUE,
                    versionId = parseVersion(manifest[CordappImpl.CORDAPP_WORKFLOW_VERSION], CordappImpl.CORDAPP_WORKFLOW_VERSION),
                    licence = manifest[CordappImpl.CORDAPP_WORKFLOW_LICENCE] ?: CordappImpl.UNKNOWN_VALUE
            )
        } else {
            null
        }

        when {
            // combined Contract and Workflow Jar?
            contractInfo != null && workflowInfo != null -> return Cordapp.Info.ContractAndWorkflow(contractInfo, workflowInfo)
            contractInfo != null -> return contractInfo
            workflowInfo != null -> return workflowInfo
        }

        return Cordapp.Info.Default(
                shortName = manifest["Name"] ?: defaultName,
                vendor = manifest["Implementation-Vendor"] ?: CordappImpl.UNKNOWN_VALUE,
                version = manifest["Implementation-Version"] ?: CordappImpl.UNKNOWN_VALUE,
                licence = CordappImpl.UNKNOWN_VALUE
        )
    }

    private fun parseVersion(versionStr: String?, attributeName: String): Int {
        if (versionStr == null) {
            throw CordappInvalidVersionException(
                    "Target versionId attribute $attributeName not specified. Please specify a whole number starting from 1.",
                    CordappErrors.MISSING_VERSION_ATTRIBUTE,
                    listOf(attributeName))
        }
        val version = versionStr.toIntOrNull()
                ?: throw CordappInvalidVersionException(
                        "Version identifier ($versionStr) for attribute $attributeName must be a whole number starting from 1.",
                        CordappErrors.INVALID_VERSION_IDENTIFIER,
                        listOf(versionStr, attributeName))
        if (version < PlatformVersionSwitches.FIRST_VERSION) {
            throw CordappInvalidVersionException(
                    "Target versionId ($versionStr) for attribute $attributeName must not be smaller than 1.",
                    CordappErrors.INVALID_VERSION_IDENTIFIER,
                    listOf(versionStr, attributeName))
        }
        return version
    }

    private fun findNotaryService(scanResult: RestrictedScanResult): Class<out NotaryService>? {
        // Note: we search for implementations of both NotaryService and SinglePartyNotaryService as
        // the scanner won't find subclasses deeper down the hierarchy if any intermediate class is not
        // present in the CorDapp.
        val result = scanResult.getClassesWithSuperclass(NotaryService::class) +
                scanResult.getClassesWithSuperclass(SinglePartyNotaryService::class)
        if (result.isNotEmpty()) {
            logger.info("Found notary service CorDapp implementations: " + result.joinToString(", "))
        }
        return result.firstOrNull()
    }

    private fun getJarHash(url: URL): SecureHash.SHA256 = url.openStream().readFully().sha256()

    private fun findServices(scanResult: RestrictedScanResult): List<Class<out SerializeAsToken>> {
        return scanResult.getClassesWithAnnotation(SerializeAsToken::class, CordaService::class)
    }

    private fun findInitiatedFlows(scanResult: RestrictedScanResult): List<Class<out FlowLogic<*>>> {
        return scanResult.getClassesWithAnnotation(FlowLogic::class, InitiatedBy::class)
    }

    private fun Class<out FlowLogic<*>>.isUserInvokable(): Boolean {
        return Modifier.isPublic(modifiers) && !isLocalClass && !isAnonymousClass && (!isMemberClass || Modifier.isStatic(modifiers))
    }

    private fun findRPCFlows(scanResult: RestrictedScanResult): List<Class<out FlowLogic<*>>> {
        return scanResult.getClassesWithAnnotation(FlowLogic::class, StartableByRPC::class).filter { it.isUserInvokable() }
    }

    private fun findServiceFlows(scanResult: RestrictedScanResult): List<Class<out FlowLogic<*>>> {
        return scanResult.getClassesWithAnnotation(FlowLogic::class, StartableByService::class)
    }

    private fun findSchedulableFlows(scanResult: RestrictedScanResult): List<Class<out FlowLogic<*>>> {
        return scanResult.getClassesWithAnnotation(FlowLogic::class, SchedulableFlow::class)
    }

    private fun findAllFlows(scanResult: RestrictedScanResult): List<Class<out FlowLogic<*>>> {
        return scanResult.getConcreteClassesOfType(FlowLogic::class)
    }

    private fun findAllCordappClasses(scanResult: RestrictedScanResult): List<String> {
        return scanResult.getAllStandardClasses() + scanResult.getAllInterfaces()
    }

    private fun findContractClassNamesWithVersionCheck(scanResult: RestrictedScanResult): List<String> {
        val contractClasses = coreContractClasses.flatMapTo(LinkedHashSet()) { scanResult.getNamesOfClassesImplementingWithClassVersionCheck(it) }.toList()
        for (contractClass in contractClasses) {
            contractClass.warnContractWithoutConstraintPropagation(appClassLoader)
        }
        return contractClasses
    }

    private fun validateContractStateClassVersion(scanResult: RestrictedScanResult) {
        coreContractClasses.forEach { scanResult.versionCheckClassesImplementing(it) }
    }

    private fun validateWhitelistClassVersion(scanResult: RestrictedScanResult) {
        scanResult.versionCheckClassesImplementing(SerializationWhitelist::class)
    }

    private fun findWhitelists(cordappJarPath: RestrictedURL): List<SerializationWhitelist> {
        val whitelists = ServiceLoader.load(SerializationWhitelist::class.java, appClassLoader).toList()
        return whitelists.filter {
            it.javaClass.location == cordappJarPath.url && it.javaClass.name.startsWith(cordappJarPath.qualifiedNamePrefix)
        } + DefaultWhitelist // Always add the DefaultWhitelist to the whitelist for an app.
    }

    private fun findSerializers(scanResult: RestrictedScanResult): List<SerializationCustomSerializer<*, *>> {
        return scanResult.getClassesImplementingWithClassVersionCheck(SerializationCustomSerializer::class)
    }

    private fun findCheckpointSerializers(scanResult: RestrictedScanResult): List<CheckpointCustomSerializer<*, *>> {
        return scanResult.getClassesImplementingWithClassVersionCheck(CheckpointCustomSerializer::class)
    }

    private fun findCustomSchemas(scanResult: RestrictedScanResult): Set<MappedSchema> {
        return scanResult.getClassesWithSuperclass(MappedSchema::class).instances().toSet()
    }

    private fun scanCordapp(cordappJarPath: RestrictedURL): RestrictedScanResult {
        val cordappElement = cordappJarPath.url.toString()
        logger.info("Scanning CorDapp in $cordappElement")
        val scanResult = ClassGraph()
            .filterClasspathElementsByURL { elt -> elt == cordappJarPath.url }
            .overrideClassLoaders(appClassLoader)
            .ignoreParentClassLoaders()
            .enableAllInfo()
            .pooledScan()
        return RestrictedScanResult(scanResult, cordappJarPath.qualifiedNamePrefix, cordappJarPath)
    }

    private fun <T : Any> loadClass(className: String, type: KClass<T>): Class<out T>? {
        return try {
            Class.forName(className, false, appClassLoader).asSubclass(type.java)
        } catch (e: ClassCastException) {
            logger.warn("As $className must be a sub-type of ${type.java.name}")
            null
        } catch (e: Exception) {
            logger.warn("Unable to load class $className", e)
            null
        }
    }

    // TODO Remove this class as rootPackageName is never non-null.
    /** @property rootPackageName only this package and subpackages may be extracted from [url], or null to allow all packages. */
    private data class RestrictedURL(val url: URL, val rootPackageName: String?) {
        val qualifiedNamePrefix: String get() = rootPackageName?.let { "$it." } ?: ""
    }

    private fun <T : Any> List<Class<out T>>.instances(): List<T> {
        return map { it.kotlin.objectOrNewInstance() }
    }

    private inner class RestrictedScanResult(private val scanResult: ScanResult, private val qualifiedNamePrefix: String,
                                             private val cordappJarPath: RestrictedURL) : AutoCloseable {

        fun getNamesOfClassesImplementingWithClassVersionCheck(type: KClass<*>): List<String> {
            return scanResult.getClassesImplementing(type.java.name).filter { it.name.startsWith(qualifiedNamePrefix) }.map {
                validateClassFileVersion(it)
                it.name
            }
        }

        fun versionCheckClassesImplementing(type: KClass<*>) {
            return scanResult.getClassesImplementing(type.java.name).filter { it.name.startsWith(qualifiedNamePrefix) }.forEach {
                validateClassFileVersion(it)
            }
        }

        fun <T : Any> getClassesWithSuperclass(type: KClass<T>): List<Class<out T>> {
            return scanResult
                    .getSubclasses(type.java.name)
                    .names
                    .filter { it.startsWith(qualifiedNamePrefix) }
                    .mapNotNull { loadClass(it, type) }
                    .filterNot { it.isAbstractClass }
        }

        fun <T : Any> getClassesImplementingWithClassVersionCheck(type: KClass<T>): List<T> {
            return scanResult
                    .getClassesImplementing(type.java.name)
                    .filter { it.name.startsWith(qualifiedNamePrefix) }
                    .mapNotNull {
                        validateClassFileVersion(it)
                        loadClass(it.name, type) }
                    .filterNot { it.isAbstractClass }
                    .map { it.kotlin.objectOrNewInstance() }
        }

        fun <T : Any> getClassesWithAnnotation(type: KClass<T>, annotation: KClass<out Annotation>): List<Class<out T>> {
            return scanResult
                    .getClassesWithAnnotation(annotation.java.name)
                    .names
                    .filter { it.startsWith(qualifiedNamePrefix) }
                    .mapNotNull { loadClass(it, type) }
                    .filterNot { Modifier.isAbstract(it.modifiers) }
        }

        fun <T : Any> getConcreteClassesOfType(type: KClass<T>): List<Class<out T>> {
            return scanResult
                    .getSubclasses(type.java.name)
                    .names
                    .filter { it.startsWith(qualifiedNamePrefix) }
                    .mapNotNull { loadClass(it, type) }
                    .filterNot { it.isAbstractClass }
        }

        fun getAllStandardClasses(): List<String> {
            return scanResult
                    .allStandardClasses
                    .names
                    .filter { it.startsWith(qualifiedNamePrefix) }
        }

        fun getAllInterfaces(): List<String> {
            return scanResult
                    .allInterfaces
                    .names
                    .filter { it.startsWith(qualifiedNamePrefix) }
        }

        private fun validateClassFileVersion(classInfo: ClassInfo) {
            if (classInfo.classfileMajorVersion < JDK1_2_CLASS_FILE_FORMAT_MAJOR_VERSION ||
                classInfo.classfileMajorVersion > JDK8_CLASS_FILE_FORMAT_MAJOR_VERSION)
                    throw IllegalStateException("Class ${classInfo.name} from jar file ${cordappJarPath.url} has an invalid version of " +
                            "${classInfo.classfileMajorVersion}")
        }

        override fun close() = scanResult.close()
    }
}

/**
 * Thrown when scanning CorDapps.
 */
class MultipleCordappsForFlowException(
        message: String,
        flowName: String,
        jars: String
) : CordaRuntimeException(message), ErrorCode<CordappErrors> {
    override val code = CordappErrors.MULTIPLE_CORDAPPS_FOR_FLOW
    override val parameters = listOf(flowName, jars)
}

/**
 * Thrown if an exception occurs whilst parsing version identifiers within cordapp configuration
 */
class CordappInvalidVersionException(
        msg: String,
        override val code: CordappErrors,
        override val parameters: List<Any> = listOf()
) : CordaRuntimeException(msg), ErrorCode<CordappErrors>

/**
 * Thrown if duplicate CorDapps are installed on the node
 */
class DuplicateCordappsInstalledException(app: Cordapp, duplicates: Set<Cordapp>)
    : CordaRuntimeException("IllegalStateExcepion", "The CorDapp (name: ${app.info.shortName}, file: ${app.name}) " +
        "is installed multiple times on the node. The following files correspond to the exact same content: " +
        "${duplicates.map { it.name }}", null), ErrorCode<CordappErrors> {
    override val code = CordappErrors.DUPLICATE_CORDAPPS_INSTALLED
    override val parameters = listOf(app.info.shortName, app.name, duplicates.map { it.name })
}

/**
 * Thrown if an exception occurs during loading cordapps.
 */
class InvalidCordappException(message: String) : CordaRuntimeException(message)

abstract class CordappLoaderTemplate : CordappLoader {

    companion object {

        private val logger = contextLogger()
    }

    override val flowCordappMap: Map<Class<out FlowLogic<*>>, Cordapp> by lazy {
        cordapps.flatMap { corDapp -> corDapp.allFlows.map { flow -> flow to corDapp } }
                .groupBy { it.first }
                .mapValues { entry ->
                    if (entry.value.size > 1) {
                        logger.error("There are multiple CorDapp JARs on the classpath for flow " +
                                "${entry.value.first().first.name}: [ ${entry.value.joinToString { it.second.jarPath.toString() }} ].")
                        entry.value.forEach { (_, cordapp) ->
                            ZipInputStream(cordapp.jarPath.openStream()).use { zip ->
                                val ident = BigInteger(64, Random()).toString(36)
                                logger.error("Contents of: ${cordapp.jarPath} will be prefaced with: $ident")
                                var e = zip.nextEntry
                                while (e != null) {
                                    logger.error("$ident\t ${e.name}")
                                    e = zip.nextEntry
                                }
                            }
                        }
                        throw MultipleCordappsForFlowException("There are multiple CorDapp JARs on the classpath for flow " +
                                "${entry.value.first().first.name}: [ ${entry.value.joinToString { it.second.jarPath.toString() }} ].",
                                entry.value.first().first.name,
                                entry.value.joinToString { it.second.jarPath.toString() })
                    }
                    entry.value.single().second
                }
    }

    override val cordappSchemas: Set<MappedSchema> by lazy {
        cordapps.flatMap { it.customSchemas }.toSet()
    }
}
