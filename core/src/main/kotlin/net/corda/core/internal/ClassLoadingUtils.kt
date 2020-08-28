package net.corda.core.internal

import io.github.classgraph.ClassGraph
import io.github.classgraph.ClassInfo
import net.corda.core.StubOutForDJVM
import net.corda.core.serialization.internal.AttachmentURLStreamHandlerFactory.attachmentScheme

class ClassGraphConstants {
    companion object {
        const val DEFAULT_MAX_BUFFERED_JAR_SIZE = 64 * 1024 * 1024
        const val LOW_MEMORY_MODE_MAX_BUFFERED_JAR_SIZE = 1024 * 1024
    }
}

/**
 * Creates instances of all the classes in the classpath of the provided classloader, which implement the interface of the provided class.
 * @param classloader the classloader, which will be searched for the classes.
 * @param clazz the class of the interface, which the classes - to be returned - must implement.
 * @param classVersionRange if specified an exception is raised if class version is not within the passed range.
 *
 * @return instances of the identified classes.
 * @throws IllegalArgumentException if the classes found do not have proper constructors.
 * @throws UnsupportedClassVersionError if the class version is not within range.
 *
 * Note: In order to be instantiated, the associated classes must:
 * - be non-abstract
 * - either be a Kotlin object or have a constructor with no parameters (or only optional ones)
 */
@StubOutForDJVM
fun <T: Any> createInstancesOfClassesImplementing(classloader: ClassLoader, clazz: Class<T>,
                                                  classVersionRange: IntRange? = null, lowMemoryMode: Boolean = false): Set<T> {
    return getNamesOfClassesImplementing(classloader, clazz, classVersionRange, lowMemoryMode)
        .map { classloader.loadClass(it).asSubclass(clazz) }
        .mapTo(LinkedHashSet()) { it.kotlin.objectOrNewInstance() }
}

/**
 * Scans for all the non-abstract classes in the classpath of the provided classloader which implement the interface of the provided class.
 * @param classloader the classloader, which will be searched for the classes.
 * @param clazz the class of the interface, which the classes - to be returned - must implement.
 * @param classVersionRange if specified an exception is raised if class version is not within the passed range.
 *
 * @return names of the identified classes.
 * @throws UnsupportedClassVersionError if the class version is not within range.
 */
@StubOutForDJVM
fun <T: Any> getNamesOfClassesImplementing(classloader: ClassLoader, clazz: Class<T>,
                                           classVersionRange: IntRange? = null, lowMemoryMode: Boolean): Set<String> {
    return ClassGraph().overrideClassLoaders(classloader)
        .enableURLScheme(attachmentScheme)
        .setMaxBufferedJarRAMSize(if (lowMemoryMode) ClassGraphConstants.LOW_MEMORY_MODE_MAX_BUFFERED_JAR_SIZE else ClassGraphConstants.DEFAULT_MAX_BUFFERED_JAR_SIZE)
        .ignoreParentClassLoaders()
        .enableClassInfo()
        .pooledScan()
        .use { result ->
            classVersionRange?.let {
                result.allClasses.firstOrNull { c -> c.classfileMajorVersion !in classVersionRange }?.also {
                    throw UnsupportedClassVersionError("Class ${it.name} found in ${it.classpathElementURL} " +
                            "has an unsupported class version of ${it.classfileMajorVersion}")
                }
            }
            result.getClassesImplementing(clazz.name)
                .filterNot(ClassInfo::isAbstract)
                .mapTo(LinkedHashSet(), ClassInfo::getName)
        }
}

fun <T: Any?> executeWithThreadContextClassLoader(classloader: ClassLoader, fn: () -> T): T {
    val threadClassLoader = Thread.currentThread().contextClassLoader
    try {
        Thread.currentThread().contextClassLoader = classloader
        return fn()
    } finally {
        Thread.currentThread().contextClassLoader = threadClassLoader
    }

}
