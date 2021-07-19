@file:JvmName("CordappDependencies")
package net.corda.vega.contracts

import net.corda.core.crypto.SecureHash
import net.corda.core.transactions.TransactionBuilder
import java.net.URLClassLoader

/**
 * This is a crude, home-grown Cordapp dependency mechanism.
 */
private val EXTERNAL_CORDAPPS: List<SecureHash> by lazy {
    loadDependencies()
}

/**
 * Locate any META-INF/Cordapp-Dependencies file that
 * is part of this particular CorDapp.
 */
private fun loadDependencies(): List<SecureHash> {
    val cordappURL = object {}::class.java.protectionDomain.codeSource.location
    return URLClassLoader(arrayOf(cordappURL), null).use { cl ->
        val deps = cl.getResource("META-INF/Cordapp-Dependencies") ?: return emptyList()
        deps.openStream().bufferedReader().useLines { lines ->
            lines.filterNot(String::isBlank).map(SecureHash.Companion::create).toList()
        }
    }
}

fun TransactionBuilder.withExternalCordapps(): TransactionBuilder {
    for (cordapp in EXTERNAL_CORDAPPS) {
        addAttachment(cordapp)
    }
    return this
}
