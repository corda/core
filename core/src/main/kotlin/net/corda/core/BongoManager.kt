package net.corda.core

import java.security.AccessControlException
import java.security.Permission

@Suppress("unused")
class BongoManager : SecurityManager() {
    override fun checkExit(status: Int) {
        if (status != 0) {
            val callStack = classContext.joinToString(
                separator = System.lineSeparator(),
                prefix = System.lineSeparator(),
                transform = Class<*>::getName
            )

            System.err.println("EXIT-CODE=$status$callStack")
            throw AccessControlException("NAUGHTY! (I'll give you 'exitCode=$status'...!!!)$callStack")
        }
    }

    override fun checkPermission(perm: Permission?) {
    }
}