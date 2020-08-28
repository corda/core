package net.corda.node.services.config.shell

import net.corda.core.internal.div
import net.corda.node.internal.clientSslOptionsCompatibleWith
import net.corda.node.services.config.NodeConfiguration
import net.corda.tools.shell.ShellConfiguration
import net.corda.tools.shell.ShellConfiguration.Companion.COMMANDS_DIR
import net.corda.tools.shell.ShellConfiguration.Companion.CORDAPPS_DIR
import net.corda.tools.shell.ShellConfiguration.Companion.SSHD_HOSTKEY_DIR

//re-packs data to Shell specific classes
fun NodeConfiguration.toShellConfig() = ShellConfiguration(
        commandsDirectory = this.baseDirectory / COMMANDS_DIR,
        cordappsDirectory = this.baseDirectory.toString() / CORDAPPS_DIR,
        user = INTERNAL_SHELL_USER,
        password = internalShellPassword,
        permissions = internalShellPermissions(!this.localShellUnsafe),
        localShellAllowExitInSafeMode = this.localShellAllowExitInSafeMode,
        localShellUnsafe = this.localShellUnsafe,
        hostAndPort = this.rpcOptions.address,
        ssl = clientSslOptionsCompatibleWith(this.rpcOptions),
        sshdPort = this.sshd?.port,
        sshHostKeyDirectory = this.baseDirectory / SSHD_HOSTKEY_DIR,
        noLocalShell = this.noLocalShell,
        lowMemoryMode = this.lowMemoryMode)
