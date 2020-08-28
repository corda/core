package net.corda.tools.shell

import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.messaging.ClientRpcSslOptions
import java.nio.file.Path

data class ShellConfiguration(
        val commandsDirectory: Path,
        val cordappsDirectory: Path? = null,
        var user: String = "",
        var password: String = "",
        var permissions: Set<String>? = null,
        var localShellAllowExitInSafeMode: Boolean = false,
        var localShellUnsafe: Boolean = false,
        val hostAndPort: NetworkHostAndPort,
        val ssl: ClientRpcSslOptions? = null,
        val sshdPort: Int? = null,
        val sshHostKeyDirectory: Path? = null,
        val noLocalShell: Boolean = false,
        val lowMemoryMode: Boolean = false) {
    companion object {
        const val COMMANDS_DIR = "shell-commands"
        const val CORDAPPS_DIR = "cordapps"
        const val SSHD_HOSTKEY_DIR = "ssh"
    }
}
