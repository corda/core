package net.corda.node.internal

interface LifecycleSupport : Startable, Stoppable {
    fun restart() {
        stop()
        start()
    }
}

interface Stoppable : AutoCloseable {
    fun stop()

    override fun close() = stop()
}

interface Startable {
    fun start()

    val started: Boolean
}

interface Connectable {

    val connected: Boolean

    fun connect()

    fun disconnect()

    fun reconnect() {
        disconnect()
        connect()
    }
}