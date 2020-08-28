package net.corda.nodeapi.internal

object ArtemisConstants {
    const val MESSAGE_ID_KEY = "_AMQ_DUPL_ID"
    const val DEFAULT_THREAD_POOL_MAX_SIZE = 5
    const val LOW_MEMORY_MODE_THREAD_POOL_MAX_SIZE = 2
    const val DEFAULT_ID_CACHE_SIZE = 2000
    const val LOW_MEMORY_MODE_ID_CACHE_SIZE = 125
    const val DEFAULT_REMOTING_THREADS_NUMBER_MULTIPLIER = 3
    const val LOW_MEMORY_MODE_REMOTING_THREADS_NUMBER = 2
}