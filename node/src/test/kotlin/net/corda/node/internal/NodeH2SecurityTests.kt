package net.corda.node.internal

import com.nhaarman.mockito_kotlin.atLeast
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import net.corda.core.serialization.SerializeAsToken
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.node.VersionInfo
import net.corda.node.services.config.NodeConfiguration
import net.corda.node.services.config.NodeH2Settings
import net.corda.node.services.events.NodeSchedulerService
import net.corda.node.services.messaging.MessagingService
import net.corda.node.services.statemachine.StateMachineManager
import net.corda.nodeapi.internal.cryptoservice.CryptoService
import net.corda.nodeapi.internal.persistence.CouldNotCreateDataSourceException
import net.corda.nodeapi.internal.persistence.DatabaseConfig
import org.assertj.core.api.Assertions.assertThat
import org.h2.tools.Server
import org.junit.Test
import java.net.InetAddress
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.util.*
import java.util.concurrent.ExecutorService
import javax.sql.DataSource
import kotlin.test.assertFailsWith

class NodeH2SecurityTests {

    @Test(timeout=300_000)
    fun `h2 server on the host name requires non-default database password`() {
        hikaryProperties.setProperty("dataSource.url", "jdbc:h2:file:my_file")
        hikaryProperties.setProperty("dataSource.password", "")
        address = NetworkHostAndPort(InetAddress.getLocalHost().hostName, 1080)
        val node = MockNode()

        val exception = assertFailsWith(CouldNotCreateDataSourceException::class) {
            node.startDb()
        }
        assertThat(exception.message).contains("Database password is required for H2 server listening on ")
    }

    @Test(timeout=300_000)
    fun `h2 server on the host IP requires non-default database password`() {
        hikaryProperties.setProperty("dataSource.url", "jdbc:h2:file:my_file")
        hikaryProperties.setProperty("dataSource.password", "")
        address = NetworkHostAndPort(InetAddress.getLocalHost().hostAddress, 1080)
        val node = MockNode()

        val exception = assertFailsWith(CouldNotCreateDataSourceException::class) {
            node.startDb()
        }
        assertThat(exception.message).contains("Database password is required for H2 server listening on")
    }

    @Test(timeout=300_000)
    fun `h2 server on the host name requires non-blank database password`() {
        hikaryProperties.setProperty("dataSource.url", "jdbc:h2:file:my_file")
        hikaryProperties.setProperty("dataSource.password", " ")
        address = NetworkHostAndPort(InetAddress.getLocalHost().hostName, 1080)
        val node = MockNode()

        val exception = assertFailsWith(CouldNotCreateDataSourceException::class) {
            node.startDb()
        }
        assertThat(exception.message).contains("Database password is required for H2 server listening on")
    }

    @Test(timeout=300_000)
    fun `h2 server on the host IP requires non-blank database password`() {
        hikaryProperties.setProperty("dataSource.url", "jdbc:h2:file:my_file")
        hikaryProperties.setProperty("dataSource.password", " ")
        address = NetworkHostAndPort(InetAddress.getLocalHost().hostAddress, 1080)
        val node = MockNode()

        val exception = assertFailsWith(CouldNotCreateDataSourceException::class) {
            node.startDb()
        }

        assertThat(exception.message).contains("Database password is required for H2 server listening on")
    }

    @Test(timeout=300_000)
    fun `h2 server on localhost runs with the default database password`() {
        hikaryProperties.setProperty("dataSource.url", "jdbc:h2:file:dir/file;")
        hikaryProperties.setProperty("dataSource.password", "")
        address = NetworkHostAndPort("localhost", 80)

        val node = MockNode()
        node.startDb()

        verify(dataSource, atLeast(1)).connection
    }

    @Test(timeout=300_000)
    fun `h2 server to loopback IP runs with the default database password`() {
        hikaryProperties.setProperty("dataSource.url", "jdbc:h2:file:dir/file;")
        hikaryProperties.setProperty("dataSource.password", "")
        address = NetworkHostAndPort("127.0.0.1", 80)

        val node = MockNode()
        node.startDb()

        verify(dataSource, atLeast(1)).connection
    }

    @Test(timeout=300_000)
    fun `h2 server set allowedClasses system properties`() {
        System.setProperty("h2.allowedClasses", "*")
        hikaryProperties.setProperty("dataSource.url", "jdbc:h2:file:dir/file;")
        hikaryProperties.setProperty("dataSource.password", "")
        address = NetworkHostAndPort("127.0.0.1", 80)

        val node = MockNode()
        node.startDb()

        val allowClasses = System.getProperty("h2.allowedClasses").split(",")
        assertThat(allowClasses).contains("org.h2.mvstore.db.MVTableEngine",
                "org.locationtech.jts.geom.Geometry" ,
                "org.h2.server.TcpServer")
        assertThat(allowClasses).doesNotContain("*")
    }

    private val config = mock<NodeConfiguration>()
    private val hikaryProperties = Properties()
    private val database = DatabaseConfig()
    private var address: NetworkHostAndPort? = null
    val dataSource = mock<DataSource>()

    init {
        whenever(config.database).thenReturn(database)
        whenever(config.dataSourceProperties).thenReturn(hikaryProperties)
        whenever(config.baseDirectory).thenReturn(mock())
        whenever(config.effectiveH2Settings).thenAnswer { NodeH2Settings(address) }
    }

    private inner class MockNode: Node(config, VersionInfo.UNKNOWN, false) {
        fun startDb() = startDatabase()

        override fun makeMessagingService(): MessagingService {
            val service = mock<MessagingService>(extraInterfaces = arrayOf(SerializeAsToken::class))
            whenever(service.activeChange).thenReturn(mock())
            return service
        }

        override fun makeStateMachineManager(): StateMachineManager = mock()

        override fun createExternalOperationExecutor(numberOfThreads: Int): ExecutorService = mock()

        override fun makeCryptoService(): CryptoService = mock()

        override fun makeMembershipGroupUpdater(): LifecycleSupport = mock()

        override fun makeNodeSchedulerService(): NodeSchedulerService = mock()

        override fun startHikariPool() {
            val connection = mock<Connection>()
            val metaData = mock<DatabaseMetaData>()
            whenever(dataSource.connection).thenReturn(connection)
            whenever(connection.metaData).thenReturn(metaData)
            database.start(dataSource)
        }

        override fun createH2Server(baseDir: String, databaseName: String, port: Int): Server {
            val server = mock<Server>()
            whenever(server.start()).thenReturn(server)
            whenever(server.url).thenReturn("")
            return server
        }
    }
}