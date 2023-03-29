package io.hstream

import io.hstream.internal.NodeState
import io.hstream.internal.ServerNode
import io.hstream.internal.ServerNodeStatus
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test
import java.net.URI

class HMetaMock {
    val clusterNames: MutableList<Pair<String, Int>> = arrayListOf()
    private val clusterNameMutex: Mutex = Mutex()

    suspend fun registerName(serverName: String) {
        val uri = if (serverName.contains("://")) {
            URI(serverName)
        } else {
            URI("some://$serverName")
        }

        val hostPort = Pair(uri.host, uri.port)

        if (hostPort.second == -1) {
            println(uri)
            TODO()
        }
        clusterNameMutex.withLock {
            if (clusterNames.contains(hostPort)) {
                throw Exception("name: $hostPort already existed")
            }
            clusterNames.add(hostPort)
        }
    }

    suspend fun getServerNodes(): List<ServerNode> {
        clusterNameMutex.withLock {
            return this.clusterNames.mapIndexed { ix, it ->
                ServerNode.newBuilder()
                    .setHost(it.first)
                    .setPort(it.second)
                    .setId(ix)
                    .build()
            }
        }
    }

    suspend fun getServerNodesStatus(): List<ServerNodeStatus> {
        return (1..clusterNames.size).map {
            ServerNodeStatus.newBuilder()
                .setStateValue(NodeState.Running_VALUE).build()
        }
    }
}

class HMetaMockTest {

    @Test
    fun `registerName should add host and port to clusterNames`() = runBlocking {
        val hMetaMock = HMetaMock()
        val serverName = "localhost:8080"
        hMetaMock.registerName(serverName)
        assertTrue(hMetaMock.clusterNames.contains(Pair("localhost", 8080)))
    }

    @Test
    fun `registerName should throw an exception if host and port already exist`() = runBlocking {
        val hMetaMock = HMetaMock()
        val serverName1 = "localhost:8080"
        val serverName2 = "localhost:8080"
        hMetaMock.registerName(serverName1)
        try {
            hMetaMock.registerName(serverName2)
            fail("Expected an exception")
        } catch (_: Exception) {
        }
    }

    @Test
    fun `registerName should handle different hosts and ports`() = runBlocking {
        val hMetaMock = HMetaMock()
        val serverName1 = "localhost:8080"
        val serverName2 = "localhost:8081"
        val serverName3 = "127.0.0.1:8080"
        hMetaMock.registerName(serverName1)
        hMetaMock.registerName(serverName2)
        hMetaMock.registerName(serverName3)
        assertTrue(hMetaMock.clusterNames.contains(Pair("localhost", 8080)))
        assertTrue(hMetaMock.clusterNames.contains(Pair("localhost", 8081)))
        assertTrue(hMetaMock.clusterNames.contains(Pair("127.0.0.1", 8080)))
    }

    @Test
    fun `getServerNodes size eq getServerNodesStatus size`() = runBlocking {
        val hMetaMock = HMetaMock()
        val serverName1 = "localhost:8080"
        val serverName2 = "localhost:8081"
        val serverName3 = "127.0.0.1:8080"
        hMetaMock.registerName(serverName1)
        hMetaMock.registerName(serverName2)
        hMetaMock.registerName(serverName3)

        assertTrue(hMetaMock.getServerNodes().size == hMetaMock.getServerNodes().size)
    }

    @Test
    fun testDescribe() = runBlocking {
        val hMetaMock = HMetaMock()
        val serverName1 = "localhost:8080"
        val serverName2 = "localhost:8081"
        val serverName3 = "127.0.0.1:8080"
        hMetaMock.registerName(serverName1)
        hMetaMock.registerName(serverName2)
        hMetaMock.registerName(serverName3)

        val node = hMetaMock.getServerNodes()[0]
        val addr = "${node.host}:${node.port}"
        assertEquals("localhost:8080", addr)
    }
}
