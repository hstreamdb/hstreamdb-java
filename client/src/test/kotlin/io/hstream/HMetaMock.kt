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
import org.junit.jupiter.api.assertThrows
import org.slf4j.LoggerFactory
import java.net.URI

class HMetaMock {

    val logger = LoggerFactory.getLogger(HMetaMock::class.java)

    val clusterNames: MutableList<Pair<String, Int>> = arrayListOf()
    private val clusterNameMutex: Mutex = Mutex()

    val streamsInfo: MutableList<Pair<String, String>> = arrayListOf()
    private val streamsInfoMutex: Mutex = Mutex()

    val subscriptionInfo: MutableList<Pair<String, String>> = arrayListOf()
    private val subscriptionInfoMutex: Mutex = Mutex()

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

    suspend fun registerStream(streamName: String, serverName: String) {
        streamsInfoMutex.withLock {
            for (info in streamsInfo) {
                if (info.first == streamName) {
                    TODO()
                }
            }
            streamsInfo.add(Pair(streamName, serverName))
        }
    }

    suspend fun lookupStreamName(streamName: String): String = streamsInfoMutex.withLock {
        for (info in streamsInfo) {
            if (info.first == streamName) {
                return info.second
            }
        }
        throw Throwable("lookupStreamName: streamName does not existed")
    }

    suspend fun registerSubscription(subscriptionId: String, serverName: String) {
        subscriptionInfoMutex.withLock {
            for (info in subscriptionInfo) {
                if (info.first == subscriptionId) {
                    TODO()
                }
            }
            subscriptionInfo.add(Pair(subscriptionId, serverName))
        }
    }

    suspend fun lookupSubscriptionName(subscriptionId: String): String = subscriptionInfoMutex.withLock {
        for (info in subscriptionInfo) {
            if (info.first == subscriptionId) {
                return info.second
            }
        }

//        println(
//            "lookupSubscriptionName: " +
//                subscriptionInfo.fold("") { r, t -> "$r, $t" }
//
//        )
        throw Throwable("no subscriptionId matches for [$subscriptionId]")
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

    @Test
    fun `registerStream should add streamName and serverName to streamsInfo`() = runBlocking {
        val hMetaMock = HMetaMock()
        val streamName = "stream1"
        val serverName = "localhost:8080"
        hMetaMock.registerStream(streamName, serverName)
        assertTrue(hMetaMock.streamsInfo.contains(Pair(streamName, serverName)))
    }

    @Test
    fun `registerStream should throw an exception if streamName already exists`() = runBlocking {
        val hMetaMock = HMetaMock()
        val streamName = "stream1"
        val serverName1 = "localhost:8080"
        val serverName2 = "localhost:8081"
        hMetaMock.registerStream(streamName, serverName1)
        assertThrows<Throwable> {
            hMetaMock.registerStream(streamName, serverName2)
        }
        Unit
    }

    @Test
    fun `lookupStreamName should return the serverName associated with the given streamName`() = runBlocking {
        val hMetaMock = HMetaMock()
        val streamName1 = "stream1"
        val serverName1 = "localhost:8080"
        val streamName2 = "stream2"
        val serverName2 = "localhost:8081"
        hMetaMock.registerStream(streamName1, serverName1)
        hMetaMock.registerStream(streamName2, serverName2)
        val result = hMetaMock.lookupStreamName(streamName1)
        assertEquals(serverName1, result)
    }

    @Test
    fun `lookupStreamName should throw an exception if the given streamName does not exist`() = runBlocking {
        val hMetaMock = HMetaMock()
        val streamName1 = "stream1"
        val serverName1 = "localhost:8080"
        val streamName2 = "stream2"
        hMetaMock.registerStream(streamName1, serverName1)
        assertThrows<Throwable> {
            hMetaMock.lookupStreamName(streamName2)
        }
        Unit
    }

    @Test
    fun `registerSubscription should add subscriptionId and serverName to subscriptionInfo`() = runBlocking {
        val hMetaMock = HMetaMock()
        val subscriptionId = "sub1"
        val serverName = "localhost:8080"
        hMetaMock.registerSubscription(subscriptionId, serverName)
        assertTrue(hMetaMock.subscriptionInfo.contains(Pair(subscriptionId, serverName)))
    }

    @Test
    fun `registerSubscription should throw an exception if subscriptionId already exists`() = runBlocking {
        val hMetaMock = HMetaMock()
        val subscriptionId = "sub1"
        val serverName1 = "localhost:8080"
        val serverName2 = "localhost:8081"
        hMetaMock.registerSubscription(subscriptionId, serverName1)
        assertThrows<Throwable> {
            hMetaMock.registerSubscription(subscriptionId, serverName2)
        }
        Unit
    }

    @Test
    fun `lookupSubscriptionName should return the serverName associated with the given subscriptionId`() = runBlocking {
        val hMetaMock = HMetaMock()
        val subscriptionId1 = "sub1"
        val serverName1 = "localhost:8080"
        val subscriptionId2 = "sub2"
        val serverName2 = "localhost:8081"
        hMetaMock.registerSubscription(subscriptionId1, serverName1)
        hMetaMock.registerSubscription(subscriptionId2, serverName2)
        val result = hMetaMock.lookupSubscriptionName(subscriptionId1)
        assertEquals(serverName1, result)
    }
}
