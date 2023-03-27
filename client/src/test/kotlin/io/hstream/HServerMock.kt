package io.hstream

import com.google.protobuf.Empty
import io.grpc.ManagedChannel
import io.grpc.Status
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.stub.StreamObserver
import io.grpc.testing.GrpcCleanupRule
import io.hstream.impl.ChannelProviderImpl
import io.hstream.impl.HStreamClientBuilderImpl
import io.hstream.impl.HStreamClientKtImpl
import io.hstream.impl.logger
import io.hstream.impl.unaryCallWithCurrentUrls
import io.hstream.internal.DeleteStreamRequest
import io.hstream.internal.DescribeClusterResponse
import io.hstream.internal.HStreamApiGrpc
import io.hstream.internal.ListStreamsRequest
import io.hstream.internal.ListStreamsResponse
import io.hstream.internal.ServerNode
import io.hstream.util.GrpcUtils
import io.hstream.util.UrlSchemaUtils.parseServerUrls
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.net.URI
import java.util.concurrent.CompletionException
import kotlin.random.Random

class HServerMock(
    private val hMetaMockCluster: List<HMetaMock>
) : HStreamApiGrpc.HStreamApiImplBase() {
    private val logger = LoggerFactory.getLogger(HServerMock::class.java)

    private val streams: MutableList<Stream> = arrayListOf()
    private val streamsMutex: Mutex = Mutex()

    private val subscriptions: MutableList<Subscription> = arrayListOf()
    private val subscriptionsMutex: Mutex = Mutex()

    override fun describeCluster(request: Empty?, responseObserver: StreamObserver<DescribeClusterResponse>?) = runBlocking {
        val serverNodes: List<ServerNode> = hMetaMockCluster[0].getServerNodes()
        val serverNodesStatus = hMetaMockCluster[0].getServerNodesStatus()
        val resp = DescribeClusterResponse
            .newBuilder()

        for (i in 1..serverNodes.size) {
            val finalI = i - 1
            resp
                .addServerNodes(serverNodes[finalI])
                .addServerNodesStatus(serverNodesStatus[finalI])
        }

        responseObserver?.onNext(resp.build())
        responseObserver?.onCompleted()
        Unit
    }

    override fun createStream(
        request: io.hstream.internal.Stream?,
        responseObserver: StreamObserver<io.hstream.internal.Stream>?
    ) {
        runBlocking {
            val newStream = GrpcUtils.streamFromGrpc(request)

            streamsMutex.withLock {
                streams.add(newStream)
            }

            responseObserver?.onNext(GrpcUtils.streamToGrpc(newStream))
            responseObserver?.onCompleted()
        }
    }

    override fun listStreams(
        request: ListStreamsRequest?,
        responseObserver: StreamObserver<ListStreamsResponse>?
    ) {
        runBlocking {
            val response = streamsMutex.withLock {
                ListStreamsResponse.newBuilder()
                    .addAllStreams(
                        streams.map {
                            GrpcUtils.streamToGrpc(it)
                        }
                    )
                    .build()
            }

            responseObserver?.onNext(response)
            responseObserver?.onCompleted()
        }
    }

    override fun deleteStream(
        request: DeleteStreamRequest?,
        responseObserver: StreamObserver<Empty>?
    ) {
        runBlocking {
            val streamName = request?.streamName ?: ""
            var deletedStream: Stream? = null

            streamsMutex.withLock {
                streams.find { it.streamName == streamName }?.let { stream ->
                    streams.remove(stream)
                    deletedStream = stream
                }
            }

            if (deletedStream == null) {
                responseObserver?.onError(Status.NOT_FOUND.asException())
            } else {
                responseObserver?.onNext(Empty.getDefaultInstance())
                responseObserver?.onCompleted()
            }
        }
    }
}

fun mockServiceImpl(hMetaMockCluster: List<HMetaMock>): HStreamApiGrpc.HStreamApiImplBase {
    return HServerMock(hMetaMockCluster)
}

fun startMockedHServer(
    grpcCleanupRule: GrpcCleanupRule,
    serverImpl: HStreamApiGrpc.HStreamApiImplBase,
    hMetaMockCluster: List<HMetaMock>,
    serverUrl: String
) {
    for (hMeta in hMetaMockCluster) {
        runBlocking { hMeta.registerName(serverUrl) }
    }
    val serverName = trimServerUrlToServerName(serverUrl)
    val name = "${urlSchemaToString(serverName.first)}://${serverName.second}"
    grpcCleanupRule.register(
        InProcessServerBuilder
            .forName(name)
            .directExecutor()
            .addService(serverImpl).build().start()
    )
    logger.info("startMockedHServer: serverName = $name")
}

fun getMockedHServerChannel(grpcCleanupRule: GrpcCleanupRule, serverName: String): ManagedChannel {
    logger.info("getMockedHServerChannel: serverName = $serverName")
    return grpcCleanupRule.register(
        InProcessChannelBuilder
            .forName(serverName)
            .directExecutor().build()
    )
}

fun mockChannelProvider(grpcCleanupRule: GrpcCleanupRule): MockedChannelProvider {
    return MockedChannelProvider(grpcCleanupRule)
}

fun trimServerUrlToServerName(serverUrl: String): Pair<UrlSchema, String> {
    val urls = parseServerUrls(serverUrl)
    if (urls != null) {
        return Pair(urls.left, urls.right[0] as String)
    } else {
        TODO()
    }
}

class MockedChannelProvider(private val grpcCleanupRule: GrpcCleanupRule) : ChannelProviderImpl() {
    override fun get(serverUrl: String): ManagedChannel {
        try {
            val serverUri = URI(serverUrl)
            if (serverUri.host == null && serverUri.port == -1) {
                return getMockedHServerChannel(grpcCleanupRule, "hstream://$serverUrl")
            }
        } catch (_: Throwable) {
            return getMockedHServerChannel(grpcCleanupRule, "hstream://$serverUrl")
        }

        return getMockedHServerChannel(grpcCleanupRule, serverUrl)
    }
}

class HServerMockTests {
    @Test
    fun testChannelProvider() {
        val grpcCleanupRule = GrpcCleanupRule()

        val hostname = "127.0.0." + randPort()
        val port = 6570
        val serverUrl = "hstream://$hostname:$port"
        val hMetaMockCluster = arrayListOf<HMetaMock>(HMetaMock())
        startMockedHServer(grpcCleanupRule, mockServiceImpl(hMetaMockCluster), hMetaMockCluster, serverUrl)

        val channelProvider = mockChannelProvider(grpcCleanupRule)
        channelProvider.get(serverUrl)
    }

    @Test
    fun testDescribeCluster() {
        val grpcCleanupRule = GrpcCleanupRule()

        val hostname = "127.0.0." + randPort()
        val port = 6570
        val serverUrl = "hstream://$hostname:$port"
        val hMetaMockCluster = arrayListOf<HMetaMock>(HMetaMock())
        startMockedHServer(grpcCleanupRule, mockServiceImpl(hMetaMockCluster), hMetaMockCluster, serverUrl)

        val channelProvider = mockChannelProvider(grpcCleanupRule)
        channelProvider.get(serverUrl)

        unaryCallWithCurrentUrls(arrayListOf(serverUrl), channelProvider) {
            val resp = it.describeCluster(Empty.newBuilder().build())
            assert(resp.serverNodesList.size != 0)
            logger.info("testDescribeCluster: ${resp.serverNodesList}")
        }
    }

    @Test
    fun testConn() {
        val grpcCleanupRule = GrpcCleanupRule()

        val hostname = "127.0.0." + randPort()
        val port = randPort()
        val serverUrl = "hstream://$hostname:$port"
        val hMetaMockCluster = arrayListOf<HMetaMock>(HMetaMock())
        startMockedHServer(grpcCleanupRule, mockServiceImpl(hMetaMockCluster), hMetaMockCluster, serverUrl)
        val channelProvider = mockChannelProvider(grpcCleanupRule)

        try {
            val clientBuilder = HStreamClientBuilderImpl()
            clientBuilder.serviceUrl(serverUrl)
            clientBuilder.channelProvider(channelProvider)
            val client = clientBuilder.build() as HStreamClientKtImpl
            client.refreshClusterServerUrls()
            client.listStreams()
        } catch (_: io.grpc.StatusException) {} catch (_: CompletionException) {}
    }

    @Test
    fun testBuildMockedClient() {
        buildMockedClient().listStreams()
    }
}

fun urlSchemaToString(urlSchema: UrlSchema): String {
    return when (urlSchema) {
        UrlSchema.HSTREAM -> "hstream"
        UrlSchema.HSTREAMS -> "hstreams"
    }
}

private fun randPort(): Int {
    return Random.nextInt(256)
}

fun buildMockedClient(): HStreamClient {
    val grpcCleanupRule = GrpcCleanupRule()
    // TODO: AutoCloseable?

    val hostname = "${randPort()}.${randPort()}.${randPort()}.${randPort()}"
    val port = randPort()
    val serverUrl = "hstream://$hostname:$port"
    val hMetaMockCluster = arrayListOf<HMetaMock>(HMetaMock())
    startMockedHServer(grpcCleanupRule, mockServiceImpl(hMetaMockCluster), hMetaMockCluster, serverUrl)
    val channelProvider = mockChannelProvider(grpcCleanupRule)

    val clientBuilder = HStreamClientBuilderImpl()
    clientBuilder.serviceUrl(serverUrl)
    clientBuilder.channelProvider(channelProvider)

    return clientBuilder.build() as HStreamClientKtImpl
}
