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
import io.hstream.internal.AppendRequest
import io.hstream.internal.AppendResponse
import io.hstream.internal.BatchedRecord
import io.hstream.internal.DeleteStreamRequest
import io.hstream.internal.DescribeClusterResponse
import io.hstream.internal.HStreamApiGrpc
import io.hstream.internal.HStreamRecord
import io.hstream.internal.ListStreamsRequest
import io.hstream.internal.ListStreamsResponse
import io.hstream.internal.LookupResourceRequest
import io.hstream.internal.LookupSubscriptionRequest
import io.hstream.internal.LookupSubscriptionResponse
import io.hstream.internal.ResourceType
import io.hstream.internal.ServerNode
import io.hstream.internal.SpecialOffset
import io.hstream.internal.StreamingFetchRequest
import io.hstream.internal.StreamingFetchResponse
import io.hstream.util.GrpcUtils
import io.hstream.util.UrlSchemaUtils.parseServerUrls
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.slf4j.LoggerFactory
import java.net.URI
import java.util.concurrent.CompletionException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

open class HServerMock(
    private val hMetaMockCluster: HMetaMock,
    private val serverName: String,
) : HStreamApiGrpc.HStreamApiImplBase() {
    private val logger = LoggerFactory.getLogger(HServerMock::class.java)

    private val recordsByStreamAndShard: MutableMap<String, MutableMap<Long, MutableList<HStreamRecord>>> = mutableMapOf()
    private val globalLsnCnt: AtomicInteger = AtomicInteger()

    private val serverAppendImpl = ServerAppendImpl(recordsByStreamAndShard, globalLsnCnt)
    private val serverStreamingFetchImpl = ServerStreamingFetchImpl(recordsByStreamAndShard)

    private val streams: MutableList<Stream> = arrayListOf()
    private val streamsMutex: Mutex = Mutex()

    private val subscriptions: MutableList<Subscription> = arrayListOf()
    private val subscriptionsMutex: Mutex = Mutex()

    private val streamReceivedRecord: ConcurrentHashMap<String, MutableList<BatchedRecord>> = ConcurrentHashMap()
    private val streamReceivedRecordConsumeProgress: ConcurrentHashMap<String, AtomicInteger> = ConcurrentHashMap()

    override fun describeCluster(request: Empty?, responseObserver: StreamObserver<DescribeClusterResponse>?) = runBlocking {
        val serverNodes: List<ServerNode> = hMetaMockCluster.getServerNodes()
        val serverNodesStatus = hMetaMockCluster.getServerNodesStatus()
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
                for (s in streams) {
                    if (s.streamName == newStream.streamName) {
                        TODO()
                    }
                }
                hMetaMockCluster.registerStream(newStream.streamName, serverName)
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

    override fun createSubscription(
        request: io.hstream.internal.Subscription?,
        responseObserver: StreamObserver<io.hstream.internal.Subscription>?
    ) {
        if (request!!.offset != SpecialOffset.EARLIEST) {
            responseObserver?.onError(Status.INTERNAL.asException())
        }

        runBlocking {
            streamsMutex.withLock {
                for (stream in streams) {
                    if (stream.streamName == request.streamName) {
                        return@runBlocking
                    }
                }
                TODO()
            }
        }

        val subscription = GrpcUtils.subscriptionFromGrpc(request)
        runBlocking {
            val subscriptionId = request.subscriptionId
            subscriptionsMutex.withLock {
                for (sub in subscriptions) {
                    if (sub.subscriptionId == subscriptionId) {
                        TODO()
                    }
                }
                hMetaMockCluster.registerSubscription(subscriptionId, serverName)
                subscriptions.add(
                    subscription
                )
            }
            responseObserver?.onNext(
                GrpcUtils.subscriptionToGrpc(subscription)
            )
            responseObserver?.onCompleted()
        }
    }

    override fun lookupSubscription(
        request: LookupSubscriptionRequest?,
        responseObserver: StreamObserver<LookupSubscriptionResponse>?
    ) {
        try {
            runBlocking {
                val serverName = hMetaMockCluster.lookupSubscriptionName(request!!.subscriptionId)
                val serverNode = serverNameToServerNode(serverName)
                responseObserver?.onNext(
                    LookupSubscriptionResponse.newBuilder()
                        .setSubscriptionId(request.subscriptionId)
                        .setServerNode(serverNode)
                        .build()
                )
            }
        } catch (e: Throwable) {
            logger.error("lookup subscription failed: $e")
            responseObserver?.onError(
                Status.NOT_FOUND.asException()
            )
        }
    }

    override fun append(request: AppendRequest?, responseObserver: StreamObserver<AppendResponse>?) {
        try {
            checkStreamBelonging(request!!.streamName)
        } catch (e: Throwable) {
            responseObserver?.onError(
                Status.INVALID_ARGUMENT.asException()
            )
        }

        serverAppendImpl.append(request, responseObserver)
    }

    private fun checkStreamBelonging(streamName: String) {
        assert(
            serverName == runBlocking { hMetaMockCluster.lookupStreamName(streamName) }
        )
    }

    private fun checkSubscriptionBelonging(subscriptionId: String) {
        assert(
            serverName == runBlocking { hMetaMockCluster.lookupSubscriptionName(subscriptionId) }
        )
    }

    override fun lookupResource(request: LookupResourceRequest?, responseObserver: StreamObserver<ServerNode>?) {
        val serverName: String = runBlocking {
            when (request!!.resType) {
                ResourceType.ResStream -> hMetaMockCluster.lookupStreamName(request.resId)
                ResourceType.ResSubscription -> hMetaMockCluster.lookupSubscriptionName(request.resId)
                ResourceType.ResShard -> TODO()
                ResourceType.ResShardReader -> TODO()
                ResourceType.ResConnector -> TODO()
                ResourceType.ResQuery -> TODO()
                ResourceType.ResView -> TODO()
                ResourceType.UNRECOGNIZED -> TODO()
                else -> TODO()
            }
        }

        val serverNode: ServerNode = serverNameToServerNode(serverName)

        responseObserver?.onNext(serverNode)
        responseObserver?.onCompleted()
    }

    override fun streamingFetch(responseObserver: StreamObserver<StreamingFetchResponse>?): StreamObserver<StreamingFetchRequest> {
        return object : StreamObserver<StreamingFetchRequest> {
            val isInitReq: AtomicBoolean = AtomicBoolean(true)

            override fun onNext(request: StreamingFetchRequest) {
                if (isInitReq.get()) {
                    try {
                        checkSubscriptionBelonging(request.subscriptionId)
                    } catch (e: Throwable) {
                        responseObserver?.onError(
                            Status.INVALID_ARGUMENT.asException()
                        )
                    }
                    isInitReq.set(false)
                }

                val response = StreamingFetchResponse.newBuilder()
                    .setReceivedRecords(
                        io.hstream.internal.ReceivedRecord.newBuilder()
                            .build()
                    )
                    .build()

                responseObserver?.onNext(response)
            }

            override fun onError(t: Throwable?) {
            }

            override fun onCompleted() {
                responseObserver?.onCompleted()
            }
        }
    }
}

fun mockServiceImpl(hMetaMockCluster: HMetaMock, serverName: String, hServerMockImpl: Class<out HStreamApiGrpc.HStreamApiImplBase>): HStreamApiGrpc.HStreamApiImplBase {
    return hServerMockImpl.getConstructor(HMetaMock::class.java, String::class.java).newInstance(hMetaMockCluster, serverName)
}

fun mockServiceImpl(hMetaMockCluster: HMetaMock, serverName: String): HStreamApiGrpc.HStreamApiImplBase {
    return mockServiceImpl(hMetaMockCluster, serverName, HServerMock::class.java)
}

fun startMockedHServer(
    grpcCleanupRule: GrpcCleanupRule,
    serverImpl: HStreamApiGrpc.HStreamApiImplBase,
    hMetaMockCluster: HMetaMock,
    serverUrl: String
) {
    runBlocking { hMetaMockCluster.registerName(serverUrl) }
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
        val hMetaMockCluster = HMetaMock()
        startMockedHServer(grpcCleanupRule, mockServiceImpl(hMetaMockCluster, serverUrl), hMetaMockCluster, serverUrl)

        val channelProvider = mockChannelProvider(grpcCleanupRule)
        channelProvider.get(serverUrl)
    }

    @Test
    fun testDescribeCluster() {
        val grpcCleanupRule = GrpcCleanupRule()

        val hostname = "127.0.0." + randPort()
        val port = 6570
        val serverUrl = "hstream://$hostname:$port"
        val hMetaMockCluster = HMetaMock()
        startMockedHServer(grpcCleanupRule, mockServiceImpl(hMetaMockCluster, serverUrl), hMetaMockCluster, serverUrl)

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
        val hMetaMockCluster = HMetaMock()
        startMockedHServer(grpcCleanupRule, mockServiceImpl(hMetaMockCluster, serverUrl), hMetaMockCluster, serverUrl)
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

    @Test
    fun createSubscriptionOnNonExistedStream() {
        assertThrows<Throwable> {
            val client = buildMockedClient()
            client.createSubscription(
                Subscription.newBuilder()
                    .subscription("some-id")
                    .offset(Subscription.SubscriptionOffset.EARLIEST)
                    .stream("some-stream")
                    .build()
            )
        }
    }

    @Test
    fun createSubscription() {
        val client = buildMockedClient()
        client.createStream("some-stream")
        client.createSubscription(
            Subscription.newBuilder()
                .subscription("some-id")
                .offset(Subscription.SubscriptionOffset.EARLIEST)
                .stream("some-stream")
                .build()
        )
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

fun buildMockedClient(hServerMock: Class<HStreamApiGrpc.HStreamApiImplBase>): HStreamClient {
    val grpcCleanupRule = GrpcCleanupRule()
    // TODO: AutoCloseable?

    val hostname = "${randPort()}.${randPort()}.${randPort()}.${randPort()}"
    val port = randPort()
    val serverUrl = "hstream://$hostname:$port"
    val hMetaMockCluster = HMetaMock()
    startMockedHServer(grpcCleanupRule, mockServiceImpl(hMetaMockCluster, serverUrl, hServerMock), hMetaMockCluster, serverUrl)
    val channelProvider = mockChannelProvider(grpcCleanupRule)

    val clientBuilder = HStreamClientBuilderImpl()
    clientBuilder.serviceUrl(serverUrl)
    clientBuilder.channelProvider(channelProvider)

    return clientBuilder.build() as HStreamClientKtImpl
}

fun buildMockedClient(): HStreamClient {
    return buildMockedClient(HServerMock::class.java as Class<HStreamApiGrpc.HStreamApiImplBase>)
}

fun serverNameToServerNode(serverName: String): ServerNode {
    val uri = URI(serverName)
    assert(uri.port != -1)
    return ServerNode.newBuilder()
        .setHost(uri.host)
        .setPort(uri.port)
        .build()
}
