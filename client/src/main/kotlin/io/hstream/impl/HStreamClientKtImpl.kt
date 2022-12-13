package io.hstream.impl

import com.google.protobuf.Empty
import io.grpc.ChannelCredentials
import io.hstream.BufferedProducerBuilder
import io.hstream.Cluster
import io.hstream.ConsumerBuilder
import io.hstream.ConsumerInformation
import io.hstream.HStreamClient
import io.hstream.ProducerBuilder
import io.hstream.Query
import io.hstream.QueryerBuilder
import io.hstream.ReaderBuilder
import io.hstream.Shard
import io.hstream.Stream
import io.hstream.Subscription
import io.hstream.View
import io.hstream.internal.CommandQuery
import io.hstream.internal.CreateQueryRequest
import io.hstream.internal.DeleteQueryRequest
import io.hstream.internal.DeleteStreamRequest
import io.hstream.internal.DeleteSubscriptionRequest
import io.hstream.internal.DeleteViewRequest
import io.hstream.internal.GetQueryRequest
import io.hstream.internal.GetViewRequest
import io.hstream.internal.HStreamApiGrpcKt
import io.hstream.internal.ListConsumersRequest
import io.hstream.internal.ListQueriesRequest
import io.hstream.internal.ListShardsRequest
import io.hstream.internal.ListStreamsRequest
import io.hstream.internal.ListSubscriptionsRequest
import io.hstream.internal.ListViewsRequest
import io.hstream.internal.LookupSubscriptionRequest
import io.hstream.util.GrpcUtils
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.streams.toList

class HStreamClientKtImpl(bootstrapServerUrls: List<String>, credentials: ChannelCredentials? = null) : HStreamClient {

    private val logger = LoggerFactory.getLogger(HStreamClientKtImpl::class.java)

    val channelProvider = ChannelProvider(credentials)
    val clusterServerUrls: AtomicReference<List<String>> = AtomicReference(null)

    fun <Resp> unaryCallAsync(call: suspend (stub: HStreamApiGrpcKt.HStreamApiCoroutineStub) -> Resp): CompletableFuture<Resp> {
        return unaryCallAsync(clusterServerUrls, channelProvider, call)
    }

    // warning: this method will block current thread. Do not call this in suspend functions, use unaryCallCoroutine instead!
    fun <Resp> unaryCallBlocked(call: suspend (stub: HStreamApiGrpcKt.HStreamApiCoroutineStub) -> Resp): Resp {
        return unaryCallBlocked(clusterServerUrls, channelProvider, call)
    }

    suspend fun <Resp> unaryCallCoroutine(call: suspend (stub: HStreamApiGrpcKt.HStreamApiCoroutineStub) -> Resp): Resp {
        return unaryCallCoroutine(clusterServerUrls, channelProvider, call)
    }

    fun getCoroutineStub(url: String): HStreamApiGrpcKt.HStreamApiCoroutineStub {
        return HStreamApiGrpcKt.HStreamApiCoroutineStub(channelProvider.get(url))
    }

    fun getCoroutineStubWithTimeout(url: String, timeoutSeconds: Long): HStreamApiGrpcKt.HStreamApiCoroutineStub {
        return HStreamApiGrpcKt.HStreamApiCoroutineStub(channelProvider.get(url)).withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
    }

    init {
        logger.info("client init with bootstrapServerUrls [{}]", bootstrapServerUrls)
        val describeClusterResponse = unaryCallWithCurrentUrls(
            bootstrapServerUrls,
            channelProvider
        ) { stub -> stub.describeCluster(Empty.newBuilder().build()) }
        val serverNodes = describeClusterResponse.serverNodesList
        val serverUrls: ArrayList<String> = ArrayList(serverNodes.size)
        clusterServerUrls.set(serverUrls)
        for (serverNode in serverNodes) {
            val host = serverNode.host
            val port = serverNode.port
            serverUrls.add("$host:$port")
        }
        logger.info("update clusterServerUrls to [{}]", clusterServerUrls.get())
    }

    override fun close() {
        channelProvider.close()
    }

    override fun newProducer(): ProducerBuilder {
        return ProducerBuilderImpl(this)
    }

    override fun newBufferedProducer(): BufferedProducerBuilder {
        return BufferedProducerBuilderImpl(this)
    }

    override fun newConsumer(): ConsumerBuilder {
        return ConsumerBuilderImpl(this)
    }

    override fun newReader(): ReaderBuilder {
        return ReaderBuilderImpl(this)
    }

    override fun newQueryer(): QueryerBuilder {
        return QueryerBuilderImpl(this, clusterServerUrls.get(), channelProvider)
    }

    override fun createStream(stream: String?) {
        createStream(stream, 1)
    }

    override fun createStream(stream: String?, replicationFactor: Short) {
        createStream(stream, replicationFactor, 1, 3600 * 24)
    }

    override fun createStream(stream: String?, replicationFactor: Short, shardCnt: Int) {
        createStream(stream, replicationFactor, shardCnt, 3600 * 24)
    }

    override fun createStream(stream: String?, replicationFactor: Short, shardCnt: Int, backlogDuration: Int) {
        checkNotNull(stream)
        check(replicationFactor in 1..15)
        check(shardCnt >= 1)

        unaryCallBlocked {
            it.createStream(
                GrpcUtils.streamToGrpc(
                    Stream.newBuilder()
                        .streamName(stream)
                        .replicationFactor(replicationFactor.toInt())
                        .shardCount(shardCnt)
                        .backlogDuration(backlogDuration)
                        .build()
                )
            )
        }
    }

    override fun createStream(stream: Stream?) {
        checkNotNull(stream)
        unaryCallBlocked {
            it.createStream(
                GrpcUtils.streamToGrpc(stream)
            )
        }
    }

    override fun listShards(streamName: String?): List<Shard> {
        checkNotNull(streamName)
        val listShardsRequest = ListShardsRequest.newBuilder().setStreamName(streamName).build()
        val listShardsResponse = unaryCallBlocked { it.listShards(listShardsRequest) }
        return listShardsResponse.shardsList.map {
            Shard(
                it.streamName,
                it.shardId,
                it.startHashRangeKey,
                it.endHashRangeKey
            )
        }
    }

    override fun deleteStream(stream: String?) {
        deleteStream(stream, false)
    }

    override fun deleteStream(stream: String?, force: Boolean) {
        val deleteStreamRequest = DeleteStreamRequest.newBuilder().setStreamName(stream).setForce(force).build()
        unaryCallBlocked { it.deleteStream(deleteStreamRequest) }
    }

    override fun listStreams(): List<Stream> {
        val listStreamsResponse = unaryCallBlocked { it.listStreams(ListStreamsRequest.newBuilder().build()) }
        return listStreamsResponse.streamsList.map(GrpcUtils::streamFromGrpc)
    }

    override fun createSubscription(subscription: Subscription?) {
        unaryCallBlocked { it.createSubscription(GrpcUtils.subscriptionToGrpc(subscription)) }
    }

    override fun listSubscriptions(): List<Subscription> {
        return unaryCallBlocked {
            it.listSubscriptions(ListSubscriptionsRequest.newBuilder().build()).subscriptionList.map(
                GrpcUtils::subscriptionFromGrpc
            )
        }
    }
    override fun deleteSubscription(subscriptionId: String?) {
        deleteSubscription(subscriptionId, false)
    }

    override fun deleteSubscription(subscriptionId: String?, force: Boolean) {
        return runBlocking {
            val serverUrl = lookupSubscriptionServerUrl(subscriptionId)
            HStreamApiGrpcKt.HStreamApiCoroutineStub(
                channelProvider.get(
                    serverUrl
                )
            ).deleteSubscription(
                DeleteSubscriptionRequest.newBuilder().setSubscriptionId(subscriptionId).setForce(force).build()
            )
        }
    }

    override fun describeCluster(): Cluster {
        return unaryCallBlocked {
            val result = it.describeCluster(Empty.getDefaultInstance())
            return@unaryCallBlocked Cluster.newBuilder().uptime(result.clusterUpTime).build()
        }
    }

    override fun createQuery(sql: String?): Query? {
        checkNotNull(sql)
        return unaryCallBlocked {
            val query = it.createQuery(CreateQueryRequest.newBuilder().setSql(sql).build())
            GrpcUtils.queryFromInternal(query)
        }
    }

    override fun listQueries(): List<Query> {
        return unaryCallBlocked {
            val result = it.listQueries(ListQueriesRequest.getDefaultInstance())
            result.queriesList.stream()
                .map(GrpcUtils::queryFromInternal)
                .toList()
        }
    }

    override fun getQuery(id: String?): Query {
        return unaryCallBlocked {
            val result = it.getQuery(GetQueryRequest.newBuilder().setId(id).build())
            GrpcUtils.queryFromInternal(result)
        }
    }

    override fun deleteQuery(id: String?) {
        unaryCallBlocked {
            it.deleteQuery(DeleteQueryRequest.newBuilder().setId(id).build())
        }
    }

    override fun createView(sql: String?) {
        unaryCallBlocked {
            it.executeQuery(CommandQuery.newBuilder().setStmtText(sql).build())
        }
    }

    override fun listViews(): List<View> {
        return unaryCallBlocked {
            it.listViews(ListViewsRequest.getDefaultInstance())
                .viewsList.stream()
                .map(GrpcUtils::viewFromInternal)
                .toList()
        }
    }

    override fun getView(name: String?): View {
        return unaryCallBlocked {
            val result = it.getView(GetViewRequest.newBuilder().setViewId(name).build())
            GrpcUtils.viewFromInternal(result)
        }
    }

    override fun deleteView(name: String?) {
        unaryCallBlocked {
            it.deleteView(DeleteViewRequest.newBuilder().setViewId(name).build())
        }
    }

    override fun listConsumers(subscriptionId: String?): List<ConsumerInformation> {
        return runBlocking {
            val serverUrl = lookupSubscriptionServerUrl(subscriptionId)
            val stub = HStreamApiGrpcKt.HStreamApiCoroutineStub(channelProvider.get(serverUrl))
            stub.listConsumers(ListConsumersRequest.newBuilder().setSubscriptionId(subscriptionId).build())
                .consumersList.stream().map(GrpcUtils::consumerInformationFromGrpc).toList()
        }
    }

    private final suspend fun lookupSubscriptionServerUrl(subscriptionId: String?): String {
        return unaryCallCoroutine {
            val req: LookupSubscriptionRequest =
                LookupSubscriptionRequest
                    .newBuilder()
                    .setSubscriptionId(subscriptionId)
                    .build()
            val serverNode = it.lookupSubscription(req).serverNode
            return@unaryCallCoroutine "${serverNode.host}:${serverNode.port}"
        }
    }
}
