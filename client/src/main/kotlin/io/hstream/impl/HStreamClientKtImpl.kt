package io.hstream.impl

import com.google.protobuf.Empty
import io.hstream.BufferedProducerBuilder
import io.hstream.ConsumerBuilder
import io.hstream.HStreamClient
import io.hstream.ProducerBuilder
import io.hstream.QueryerBuilder
import io.hstream.Stream
import io.hstream.Subscription
import io.hstream.internal.DeleteStreamRequest
import io.hstream.internal.DeleteSubscriptionRequest
import io.hstream.internal.HStreamApiGrpcKt
import io.hstream.internal.LookupSubscriptionRequest
import io.hstream.util.GrpcUtils
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicReference

class HStreamClientKtImpl(bootstrapServerUrls: List<String>) : HStreamClient {

    private val logger = LoggerFactory.getLogger(HStreamClientKtImpl::class.java)

    companion object ConnectionManager {
        val channelProvider = ChannelProvider()
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
        return ProducerBuilderImpl()
    }

    override fun newBufferedProducer(): BufferedProducerBuilder {
        return BufferedProducerBuilderImpl()
    }

    override fun newConsumer(): ConsumerBuilder {
        return ConsumerBuilderImpl()
    }

    override fun newQueryer(): QueryerBuilder {
        return QueryerBuilderImpl(this, clusterServerUrls.get(), channelProvider)
    }

    override fun createStream(stream: String?) {
        createStream(stream, 1)
    }

    override fun createStream(stream: String?, replicationFactor: Short) {
        checkNotNull(stream)
        check(replicationFactor in 1..15)

        unaryCallBlocked {
            it.createStream(
                GrpcUtils.streamToGrpc(
                    Stream(
                        stream,
                        replicationFactor.toInt()
                    )
                )
            )
        }
    }

    override fun deleteStream(stream: String?) {

        val deleteStreamRequest = DeleteStreamRequest.newBuilder().setStreamName(stream).build()
        unaryCallBlocked { it.deleteStream(deleteStreamRequest) }
    }

    override fun listStreams(): List<Stream> {
        val listStreamsResponse = unaryCallBlocked { it.listStreams(Empty.getDefaultInstance()) }
        return listStreamsResponse.streamsList.map(GrpcUtils::streamFromGrpc)
    }

    override fun createSubscription(subscription: Subscription?) {
        unaryCallBlocked { it.createSubscription(GrpcUtils.subscriptionToGrpc(subscription)) }
    }

    override fun listSubscriptions(): List<Subscription> {
        return unaryCallBlocked {
            it.listSubscriptions(Empty.getDefaultInstance()).subscriptionList.map(
                GrpcUtils::subscriptionFromGrpc
            )
        }
    }

    override fun deleteSubscription(subscriptionId: String?) {
        return runBlocking {
            val serverUrl = lookupServerUrl(subscriptionId)
            HStreamApiGrpcKt.HStreamApiCoroutineStub(
                channelProvider.get(
                    serverUrl
                )
            ).deleteSubscription(
                DeleteSubscriptionRequest.newBuilder().setSubscriptionId(subscriptionId).build()
            )
        }
    }

    private final suspend fun lookupServerUrl(subscriptionId: String?): String {
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
