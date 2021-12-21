package io.hstream.impl

import com.google.protobuf.Empty
import io.hstream.*
import io.hstream.internal.DeleteStreamRequest
import io.hstream.internal.DeleteSubscriptionRequest
import io.hstream.internal.HStreamApiGrpcKt
import io.hstream.util.GrpcUtils
import java.util.concurrent.atomic.AtomicReference

class HStreamClientKtImpl(bootstrapServerUrls: List<String>) : HStreamClient {

    val channelProvider = ChannelProvider()
    private val clusterServerUrls: AtomicReference<List<String>> = AtomicReference(null)

    init {

        val describeClusterResponse = unaryCallWithCurrentUrls(bootstrapServerUrls, channelProvider) { stub -> stub.describeCluster(Empty.newBuilder().build()) }
        val serverNodes = describeClusterResponse.serverNodesList
        val serverUrls: ArrayList<String> = ArrayList(serverNodes.size)
        clusterServerUrls.compareAndSet(null, serverUrls)
        for (serverNode in serverNodes) {
            val host = serverNode.host
            val port = serverNode.port
            serverUrls.add("$host:$port")
        }

    }


    override fun close() {
        channelProvider.close()
    }

    override fun newProducer(): ProducerBuilder {
        return ProducerBuilderImpl(clusterServerUrls, channelProvider);
    }

    override fun newConsumer(): ConsumerBuilder {
        TODO("Not yet implemented")
    }

    override fun newQueryer(): QueryerBuilder {
        TODO("Not yet implemented")
    }

    override fun createStream(stream: String?) {
        createStream(stream, 1);
    }

    override fun createStream(stream: String?, replicationFactor: Short) {
        checkNotNull(stream)
        check(replicationFactor in 1..15)

        unaryCallBlocking { it.createStream(GrpcUtils.streamToGrpc(Stream(stream, replicationFactor.toInt()))) }
    }

    override fun deleteStream(stream: String?) {

        val deleteStreamRequest = DeleteStreamRequest.newBuilder().setStreamName(stream).build();
        unaryCallBlocking { it.deleteStream(deleteStreamRequest) }
    }

    override fun listStreams(): List<Stream> {
        val listStreamsResponse = unaryCallBlocking { it.listStreams(Empty.getDefaultInstance()) }
        return listStreamsResponse.streamsList.map(GrpcUtils::streamFromGrpc)
    }

    override fun createSubscription(subscription: Subscription?) {
        unaryCallBlocking { it.createSubscription(GrpcUtils.subscriptionToGrpc(subscription)) }
    }

    override fun listSubscriptions(): List<Subscription> {
        return unaryCallBlocking { it.listSubscriptions(Empty.getDefaultInstance()).subscriptionList.map(GrpcUtils::subscriptionFromGrpc) }
    }

    override fun deleteSubscription(subscriptionId: String?) {
        return unaryCallBlocking { it.deleteSubscription(DeleteSubscriptionRequest.newBuilder().setSubscriptionId(subscriptionId).build()) }
    }

    fun <Resp> unaryCallBlocking(call: suspend (stub: HStreamApiGrpcKt.HStreamApiCoroutineStub) -> Resp): Resp {
        return unaryCall(clusterServerUrls, channelProvider, call)
    }
}