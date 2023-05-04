package io.hstream

import io.grpc.stub.StreamObserver
import io.hstream.internal.AppendRequest
import io.hstream.internal.AppendResponse
import io.hstream.internal.BatchedRecord
import io.hstream.internal.HStreamApiGrpc
import io.hstream.internal.LookupShardRequest
import io.hstream.internal.LookupShardResponse
import io.hstream.internal.RecordId
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.trySendBlocking
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

class BlackBoxSinkHServerMock(
    hMetaMockCluster: HMetaMock,
    private val serverName: String,
) : HServerMock(
    hMetaMockCluster,
    serverName
) {
    val delayTimeMs: AtomicLong = AtomicLong(0)
    val streamNameFlushChannelMap: MutableMap<String, Channel<Pair<List<RecordId>, BatchedRecord>>> = mutableMapOf()

    override fun append(request: AppendRequest?, responseObserver: StreamObserver<AppendResponse>?) {
        val sleepTimeMs = delayTimeMs.get()
        if (sleepTimeMs != 0L) {
            io.hstream.impl.logger.info("append: sleep for $sleepTimeMs ms")
            Thread.sleep(
                sleepTimeMs
            )
        }

        val streamName = request!!.streamName
        val channel = this.streamNameFlushChannelMap.getOrPut(streamName) {
            Channel(1000)
        }

        val recordIds = (1..request.records.batchSize).map {
            RecordId.newBuilder()
                .setShardId(Random.nextLong())
                .setBatchId(Random.nextLong())
                .setBatchIndex(it).build()
        }

        val sendResult = channel.trySendBlocking(Pair(recordIds, request.records))
        if (sendResult.isClosed || sendResult.isFailure) {
            io.hstream.impl.logger.error(sendResult.toString())
            responseObserver!!.onError(
                io.grpc.Status.INTERNAL.asException()
            )
        }

        val shardId = request.shardId
        responseObserver!!.onNext(
            AppendResponse.newBuilder()
                .setStreamName(streamName)
                .setShardId(shardId)
                .addAllRecordIds(recordIds)
                .build()
        )
        responseObserver.onCompleted()
    }

    override fun lookupShard(request: LookupShardRequest?, responseObserver: StreamObserver<LookupShardResponse>?) {
        val shardId = request!!.shardId

        responseObserver!!.onNext(
            LookupShardResponse.newBuilder()
                .setShardId(shardId)
                .setServerNode(serverNameToServerNode(serverName))
                .build()
        )

        responseObserver.onCompleted()
    }
}

class BlackBoxSinkHServerMockController(
    private val streamNameFlushChannelMap: MutableMap<String, Channel<Pair<List<RecordId>, BatchedRecord>>>,
    private val delayTimeMs: AtomicLong,
) {
    fun getStreamNameFlushChannel(streamName: String): Channel<Pair<List<RecordId>, BatchedRecord>> {
        return this.streamNameFlushChannelMap.getOrPut(streamName) {
            Channel(1000)
        }
    }

    fun setDelayTimeMs(newValue: Long) {
        this.delayTimeMs.set(newValue)
    }
}

fun buildBlackBoxSinkClient_(): Pair<HStreamClient, Pair<BlackBoxSinkHServerMockController, MockedChannelProvider>> {
    val xs = buildMockedClient_(
        BlackBoxSinkHServerMock::class.java as Class<HStreamApiGrpc.HStreamApiImplBase>
    )
    val client = xs.first
    val server = xs.second.first as BlackBoxSinkHServerMock
    val controller = BlackBoxSinkHServerMockController(
        server.streamNameFlushChannelMap,
        server.delayTimeMs
    )
    return Pair(client, Pair(controller, xs.second.second))
}

fun buildBlackBoxSinkClient(): HStreamClient {
    return buildBlackBoxSinkClient_().first
}
