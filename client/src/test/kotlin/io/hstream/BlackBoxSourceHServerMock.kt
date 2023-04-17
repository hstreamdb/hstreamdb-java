package io.hstream

import io.grpc.stub.StreamObserver
import io.hstream.internal.HStreamApiGrpc
import io.hstream.internal.LookupSubscriptionRequest
import io.hstream.internal.LookupSubscriptionResponse
import io.hstream.internal.ReceivedRecord
import io.hstream.internal.RecordId
import io.hstream.internal.ServerNode
import io.hstream.internal.StreamingFetchRequest
import io.hstream.internal.StreamingFetchResponse
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.trySendBlocking
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.mockito.junit.MockitoJUnitRunner
import java.net.URI
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.random.Random

class BlackBoxSourceHServerMock(
    hMetaMockCluster: HMetaMock,
    private val serverName: String
) : HServerMock(
    hMetaMockCluster,
    serverName
) {
    private val consumerNameChannelMap: MutableMap<String, Channel<List<RecordId>>> = mutableMapOf()
    fun getConsumerNameChannelMap(): MutableMap<String, Channel<List<RecordId>>> {
        return this.consumerNameChannelMap
    }

    private val uri = run {
        val uri = URI(serverName)
        assert(uri.port != -1)
        uri
    }

    override fun lookupSubscription(
        request: LookupSubscriptionRequest?,
        responseObserver: StreamObserver<LookupSubscriptionResponse>?
    ) {
        responseObserver?.onNext(
            LookupSubscriptionResponse.newBuilder()
                .setSubscriptionId(request?.subscriptionId)
                .setServerNode(
                    ServerNode.newBuilder()
                        .setHost(uri.host)
                        .setPort(uri.port)
                        .setId(0)
                        .build()
                ).build()
        )
        responseObserver?.onCompleted()
    }

    override fun streamingFetch(responseObserver: StreamObserver<StreamingFetchResponse>?): StreamObserver<StreamingFetchRequest> {
        val channelMapRef = this.consumerNameChannelMap
        val isInitReq = AtomicBoolean(true)

        return object : StreamObserver<StreamingFetchRequest> {

            lateinit var channel: Channel<List<RecordId>>

            override fun onNext(request: StreamingFetchRequest) {
                if (isInitReq.get()) {
                    isInitReq.set(false)
                    channel = channelMapRef.getOrPut(request.consumerName) { Channel<List<RecordId>>(6000) }
                }

                val ackIdsList: List<RecordId> = request.ackIdsList
                println("[DEBUG]: begin `trySendBlocking`")
                assert(channel.trySendBlocking(ackIdsList).isSuccess)
                println("[DEBUG]: end `trySendBlocking`")

                val len = 100
                val response = StreamingFetchResponse.newBuilder()
                    .setReceivedRecords(
                        ReceivedRecord.newBuilder()
                            .addAllRecordIds(
                                (1..len).map {
                                    RecordId.newBuilder()
                                        .setShardId(Random.nextLong())
                                        .setBatchId(Random.nextLong())
                                        .setBatchIndex(it).build()
                                }
                            )
                            .setRecord(buildRandomBatchedRecord(len))
                            .build()
                    )
                    .build()
                responseObserver?.onNext(response)
            }

            override fun onError(t: Throwable) {
                responseObserver?.onError(t)
            }

            override fun onCompleted() {
                responseObserver?.onCompleted()
            }
        }
    }
}

fun buildBlackBoxSourceClient_(): Pair<HStreamClient, MutableMap<String, Channel<List<RecordId>>>> {
    val xs = buildMockedClient_(
        BlackBoxSourceHServerMock::class.java as Class<HStreamApiGrpc.HStreamApiImplBase>
    )
    val serverImpl: BlackBoxSourceHServerMock = (xs.second) as BlackBoxSourceHServerMock
    val channel = serverImpl.getConsumerNameChannelMap()
    return Pair(xs.first, channel)
}

fun buildBlackBoxSourceClient(): HStreamClient {
    return buildBlackBoxSourceClient_().first
}

@RunWith(MockitoJUnitRunner::class)
class BlackBoxSourceHServerMockTests {
    @Test
    fun `test buildBlackBoxSourceClient`() {
        val client = buildBlackBoxSourceClient()
        client.createStream("some-s")
        assert(client.listStreams().size == 1)
    }

    @Test
    fun `test BlackBoxSourceHServerMock can fetch many records`() {
        val client = buildBlackBoxSourceClient()
        val records = CopyOnWriteArrayList<ReceivedRawRecord>()
        val countDownLatch = CountDownLatch(50)
        val consumer = client.newConsumer()
            .subscription("any-sub")
            .rawRecordReceiver { record, ackSender ->
                records.add(record)
                ackSender.ack()
                countDownLatch.countDown()
            }
            .build()
        consumer.startAsync().awaitRunning()
        countDownLatch.await()
        consumer.stopAsync().awaitTerminated()
    }

    @Test
    fun `test ack should really ack`() {
        val consumerName = "some-consumer"
        val xs = buildBlackBoxSourceClient_()
        val client = xs.first
        val records = CopyOnWriteArrayList<ReceivedRawRecord>()
        val countDownLatch = CountDownLatch(50)
        val consumer = client.newConsumer()
            .subscription("any-sub")
            .name(consumerName)
            .rawRecordReceiver { record, ackSender ->
                records.add(record)
                ackSender.ack()
                countDownLatch.countDown()
            }
            .build()
        consumer.startAsync().awaitRunning()
        countDownLatch.await()
        consumer.stopAsync().awaitTerminated()

        val channel = xs.second[consumerName]
        val channelAcc = mutableListOf<RecordId>()
        var ret: List<RecordId>?
        while (run {
            ret = channel!!.tryReceive().getOrNull()
            ret != null
        }
        ) {
            channelAcc.addAll(ret!!)
        }
        assert(records.size == channelAcc.size)
    }
}
