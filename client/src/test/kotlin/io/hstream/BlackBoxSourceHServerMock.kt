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
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.mockito.junit.MockitoJUnitRunner
import java.net.URI
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import kotlin.random.Random

class BlackBoxSourceHServerMock(
    hMetaMockCluster: HMetaMock,
    private val serverName: String
) : HServerMock(
    hMetaMockCluster,
    serverName
) {
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
        return object : StreamObserver<StreamingFetchRequest> {
            override fun onNext(request: StreamingFetchRequest) {
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

fun buildBlackBoxSourceClient(): HStreamClient {
    return buildMockedClient(
        BlackBoxSourceHServerMock::class.java as Class<HStreamApiGrpc.HStreamApiImplBase>
    )
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
        val records = CopyOnWriteArrayList<io.hstream.ReceivedRawRecord>()
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
}
