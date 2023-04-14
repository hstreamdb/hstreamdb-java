package io.hstream

import io.grpc.stub.StreamObserver
import io.hstream.internal.HStreamRecord
import io.hstream.internal.RecordId
import io.hstream.internal.StreamingFetchRequest
import io.hstream.internal.StreamingFetchResponse

class ServerStreamingFetchImpl(
    private val recordsByStreamAndShard: MutableMap<String, MutableMap<Long, MutableList<HStreamRecord>>>
) {
    private val ackedRecordIdsBySubscription: MutableMap<String, MutableSet<RecordId>> = mutableMapOf()

    fun streamingFetch(responseObserver: StreamObserver<StreamingFetchResponse>): StreamObserver<StreamingFetchRequest> {
        val streamObserver = object : StreamObserver<StreamingFetchRequest> {
            override fun onNext(request: StreamingFetchRequest) {
                val subscriptionId = request.subscriptionId
                val consumerName = request.consumerName
                val ackIds = request.ackIdsList

                val records = TODO()

                val response = StreamingFetchResponse.newBuilder()
//                    .setReceivedRecords(TODO())
//                    .setSubscriptionId(subscriptionId)
//                    .setConsumerName(consumerName)
                    .build()

                responseObserver.onNext(response)

                if (ackIds.isNotEmpty()) {
                    val ackedRecordIds = ackedRecordIdsBySubscription.getOrPut(subscriptionId) { mutableSetOf() }
                    ackIds.forEach {
                        ackedRecordIds.add(it)
                    }
                }
            }

            override fun onError(t: Throwable) {
                responseObserver.onError(t)
            }

            override fun onCompleted() {
                responseObserver.onCompleted()
            }
        }

        return streamObserver
    }
}
