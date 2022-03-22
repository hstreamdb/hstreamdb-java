package io.hstream.impl

import io.hstream.Responder
import io.hstream.internal.RecordId
import io.hstream.internal.StreamingFetchRequest
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.collections.ArrayList

class AckSender(
    private val subscriptionId: String,
    private val ackFlow: MutableSharedFlow<StreamingFetchRequest>,
    private val consumerName: String,
    private val bufferSize: Int
) {
    private val lock = Mutex()
    private val buffer: MutableList<RecordId> = ArrayList(100)
    suspend fun ack(recordId: RecordId) {
        lock.withLock {
            buffer.add(recordId)
            if (buffer.size >= bufferSize) {
                val request = StreamingFetchRequest.newBuilder()
                    .setSubscriptionId(subscriptionId)
                    .setConsumerName(consumerName)
                    .addAllAckIds(ArrayList(buffer))
                    .build()
                ackFlow.emit(request)
                buffer.clear()
            }
        }
    }
}

class ResponderImpl(
    private val ackSender: AckSender,
    private val recordId: RecordId
) : Responder {
    override fun ack() {
        runBlocking {
            ackSender.ack(recordId)
        }
    }
}
