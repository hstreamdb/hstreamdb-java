package io.hstream.impl

import io.hstream.Responder
import io.hstream.internal.RecordId
import io.hstream.internal.StreamingFetchRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.launch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.collections.ArrayList
import kotlin.concurrent.withLock

class AckSender(
    private val subscriptionId: String,
    private val ackFlow: MutableSharedFlow<StreamingFetchRequest>,
    private val consumerName: String,
    private val bufferSize: Int,
    private val ackAgeLimit: Long
) {
    private val lock = ReentrantLock()
    private val buffer: MutableList<RecordId> = ArrayList(100)
    private val emitScope = CoroutineScope(Dispatchers.IO)
    private val scheduler = Executors.newScheduledThreadPool(1)

    fun ack(recordId: RecordId) {
        lock.withLock {
            if (buffer.isEmpty() && bufferSize > 1) {
                scheduler.schedule({ flush() }, ackAgeLimit, TimeUnit.MILLISECONDS)
            }
            buffer.add(recordId)
            if (buffer.size >= bufferSize) {
                flush()
            }
        }
    }

    fun flush() {
        lock.withLock {
            if (buffer.isEmpty()) {
                return
            }
            val request = StreamingFetchRequest.newBuilder()
                .setSubscriptionId(subscriptionId)
                .setConsumerName(consumerName)
                .addAllAckIds(ArrayList(buffer))
                .build()
            emitScope.launch {
                ackFlow.emit(request)
            }
            buffer.clear()
        }
    }
}

class ResponderImpl(
    private val ackSender: AckSender,
    private val recordId: RecordId
) : Responder {
    override fun ack() {
        ackSender.ack(recordId)
    }
}
