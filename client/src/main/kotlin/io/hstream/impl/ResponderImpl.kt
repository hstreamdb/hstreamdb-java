package io.hstream.impl

import io.hstream.Responder
import io.hstream.internal.RecordId
import io.hstream.internal.StreamingFetchRequest
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import kotlin.collections.ArrayList

class ResponderImpl(
    private val subscriptionId: String,
    private val ackFlow: MutableSharedFlow<StreamingFetchRequest>,
    private val consumerId: String,
    private val recordId: RecordId
) : Responder {
    override fun ack() {
        runBlocking {
            lock.withLock {
                if (!buffer.containsKey(consumerId)) {
                    buffer[consumerId] = ArrayList(bufferSize)
                }
                buffer[consumerId]!!.add(recordId)
                if (buffer[consumerId]!!.size >= bufferSize) {
                    val request = StreamingFetchRequest.newBuilder()
                        .setSubscriptionId(subscriptionId)
                        .setConsumerName(consumerId)
                        .addAckIds(recordId)
                        .build()
                    ackFlow.emit(request)
                    buffer.remove(consumerId)
                }
            }
        }
    }

    companion object {
        private const val bufferSize = 100
        private val lock = Mutex()
        private val buffer: HashMap<String, MutableList<RecordId>> = HashMap()
        private val logger = LoggerFactory.getLogger(ResponderImpl::class.java)
    }
}
