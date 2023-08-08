package io.hstream.impl

import io.hstream.HStreamDBClientException
import io.hstream.Responder
import io.hstream.internal.RecordId
import io.hstream.internal.StreamingFetchRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.future.future
import java.io.Closeable
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.collections.ArrayList
import kotlin.concurrent.withLock

class AckSender(
    private val subscriptionId: String,
    private val ackFlow: MutableSharedFlow<StreamingFetchRequest>,
    private val consumerName: String,
    private val bufferSize: Int,
    private val ackAgeLimit: Long,
) : Closeable {
    private val lock = ReentrantLock()
    private val buffer: MutableList<RecordId> = ArrayList(bufferSize)
    private val emitScope = CoroutineScope(Dispatchers.IO)
    private var scheduler: ScheduledExecutorService? = null

    @Volatile
    private var closed: Boolean = false
    private var pendingFlushFuture: ScheduledFuture<*>? = null

    init {
        if (ackAgeLimit > 0 && bufferSize > 1) {
            scheduler = Executors.newSingleThreadScheduledExecutor()
        }
    }

    fun ack(recordId: RecordId) {
        lock.withLock {
            if (closed) {
                throw HStreamDBClientException("ackSender is Closed")
            }
            if (ackAgeLimit > 0 && buffer.isEmpty() && bufferSize > 1) {
                pendingFlushFuture = scheduler!!.schedule({
                    flush()
                }, ackAgeLimit, TimeUnit.MILLISECONDS)
            }
            buffer.add(recordId)
            if (buffer.size >= bufferSize) {
                flush()
                pendingFlushFuture?.cancel(true)
            }
        }
    }

    // timeoutMs > 0 ==> sync mode with timeout
    // timeoutMs <= 0 ==> async mode
    fun flush(timeoutMs: Long = 0) {
        lock.withLock {
            if (buffer.isEmpty()) {
                return
            }
            val request = StreamingFetchRequest.newBuilder()
                .setSubscriptionId(subscriptionId)
                .setConsumerName(consumerName)
                .addAllAckIds(ArrayList(buffer))
                .build()
            val future = emitScope.future {
                ackFlow.emit(request)
            }
            if (timeoutMs > 0) {
                try {
                    future.get(timeoutMs, TimeUnit.MILLISECONDS)
                } catch (e: Throwable) {
                    logger.error("ack failed, ${e.message}")
                }
            }
            buffer.clear()
        }
    }

    override fun close() {
        closed = true
        flush(300)
        scheduler?.shutdown()
        emitScope.cancel()
    }
}

class ResponderImpl(
    private val ackSender: AckSender,
    private val recordId: RecordId,
) : Responder {
    override fun ack() {
        ackSender.ack(recordId)
    }
}
