package io.hstream.impl

import io.hstream.BufferedProducer
import io.hstream.HStreamDBClientException
import io.hstream.RecordId
import io.hstream.internal.HStreamRecord
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.collections.ArrayList
import kotlin.concurrent.withLock

typealias Records = MutableList<HStreamRecord>
typealias Futures = MutableList<CompletableFuture<RecordId>>

class BufferedProducerKtImpl(
    stream: String,
    private val recordCountLimit: Int,
    private val flushIntervalMs: Long,
    private val maxBytesSize: Int,
    private val throwExceptionIfFull: Boolean,
    private val maxBatchSize: Int
) : ProducerKtImpl(stream), BufferedProducer {
    private var lock = ReentrantLock()
    private var orderingBuffer: HashMap<String, Records> = HashMap()
    private var orderingFutures: HashMap<String, Futures> = HashMap()
    private var orderingBytesSize: HashMap<String, Int> = HashMap()
    private var orderingJobs: HashMap<String, Job> = HashMap()
    private var batchCondition = Channel<Unit>(maxBatchSize)
    private var batchScope: CoroutineScope = CoroutineScope(Dispatchers.IO)

    @Volatile
    private var closed: Boolean = false

    private var timerService: ScheduledFuture<*>? = null

    init {
        if (flushIntervalMs > 0) {
            runTimer()
        }
    }

    private fun runTimer() {
        timerService = scheduler.scheduleAtFixedRate(
            { flush() },
            flushIntervalMs,
            flushIntervalMs,
            TimeUnit.MILLISECONDS
        )
    }

    override fun writeInternal(hStreamRecord: HStreamRecord): CompletableFuture<RecordId> {
        return addToBuffer(hStreamRecord)
    }

    private fun addToBuffer(hStreamRecord: HStreamRecord): CompletableFuture<RecordId> {
        // fuzzy check
        val recordFuture = CompletableFuture<RecordId>()
//        if (throwExceptionIfFull && isFull) {
//            recordFuture.completeExceptionally(HStreamDBClientException("buffer is full"))
//            return recordFuture
//        }
        lock.withLock {
            if (closed) {
                throw HStreamDBClientException("BufferedProducer is closed")
            }
            // it is impossible that buffer is full after holding the lock,
            // if buffer is full, there must exist another thread keeping the lock(flushing buffer).
            val key = hStreamRecord.header.key
            if (!orderingBuffer.containsKey(key)) {
                orderingBuffer[key] = ArrayList(recordCountLimit)
                orderingFutures[key] = ArrayList(recordCountLimit)
                orderingBytesSize[key] = 0
            }
            orderingBuffer[key]!!.add(hStreamRecord)
            orderingFutures[key]!!.add(recordFuture)
            orderingBytesSize[key] = orderingBytesSize[key]!! + hStreamRecord.payload.size()
            if (isFull(key)) {
                flushForKey(key)
            }
            return recordFuture
        }
    }

    private fun isFull(key: String): Boolean {
        val recordCount = orderingBuffer[key]!!.size
        val bytesSize = orderingBytesSize[key]!!
        return (recordCount == recordCountLimit) || maxBytesSize > 0 && bytesSize >= maxBytesSize
    }

    override fun flush() {
        lock.withLock {
            for ((k, _) in orderingBuffer) {
                flushForKey(k)
            }
        }
    }

    private fun flushForKey(key: String) {
        lock.withLock {
            if (orderingBuffer[key]!!.isEmpty()) {
                return
            }
//            val recordBufferCount = recordBuffer.size
//            logger.info("ready to flush recordBuffer, current buffer size is [{}]", recordBufferCount)
            runBlocking(Dispatchers.IO) { batchCondition.send(Unit) }
            val job = orderingJobs[key]
            val records = orderingBuffer[key]!!
            val futures = orderingFutures[key]!!
            orderingJobs[key] = batchScope.launch {
                job?.join()
                writeSingleKeyHStreamRecords(records, futures)
                batchCondition.receive()
            }
            orderingBuffer.remove(key)
            orderingFutures.remove(key)
            orderingBytesSize.remove(key)
        }
    }

    // only can be called by flush()
    private suspend fun writeSingleKeyHStreamRecords(records: Records, futures: Futures) {
        try {
            val ids = super.writeHStreamRecords(records, records[0].header.key)
            for (i in ids.indices) {
                futures[i].complete(ids[i])
            }
        } catch (e: Throwable) {
            futures.forEach { it.completeExceptionally(e) }
        }
    }

    override fun close() {
        if (!closed) {
            timerService?.cancel(false)
            batchScope.cancel()
            closed = true
            flush()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(BufferedProducerKtImpl::class.java)
        private val scheduler = Executors.newScheduledThreadPool(4)
    }
}
