package io.hstream.impl

import io.hstream.BatchSetting
import io.hstream.BufferedProducer
import io.hstream.FlowControlSetting
import io.hstream.HStreamDBClientException
import io.hstream.RecordId
import io.hstream.internal.HStreamRecord
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.concurrent.withLock

typealias Records = MutableList<HStreamRecord>
typealias Futures = MutableList<CompletableFuture<RecordId>>

class BufferedProducerKtImpl(
    stream: String,
    private val batchSetting: BatchSetting,
    private val flowControlSetting: FlowControlSetting,
) : ProducerKtImpl(stream), BufferedProducer {
    private var lock = ReentrantLock()
    private var orderingBuffer: HashMap<String, Records> = HashMap()
    private var orderingFutures: HashMap<String, Futures> = HashMap()
    private var orderingBytesSize: HashMap<String, Int> = HashMap()
    private var orderingJobs: HashMap<String, Job> = HashMap()
    private var batchScope: CoroutineScope = CoroutineScope(Dispatchers.IO)
    private val totalBytesSize = AtomicInteger(0)

    @Volatile
    private var closed: Boolean = false

    private val scheduler = Executors.newScheduledThreadPool(4)
    private var timerServices: HashMap<String, ScheduledFuture<*>> = HashMap()

    override fun writeInternal(hStreamRecord: HStreamRecord): CompletableFuture<RecordId> {
        return addToBuffer(hStreamRecord)
    }

    private fun addToBuffer(hStreamRecord: HStreamRecord): CompletableFuture<RecordId> {
        lock.withLock {
            if (closed) {
                throw HStreamDBClientException("BufferedProducer is closed")
            }
            val recordFuture = CompletableFuture<RecordId>()
            val bs = totalBytesSize.get()
            if (bs + hStreamRecord.payload.size() > flowControlSetting.bytesLimit) {
                recordFuture.completeExceptionally(
                    HStreamDBClientException("total bytes size has reached FlowControlSetting.bytesLimit")
                )
                return recordFuture
            }
            while (!totalBytesSize.compareAndSet(bs, bs + hStreamRecord.payload.size())) {}
            val key = hStreamRecord.header.key
            if (!orderingBuffer.containsKey(key)) {
                orderingBuffer[key] = ArrayList(batchSetting.recordCountLimit)
                orderingFutures[key] = ArrayList(batchSetting.recordCountLimit)
                orderingBytesSize[key] = 0
                if (batchSetting.ageLimit > 0) {
                    timerServices[key] =
                        scheduler.schedule({ flushForKey(key) }, batchSetting.ageLimit, TimeUnit.MILLISECONDS)
                }
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
        return (recordCount == batchSetting.recordCountLimit) || batchSetting.bytesLimit > 0 && bytesSize >= batchSetting.bytesLimit
    }

    override fun flush() {
        lock.withLock {
            for (key in orderingBuffer.keys.toList()) {
                flushForKey(key)
            }
        }
    }

    private fun flushForKey(key: String) {
        lock.withLock {
            val records = orderingBuffer[key]!!
            val futures = orderingFutures[key]!!
            val recordsBytesSize = orderingBytesSize[key]!!
            logger.info("ready to flush recordBuffer for key:$key, current buffer size is [{}]", records.size)
            orderingBuffer.remove(key)
            orderingFutures.remove(key)
            orderingBytesSize.remove(key)
            timerServices[key]?.cancel(true)
            timerServices.remove(key)
            val job = orderingJobs[key]
            orderingJobs[key] = batchScope.launch {
                job?.join()
                writeSingleKeyHStreamRecords(records, futures)
                logger.info("wrote batch for key:$key")
                val bs = totalBytesSize.get()
                while (!totalBytesSize.compareAndSet(bs, bs - recordsBytesSize)) {}
            }
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
            closed = true
            flush()
            scheduler.shutdown()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(BufferedProducerKtImpl::class.java)
    }
}
