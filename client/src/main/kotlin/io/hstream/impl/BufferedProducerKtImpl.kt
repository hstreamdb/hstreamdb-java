package io.hstream.impl

import io.hstream.BatchSetting
import io.hstream.BufferedProducer
import io.hstream.FlowControlSetting
import io.hstream.HStreamDBClientException
import io.hstream.internal.HStreamRecord
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.LinkedList
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.collections.HashMap
import kotlin.concurrent.withLock

typealias Records = MutableList<HStreamRecord>
typealias Futures = MutableList<CompletableFuture<String>>

class BufferedProducerKtImpl(
    client: HStreamClientKtImpl,
    stream: String,
    private val batchSetting: BatchSetting,
    private val flowControlSetting: FlowControlSetting,
) : ProducerKtImpl(client, stream), BufferedProducer {
    private var lock = ReentrantLock()
    private var orderingBuffer: HashMap<String, Records> = HashMap()
    private var orderingFutures: HashMap<String, Futures> = HashMap()
    private var orderingBytesSize: HashMap<String, Int> = HashMap()
    private var orderingJobs: HashMap<String, Job> = HashMap()
    private var batchScope: CoroutineScope = CoroutineScope(Dispatchers.Default)

    private val flowController: FlowController? = if (flowControlSetting.bytesLimit > 0) FlowController(flowControlSetting.bytesLimit) else null

    @Volatile
    private var closed: Boolean = false

    private val scheduler = Executors.newScheduledThreadPool(1)
    private var timerServices: HashMap<String, ScheduledFuture<*>> = HashMap()

    override fun writeInternal(hStreamRecord: HStreamRecord): CompletableFuture<String> {
        if (closed) {
            throw HStreamDBClientException("BufferedProducer is closed")
        }

        flowController?.acquire(hStreamRecord.payload.size())
        return addToBuffer(hStreamRecord)
    }

    private fun addToBuffer(hStreamRecord: HStreamRecord): CompletableFuture<String> {
        lock.withLock {
            if (closed) {
                throw HStreamDBClientException("BufferedProducer is closed")
            }

            val recordFuture = CompletableFuture<String>()
            val key = hStreamRecord.header.key
            if (!orderingBuffer.containsKey(key)) {
                orderingBuffer[key] = LinkedList()
                orderingFutures[key] = LinkedList()
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
        return batchSetting.recordCountLimit in 1..recordCount || batchSetting.bytesLimit in 1..bytesSize
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
                flowController?.release(recordsBytesSize)
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
            flowController?.releaseAll()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(BufferedProducerKtImpl::class.java)
    }

    class FlowController(private var leftBytes: Int) {

        private val lock: ReentrantLock = ReentrantLock(true)
        private val waitingList: LinkedList<BytesWaiter> = LinkedList()

        fun acquire(bytes: Int) {
            acquireInner(bytes)?.await()
        }

        fun release(bytes: Int) {
            lock.withLock {
                var availableBytes = bytes
                while (!waitingList.isEmpty() && availableBytes > 0) {
                    val bytesWaiter = waitingList.removeFirst()
                    availableBytes = bytesWaiter.fill(availableBytes)
                }

                if (availableBytes > 0) {
                    leftBytes += availableBytes
                }
            }
        }

        fun releaseAll() {
            lock.withLock {
                while (!waitingList.isEmpty()) {
                    val bytesWaiter = waitingList.removeFirst()
                    bytesWaiter.unblock()
                }
            }
        }

        private fun acquireInner(bytes: Int): BytesWaiter? {
            lock.withLock {
                return if (bytes <= leftBytes) {
                    leftBytes -= bytes
                    null
                } else {
                    val waitBytes = bytes - leftBytes
                    val bytesWaiter = BytesWaiter(waitBytes)
                    waitingList.addLast(bytesWaiter)
                    bytesWaiter
                }
            }
        }
    }

    class BytesWaiter(private var neededBytes: Int) {
        private var lock = ReentrantLock(true)
        private var isAvailable = lock.newCondition()

        fun await() {
            lock.withLock {
                while (neededBytes > 0) {
                    isAvailable.await()
                }
            }
        }

        fun fill(bytes: Int): Int {
            lock.withLock {
                if (neededBytes == 0) return bytes
                return if (neededBytes <= bytes) {
                    neededBytes = 0
                    isAvailable.signal()
                    bytes - neededBytes
                } else {
                    neededBytes -= bytes
                    0
                }
            }
        }

        fun unblock() {
            lock.withLock { fill(neededBytes) }
        }
    }
}
