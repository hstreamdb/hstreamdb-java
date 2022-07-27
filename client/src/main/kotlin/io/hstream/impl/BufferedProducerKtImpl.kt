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
    private var shardAppendBuffer: HashMap<Long, Records> = HashMap()
    private var shardAppendFutures: HashMap<Long, Futures> = HashMap()
    private var shardAppendBytesSize: HashMap<Long, Int> = HashMap()
    private var shardAppendJobs: HashMap<Long, Job> = HashMap()
    private var batchScope: CoroutineScope = CoroutineScope(Dispatchers.Default)

    private val flowController: FlowController? = if (flowControlSetting.bytesLimit > 0) FlowController(flowControlSetting.bytesLimit) else null

    @Volatile
    private var closed: Boolean = false

    private val scheduler = Executors.newScheduledThreadPool(1)
    private var timerServices: HashMap<Long, ScheduledFuture<*>> = HashMap()

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
            val partitionKey = hStreamRecord.header.key
            val shardId = calculateShardIdByPartitionKey(partitionKey)
            if (!shardAppendBuffer.containsKey(shardId)) {
                shardAppendBuffer[shardId] = LinkedList()
                shardAppendFutures[shardId] = LinkedList()
                shardAppendBytesSize[shardId] = 0
                if (batchSetting.ageLimit > 0) {
                    timerServices[shardId] =
                        scheduler.schedule({ flushForShard(shardId) }, batchSetting.ageLimit, TimeUnit.MILLISECONDS)
                }
            }
            shardAppendBuffer[shardId]!!.add(hStreamRecord)
            shardAppendFutures[shardId]!!.add(recordFuture)
            shardAppendBytesSize[shardId] = shardAppendBytesSize[shardId]!! + hStreamRecord.payload.size()
            if (isFull(shardId)) {
                flushForShard(shardId)
            }
            return recordFuture
        }
    }

    private fun isFull(shardId: Long): Boolean {
        val recordCount = shardAppendBuffer[shardId]!!.size
        val bytesSize = shardAppendBytesSize[shardId]!!
        return batchSetting.recordCountLimit in 1..recordCount || batchSetting.bytesLimit in 1..bytesSize
    }

    override fun flush() {
        lock.withLock {
            for (shard in shardAppendBuffer.keys.toList()) {
                flushForShard(shard)
            }
        }
    }

    private fun flushForShard(shardId: Long) {
        lock.withLock {
            val records = shardAppendBuffer[shardId]!!
            val futures = shardAppendFutures[shardId]!!
            val recordsBytesSize = shardAppendBytesSize[shardId]!!
            logger.info("ready to flush recordBuffer for shard:$shardId, current buffer size is [{}]", records.size)
            shardAppendBuffer.remove(shardId)
            shardAppendFutures.remove(shardId)
            shardAppendBytesSize.remove(shardId)
            timerServices[shardId]?.cancel(true)
            timerServices.remove(shardId)
            val job = shardAppendJobs[shardId]
            shardAppendJobs[shardId] = batchScope.launch {
                job?.join()
                writeShard(shardId, records, futures)
                logger.info("wrote batch for shard:$shardId")
                flowController?.release(recordsBytesSize)
            }
        }
    }

    // only can be called by flush()
    private suspend fun writeShard(shardId: Long, records: Records, futures: Futures) {
        try {
            val ids = super.writeHStreamRecords(records, shardId)
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
                    availableBytes = waitingList.first.fill(availableBytes)
                    if (availableBytes >= 0) {
                        waitingList.removeFirst()
                    }
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
                    -1
                }
            }
        }

        fun unblock() {
            lock.withLock { fill(neededBytes) }
        }
    }
}
