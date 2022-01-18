package io.hstream.impl

import io.hstream.BufferedProducer
import io.hstream.HStreamDBClientException
import io.hstream.RecordId
import io.hstream.internal.HStreamRecord
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.util.LinkedList
import java.util.Queue
import java.util.concurrent.CompletableFuture
import kotlin.collections.ArrayList

class BufferedProducerKtImpl(
    stream: String,
    private val recordCountLimit: Int,
    private val flushIntervalMs: Long,
    private val maxBytesSize: Int,
    private val throwExceptionIfFull: Boolean
) : ProducerKtImpl(stream), BufferedProducer {
    private var lock: Mutex = Mutex()
    private var recordBuffer: MutableList<HStreamRecord> = ArrayList(recordCountLimit)
    private var futures: MutableList<CompletableFuture<RecordId>> = ArrayList(recordCountLimit)
    private var waitQueue: Queue<CompletableFuture<Unit>> = LinkedList()

    @Volatile
    private var closed: Boolean = false
    private var timerJob: Job? = null
    private var bytesSize: Int = 0

    init {
        if (flushIntervalMs > 0) {
            timerJob = runTimer()
        }
    }

    override fun writeInternal(hStreamRecord: HStreamRecord): CompletableFuture<RecordId> {
        if (closed) {
            throw HStreamDBClientException("BufferedProducer is closed")
        }

        runBlocking { lock.lock() }

        if (isFull()) {
            if (throwExceptionIfFull) {
                throw HStreamDBClientException("buffer is full")
            }
            val future = CompletableFuture<Unit>()
            waitQueue.add(future)
            lock.unlock()
            future.join()
            return writeInternal(hStreamRecord)
        }

        val completableFuture = CompletableFuture<RecordId>()
        recordBuffer.add(hStreamRecord)
        futures.add(completableFuture)
        bytesSize += hStreamRecord.payload.size()
        val needFlush = isFull()
        lock.unlock()
        if (needFlush) {
            flushScope.launch {
                flushInternal()
            }
        }
        return completableFuture
    }

    override fun flush() {
        runBlocking { flushInternal() }
    }

    private suspend fun flushInternal() {
        lock.lock()
        try {
            if (recordBuffer.isEmpty()) {
                return
            }
            val recordBufferCount = recordBuffer.size
            logger.info("ready to flush recordBuffer, current buffer size is [{}]", recordBufferCount)
            val ids = super.writeHStreamRecords(recordBuffer)
            for (i in ids.indices) {
                futures[i].complete(ids[i])
            }
            logger.info("flush the record buffer successfully")
        } catch (e: Exception) {
            for (i in futures.indices) {
                futures[i].completeExceptionally(e)
            }
        } finally {
            recordBuffer.clear()
            futures.clear()
            bytesSize = 0
            for (it in waitQueue) {
                it.complete(Unit)
            }
            lock.unlock()
        }
    }

    private fun isFull(): Boolean {
        return (recordBuffer.size == recordCountLimit) || maxBytesSize >= 0 && bytesSize > maxBytesSize
    }

    private fun runTimer(): Job {
        return timerScope.launch {
            while (!closed) {
                delay(flushIntervalMs)
                flushInternal()
            }
        }
    }

    override fun close() {
        if (!closed) {
            flush()
        }
        timerJob?.cancel()
        closed = true
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ProducerKtImpl::class.java)
        private val timerScope = CoroutineScope(Dispatchers.Default)
        private val flushScope = CoroutineScope(Dispatchers.Default)
    }
}
