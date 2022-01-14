package io.hstream.impl

import io.hstream.BufferedProducer
import io.hstream.HStreamDBClientException
import io.hstream.RecordId
import io.hstream.internal.HStreamRecord
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Semaphore

class BufferedProducerKtImpl(
    stream: String,
    private val recordCountLimit: Int,
    private val flushIntervalMs: Long,
    private val maxBytesSize: Int,
) : ProducerKtImpl(stream), Closeable, BufferedProducer {
    private var semaphore: Semaphore = Semaphore(recordCountLimit)
    private var lock: Mutex = Mutex()
    private var recordBuffer: MutableList<HStreamRecord> = ArrayList(recordCountLimit)
    private var futures: MutableList<CompletableFuture<RecordId>> = ArrayList(recordCountLimit)

    @Volatile
    private var closed: Boolean = false
    private var bytesSize: Int = 0

    init {
        if (flushIntervalMs > 0) {
            runTimer()
        }
    }

    override fun writeInternal(hStreamRecord: HStreamRecord): CompletableFuture<RecordId> {
        if (closed) {
            throw HStreamDBClientException("BufferedProducer is closed")
        }
        return addToBuffer(hStreamRecord)
    }

    override fun flush() {
        futureForIO { lock.lock() }.join()
        try {
            if (recordBuffer.isEmpty()) {
                return
            } else {
                val recordBufferCount = recordBuffer.size
                logger.info("ready to flush recordBuffer, current buffer size is [{}]", recordBufferCount)
                super.writeHStreamRecords(recordBuffer)
                    // WARNING: Do not explicitly mark the type of 'recordIds'!
                    //          The first argument of handle is of type 'List<RecordId>!'.
                    //          If it is explicitly marked as 'List<RecordId>', a producer
                    //          will throw an exception but can not be handled because of
                    //          inconsistent type when it exhausts its retry times. This
                    //          causes the whole program to be stuck forever.
                    .handle<Any?> { recordIds, exception: Throwable? ->
                        if (exception == null) {
                            for (i in recordIds.indices) {
                                futures[i].complete(recordIds[i])
                            }
                        } else {
                            for (i in futures.indices) {
                                futures[i].completeExceptionally(exception)
                            }
                        }
                        null
                    }
                    .join()
                recordBuffer.clear()
                futures.clear()
                bytesSize = 0
                logger.info("flush the record buffer successfully")
                semaphore.release(recordBufferCount)
            }
        } finally {
            lock.unlock()
        }
    }

    private fun addToBuffer(hStreamRecord: HStreamRecord): CompletableFuture<RecordId> {
        try {
            semaphore.acquire()
        } catch (e: InterruptedException) {
            throw HStreamDBClientException(e)
        }
        futureForIO { lock.lock() }.join()

        var needFlush = false
        val completableFuture = CompletableFuture<RecordId>()
        try {
            recordBuffer.add(hStreamRecord)
            futures.add(completableFuture)
            bytesSize += hStreamRecord.payload.size()
            if (recordBuffer.size == recordCountLimit) {
                needFlush = true
            } else if (maxBytesSize >= 0 && bytesSize > maxBytesSize) {
                needFlush = true
            }
        } finally {
            lock.unlock()
        }
        if (needFlush) {
            flush()
        }
        return completableFuture
    }

    private fun runTimer() {
        timerScope.launch {
            while (!closed) {
                delay(flushIntervalMs)
                flush()
            }
            flush()
        }
    }

    override fun close() {
        closed = true
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ProducerKtImpl::class.java)
        private val timerScope = CoroutineScope(Dispatchers.Default)
    }
}
