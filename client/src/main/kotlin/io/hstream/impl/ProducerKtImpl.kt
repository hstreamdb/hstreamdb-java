package io.hstream.impl

import io.hstream.HRecord
import io.hstream.HStreamDBClientException
import io.hstream.Producer
import io.hstream.RecordId
import io.hstream.internal.AppendRequest
import io.hstream.internal.HStreamApiGrpcKt
import io.hstream.internal.HStreamRecord
import io.hstream.internal.LookupStreamRequest
import io.hstream.util.GrpcUtils
import io.hstream.util.RecordUtils
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.future
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

class ProducerKtImpl(
    private val stream: String,
    private val enableBatch: Boolean,
    private val recordCountLimit: Int
) : Producer {
    private var semaphore: Semaphore? = null
    private var lock: Lock? = null
    private var recordBuffer: MutableList<HStreamRecord>? = null
    private var futures: MutableList<CompletableFuture<RecordId>>? = null
    private val serverUrlRef: AtomicReference<String> = AtomicReference(null)

    init {
        if (enableBatch) {
            semaphore = Semaphore(recordCountLimit)
            lock = ReentrantLock()
            recordBuffer = ArrayList(recordCountLimit)
            futures = ArrayList(recordCountLimit)
        }
    }

    private fun lookupServerUrl(): String {
        return HStreamClientKtImpl.unaryCall {
            val serverNode = it.lookupStream(LookupStreamRequest.newBuilder().setStreamName(stream).build()).serverNode
            return@unaryCall "${serverNode.host}:${serverNode.port}"
        }
    }

    private fun refreshServerUrl() {
        serverUrlRef.set(lookupServerUrl())
    }

    override fun write(rawRecord: ByteArray): CompletableFuture<RecordId> {
        val hStreamRecord = RecordUtils.buildHStreamRecordFromRawRecord(rawRecord)
        return writeInternal(hStreamRecord)
    }

    override fun write(hRecord: HRecord): CompletableFuture<RecordId> {
        val hStreamRecord = RecordUtils.buildHStreamRecordFromHRecord(hRecord)
        return writeInternal(hStreamRecord)
    }

    private fun writeInternal(hStreamRecord: HStreamRecord): CompletableFuture<RecordId> {
        return if (!enableBatch) {
            val future = CompletableFuture<RecordId>()
            writeHStreamRecords(listOf(hStreamRecord))
                .handle<Any?> { recordIds: List<RecordId>, exception: Throwable? ->
                    if (exception == null) {
                        future.complete(recordIds[0])
                    } else {
                        future.completeExceptionally(exception)
                    }
                }
            future
        } else {
            addToBuffer(hStreamRecord)
        }
    }

    private fun flush() {
        lock!!.lock()
        try {
            if (recordBuffer!!.isEmpty()) {
                return
            } else {
                val recordBufferCount = recordBuffer!!.size
                logger.info("start flush recordBuffer, current buffer size is: {}", recordBufferCount)
                writeHStreamRecords(recordBuffer)
                    .handle<Any?> { recordIds: List<RecordId>, exception: Throwable? ->
                        if (exception == null) {
                            for (i in recordIds.indices) {
                                futures!![i].complete(recordIds[i])
                            }
                        } else {
                            for (i in futures!!.indices) {
                                futures!![i].completeExceptionally(exception)
                            }
                        }
                        null
                    }
                    .join()
                recordBuffer!!.clear()
                futures!!.clear()
                logger.info("finish clearing record buffer")
                semaphore!!.release(recordBufferCount)
            }
        } finally {
            lock!!.unlock()
        }
    }

    private suspend fun appendWithRetry(appendRequest: AppendRequest, tryTimes: Int): List<RecordId> {
        check(tryTimes > 0)
        var serverUrl = serverUrlRef.get()
        if (serverUrl == null) {
            refreshServerUrl()
            serverUrl = serverUrlRef.get()
        }
        checkNotNull(serverUrl)
        try {
            return HStreamApiGrpcKt.HStreamApiCoroutineStub(HStreamClientKtImpl.channelProvider.get(serverUrl))
                .append(appendRequest).recordIdsList.map(GrpcUtils::recordIdFromGrpc)
        } catch (e: Exception) {
            if (tryTimes == 1) {
                throw e
            } else {
                delay(1000)
                refreshServerUrl()
                return appendWithRetry(appendRequest, tryTimes - 1)
            }
        }
    }

    @OptIn(DelicateCoroutinesApi::class)
    private fun writeHStreamRecords(
        hStreamRecords: List<HStreamRecord>?
    ): CompletableFuture<List<RecordId>> {
        val appendRequest = AppendRequest.newBuilder().setStreamName(stream).addAllRecords(hStreamRecords).build()
        return GlobalScope.future { appendWithRetry(appendRequest, 3) }
    }

    private fun addToBuffer(hStreamRecord: HStreamRecord): CompletableFuture<RecordId> {
        try {
            semaphore!!.acquire()
        } catch (e: InterruptedException) {
            throw HStreamDBClientException(e)
        }
        lock!!.lock()
        return try {
            val completableFuture = CompletableFuture<RecordId>()
            recordBuffer!!.add(hStreamRecord)
            futures!!.add(completableFuture)
            if (recordBuffer!!.size == recordCountLimit) {
                flush()
            }
            completableFuture
        } finally {
            lock!!.unlock()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ProducerKtImpl::class.java)
    }
}
