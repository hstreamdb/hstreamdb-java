package io.hstream.impl

import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.hstream.HRecord
import io.hstream.HStreamDBClientException
import io.hstream.Producer
import io.hstream.Record
import io.hstream.internal.AppendRequest
import io.hstream.internal.HStreamRecord
import io.hstream.internal.LookupStreamRequest
import io.hstream.util.GrpcUtils
import io.hstream.util.RecordUtils
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import kotlin.collections.HashMap

open class ProducerKtImpl(private val client: HStreamClientKtImpl, private val stream: String) : Producer {
    private val serverUrls: HashMap<String, String> = HashMap()
    private val serverUrlsLock: Mutex = Mutex()

    private suspend fun lookupServerUrl(orderingKey: String, forceUpdate: Boolean = false): String {
        if (forceUpdate) {
            return updateServerUrl(orderingKey)
        }
        val server: String? = serverUrlsLock.withLock {
            return@withLock serverUrls[orderingKey]
        }
        if (server != null) {
            return server
        }
        return updateServerUrl(orderingKey)
    }

    private suspend fun updateServerUrl(orderingKey: String): String {
        val req = LookupStreamRequest.newBuilder()
            .setStreamName(stream)
            .setOrderingKey(orderingKey)
            .build()
        val server = client.unaryCallCoroutine {
            val serverNode = it.lookupStream(req).serverNode
            return@unaryCallCoroutine "${serverNode.host}:${serverNode.port}"
        }
        serverUrlsLock.withLock {
            serverUrls[orderingKey] = server
        }
        logger.debug("updateServerUrl, key:$orderingKey, server:$server")
        return server
    }

    override fun write(rawRecord: ByteArray): CompletableFuture<String> {
        val hStreamRecord = RecordUtils.buildHStreamRecordFromRawRecord(rawRecord)
        return writeInternal(hStreamRecord)
    }

    override fun write(hRecord: HRecord): CompletableFuture<String> {
        val hStreamRecord = RecordUtils.buildHStreamRecordFromHRecord(hRecord)
        return writeInternal(hStreamRecord)
    }

    override fun write(record: Record): CompletableFuture<String> {
        val hStreamRecord = RecordUtils.buildHStreamRecordFromRecord(record)
        return writeInternal(hStreamRecord)
    }

    protected open fun writeInternal(hStreamRecord: HStreamRecord): CompletableFuture<String> {
        val future = CompletableFuture<String>()
        writeRecordScope.launch {
            try {
                val ids = writeHStreamRecords(listOf(hStreamRecord), hStreamRecord.header.key)
                future.complete(ids[0])
            } catch (e: Throwable) {
                future.completeExceptionally(e)
            }
        }
        return future
    }

    private suspend fun appendWithRetry(
        appendRequest: AppendRequest,
        orderingKey: String,
        tryTimes: Int,
        forceUpdate: Boolean = false
    ): List<String> {
        // Note: A failed grpc call can throw both 'StatusException' and 'StatusRuntimeException'.
        //       This function is for handling them.
        suspend fun handleGRPCException(serverUrl: String, e: Throwable): List<String> {
            logger.error("append with serverUrl [{}] error", serverUrl, e)
            val status = Status.fromThrowable(e)
            if (status.code == Status.UNAVAILABLE.code && tryTimes > 1) {
                delay(DefaultSettings.REQUEST_RETRY_INTERVAL_SECONDS * 1000)
                return appendWithRetry(appendRequest, orderingKey, tryTimes - 1, true)
            } else {
                throw HStreamDBClientException(e)
            }
        }

        check(tryTimes > 0)
        val serverUrl = lookupServerUrl(orderingKey, forceUpdate)
        logger.info("try append with serverUrl [{}], current left tryTimes is [{}]", serverUrl, tryTimes)
        try {
            return client.getCoroutineStub(serverUrl)
                .append(appendRequest).recordIdsList.map(GrpcUtils::recordIdFromGrpc)
        } catch (e: StatusException) {
            return handleGRPCException(serverUrl, e)
        } catch (e: StatusRuntimeException) {
            return handleGRPCException(serverUrl, e)
        }
    }

    protected suspend fun writeHStreamRecords(hStreamRecords: List<HStreamRecord>, key: String): List<String> {
        val appendRequest = AppendRequest.newBuilder().setStreamName(stream).addAllRecords(hStreamRecords).build()
        return appendWithRetry(appendRequest, key, DefaultSettings.APPEND_RETRY_MAX_TIMES)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ProducerKtImpl::class.java)
        private val writeRecordScope = CoroutineScope(Dispatchers.Default)
    }
}
