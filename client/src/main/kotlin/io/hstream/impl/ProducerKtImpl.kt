package io.hstream.impl

import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.hstream.HRecord
import io.hstream.HStreamDBClientException
import io.hstream.Producer
import io.hstream.Record
import io.hstream.RecordId
import io.hstream.internal.AppendRequest
import io.hstream.internal.HStreamApiGrpcKt
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

open class ProducerKtImpl(private val stream: String) : Producer {
    private val serverUrls: HashMap<String, String> = HashMap()
    private val serverUrlsLock: Mutex = Mutex()

    private suspend fun lookupServerUrl(orderingKey: String): String {
        return HStreamClientKtImpl.unaryCallCoroutine {
            var server: String? = serverUrlsLock.withLock {
                return@withLock serverUrls[orderingKey]
            }
            if (server != null) {
                return@unaryCallCoroutine server
            }
            val req = LookupStreamRequest.newBuilder()
                .setStreamName(stream)
                .setOrderingKey(orderingKey)
                .build()
            val serverNode = it.lookupStream(req).serverNode
            server = "${serverNode.host}:${serverNode.port}"
            logger.info("TMP, key:$orderingKey, server:$server")
            serverUrlsLock.withLock {
                serverUrls[orderingKey] = server
            }
            return@unaryCallCoroutine server
        }
    }

    override fun write(rawRecord: ByteArray): CompletableFuture<RecordId> {
        val hStreamRecord = RecordUtils.buildHStreamRecordFromRawRecord(rawRecord)
        return writeInternal(hStreamRecord)
    }

    override fun write(hRecord: HRecord): CompletableFuture<RecordId> {
        val hStreamRecord = RecordUtils.buildHStreamRecordFromHRecord(hRecord)
        return writeInternal(hStreamRecord)
    }

    override fun write(record: Record): CompletableFuture<RecordId> {
        val hStreamRecord = RecordUtils.buildHStreamRecordFromRecord(record)
        return writeInternal(hStreamRecord)
    }

    protected open fun writeInternal(hStreamRecord: HStreamRecord): CompletableFuture<RecordId> {
        val future = CompletableFuture<RecordId>()
        writeRecordScope.launch {
            try {
                val ids = writeHStreamRecords(listOf(hStreamRecord))
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
        tryTimes: Int
    ): List<RecordId> {
        // Note: A failed grpc call can throw both 'StatusException' and 'StatusRuntimeException'.
        //       This function is for handling them.
        suspend fun handleGRPCException(serverUrl: String, e: Throwable): List<RecordId> {
            logger.error("append with serverUrl [{}] error", serverUrl, e)
            val status = Status.fromThrowable(e)
            if (status.code == Status.UNAVAILABLE.code && tryTimes > 1) {
                delay(DefaultSettings.REQUEST_RETRY_INTERVAL_SECONDS * 1000)
                return appendWithRetry(appendRequest, orderingKey, tryTimes - 1)
            } else {
                throw HStreamDBClientException(e)
            }
        }

        check(tryTimes > 0)
        val serverUrl = lookupServerUrl(orderingKey)
        logger.info("try append with serverUrl [{}], current left tryTimes is [{}]", serverUrl, tryTimes)
        try {
            return HStreamApiGrpcKt.HStreamApiCoroutineStub(HStreamClientKtImpl.channelProvider.get(serverUrl))
                .append(appendRequest).recordIdsList.map(GrpcUtils::recordIdFromGrpc)
        } catch (e: StatusException) {
            return handleGRPCException(serverUrl, e)
        } catch (e: StatusRuntimeException) {
            return handleGRPCException(serverUrl, e)
        }
    }

    protected suspend fun writeHStreamRecords(hStreamRecords: List<HStreamRecord>): List<RecordId> {
        val res = emptyList<RecordId>().toMutableList()
        for ((key, rs) in hStreamRecords.groupBy { it.header.key }) {
            val appendRequest = AppendRequest.newBuilder().setStreamName(stream).addAllRecords(rs).build()
            res += appendWithRetry(appendRequest, key, DefaultSettings.APPEND_RETRY_MAX_TIMES)
        }
        return res
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ProducerKtImpl::class.java)
        private val writeRecordScope = CoroutineScope(Dispatchers.Default)
    }
}
