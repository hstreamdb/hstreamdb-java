package io.hstream.impl

import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.hstream.CompressionType
import io.hstream.HRecord
import io.hstream.HStreamDBClientException
import io.hstream.Producer
import io.hstream.Record
import io.hstream.internal.AppendRequest
import io.hstream.internal.BatchHStreamRecords
import io.hstream.internal.BatchedRecord
import io.hstream.internal.HStreamRecord
import io.hstream.internal.ListShardsRequest
import io.hstream.internal.LookupShardRequest
import io.hstream.internal.Shard
import io.hstream.util.GrpcUtils
import io.hstream.util.RecordUtils
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.util.concurrent.CompletableFuture
import java.util.zip.GZIPOutputStream
import kotlin.collections.HashMap

open class ProducerKtImpl(private val client: HStreamClientKtImpl, private val stream: String) : Producer {
    private val serverUrls: HashMap<Long, String> = HashMap()
    private val serverUrlsLock: Mutex = Mutex()
    private val shards: List<Shard>

    init {
        val listShardRequest = ListShardsRequest.newBuilder()
            .setStreamName(stream)
            .build()
        val listShardResponse = client.unaryCallBlocked { it.listShards(listShardRequest) }
        shards = listShardResponse.shardsList
    }

    private suspend fun lookupServerUrl(shardId: Long, forceUpdate: Boolean = false): String {
        if (forceUpdate) {
            return updateServerUrl(shardId)
        }
        val server: String? = serverUrlsLock.withLock {
            return@withLock serverUrls[shardId]
        }
        if (server != null) {
            return server
        }
        return updateServerUrl(shardId)
    }

    private suspend fun updateServerUrl(shardId: Long): String {
        val req = LookupShardRequest.newBuilder()
            .setShardId(shardId)
            .build()
        val server = client.unaryCallCoroutine {
            val serverNode = it.lookupShard(req).serverNode
            return@unaryCallCoroutine "${serverNode.host}:${serverNode.port}"
        }
        serverUrlsLock.withLock {
            serverUrls[shardId] = server
        }
        logger.debug("updateServerUrl, key:$shardId, server:$server")
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
                val shardId = calculateShardIdByPartitionKey(hStreamRecord.header.key)
                val ids = writeHStreamRecords(listOf(hStreamRecord), shardId)
                future.complete(ids[0])
            } catch (e: Throwable) {
                future.completeExceptionally(e)
            }
        }
        return future
    }

    protected fun calculateShardIdByPartitionKey(partitionKey: String): Long {
        val hashcode = com.google.common.hash.Hashing.md5().hashString(partitionKey, StandardCharsets.UTF_8)
        val hashValue = BigInteger(hashcode.toString(), 16)
        for (shard in shards) {
            val start = BigInteger(shard.startHashRangeKey)
            val end = BigInteger(shard.endHashRangeKey)
            if (hashValue.compareTo(start) >= 0 && hashValue.compareTo(end) <= 0) {
                return shard.shardId
            }
        }

        check(false)
        return -1
    }

    private suspend fun appendWithRetry(
        appendRequest: AppendRequest,
        shardId: Long,
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
                return appendWithRetry(appendRequest, shardId, tryTimes - 1, true)
            } else {
                throw HStreamDBClientException(e)
            }
        }

        check(tryTimes > 0)

        val serverUrl = lookupServerUrl(shardId, forceUpdate)
        logger.info("try append with serverUrl [{}], current left tryTimes is [{}]", serverUrl, tryTimes)
        return try {
            client.getCoroutineStub(serverUrl)
                .append(appendRequest).recordIdsList.map(GrpcUtils::recordIdFromGrpc)
        } catch (e: StatusException) {
            handleGRPCException(serverUrl, e)
        } catch (e: StatusRuntimeException) {
            handleGRPCException(serverUrl, e)
        }
    }

    private fun compress(records: List<HStreamRecord>, compressionType: CompressionType): ByteString {
        val recordBatch = BatchHStreamRecords.newBuilder().addAllRecords(records).build()
        return when (compressionType) {
            CompressionType.NONE -> recordBatch.toByteString()
            CompressionType.GZIP -> {
                val byteArrayOutputStream = ByteArrayOutputStream()
                val gzipOutputStream = GZIPOutputStream(byteArrayOutputStream)
                gzipOutputStream.write(recordBatch.toByteArray())
                gzipOutputStream.flush()
                ByteString.copyFrom(byteArrayOutputStream.toByteArray())
            }
        }
    }

    protected suspend fun writeHStreamRecords(
        hStreamRecords: List<HStreamRecord>,
        shardId: Long,
        compressionType: CompressionType = CompressionType.NONE
    ): List<String> {
        val payload = compress(hStreamRecords, compressionType)
        val batchedRecord = BatchedRecord.newBuilder().setCompressionType(GrpcUtils.compressionTypeToInternal(compressionType))
            .setPayload(payload).setBatchSize(hStreamRecords.size).build()
        val appendRequest = AppendRequest.newBuilder().setStreamName(stream).setShardId(shardId).setRecords(batchedRecord).build()
        return appendWithRetry(appendRequest, shardId, DefaultSettings.APPEND_RETRY_MAX_TIMES)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ProducerKtImpl::class.java)
        private val writeRecordScope = CoroutineScope(Dispatchers.Default)
    }
}
