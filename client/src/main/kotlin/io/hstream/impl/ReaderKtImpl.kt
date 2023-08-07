package io.hstream.impl

import io.hstream.HStreamDBClientException
import io.hstream.Reader
import io.hstream.ReceivedRecord
import io.hstream.Record
import io.hstream.StreamShardOffset
import io.hstream.internal.CreateShardReaderRequest
import io.hstream.internal.DeleteShardReaderRequest
import io.hstream.internal.LookupShardReaderRequest
import io.hstream.internal.ReadShardRequest
import io.hstream.util.GrpcUtils
import io.hstream.util.RecordUtils
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.CompletableFuture

class ReaderKtImpl(
    private val client: HStreamClientKtImpl,
    private val streamName: String,
    private val shardId: Long,
    private val shardOffset: StreamShardOffset,
    private val timeoutMs: Int,
    private val readerId: String,
    private val requestTimeoutMs: Long,
) : Reader {

    private var serverUrl: String
    init {
        val createShardReaderRequest = CreateShardReaderRequest.newBuilder()
            .setStreamName(streamName)
            .setShardId(shardId)
            .setShardOffset(GrpcUtils.streamShardOffsetToGrpc(shardOffset))
            .setTimeout(timeoutMs)
            .setReaderId(readerId).build()
        client.unaryCallBlocked { it.createShardReader(createShardReaderRequest) }

        val lookupShardReaderRequest = LookupShardReaderRequest.newBuilder()
            .setReaderId(readerId).build()
        val lookupShardReaderResp = client.unaryCallBlocked { it.lookupShardReader(lookupShardReaderRequest) }
        serverUrl = lookupShardReaderResp.serverNode.host + ":" + lookupShardReaderResp.serverNode.port
        logger.info("created Reader [{}] for stream [{}] shard [{}]", readerId, streamName, shardId)
    }

    override fun read(maxRecords: Int): CompletableFuture<MutableList<ReceivedRecord>> {
        val readFuture = CompletableFuture<MutableList<ReceivedRecord>>()
        readerScope.launch {
            try {
                val readShardRequest = ReadShardRequest.newBuilder().setReaderId(readerId).setMaxRecords(maxRecords).build()
                val readShardResponse = client.getCoroutineStubWithTimeoutMs(serverUrl, requestTimeoutMs)
                    .readShard(readShardRequest)
                val res = readShardResponse.receivedRecordsList.flatMap {
                    RecordUtils.decompress(it).map { receivedHStreamRecord ->
                        val hStreamRecord = receivedHStreamRecord.record
                        val header = RecordUtils.parseRecordHeaderFromHStreamRecord(hStreamRecord)
                        val publishTime = it.record.publishTime
                        val createdTime = Instant.ofEpochSecond(publishTime.seconds, publishTime.nanos.toLong())
                        val record = if (RecordUtils.isRawRecord(hStreamRecord)) {
                            val rawRecord = RecordUtils.parseRawRecordFromHStreamRecord(hStreamRecord)
                            Record.newBuilder().rawRecord(rawRecord).partitionKey(header.partitionKey).build()
                        } else {
                            val hRecord = RecordUtils.parseHRecordFromHStreamRecord(hStreamRecord)
                            Record.newBuilder().hRecord(hRecord).partitionKey(header.partitionKey).build()
                        }
                        ReceivedRecord(GrpcUtils.recordIdFromGrpc(receivedHStreamRecord.recordId), record, createdTime)
                    }
                }
                readFuture.complete(res as MutableList<ReceivedRecord>?)
            } catch (e: Throwable) {
                readFuture.completeExceptionally(HStreamDBClientException(e))
            }
        }
        return readFuture
    }

    override fun close() {
        val deleteShardReaderRequest = DeleteShardReaderRequest.newBuilder()
            .setReaderId(readerId)
            .build()
        runBlocking {
            client.getCoroutineStubWithTimeoutMs(serverUrl, requestTimeoutMs).deleteShardReader(deleteShardReaderRequest)
        }

        logger.info("Reader [{}] closed", readerId)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ReaderKtImpl::class.java)
        private val readerScope = CoroutineScope(Dispatchers.Default)
    }
}
