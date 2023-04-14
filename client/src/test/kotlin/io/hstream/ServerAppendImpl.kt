package io.hstream

import com.github.luben.zstd.Zstd
import com.github.luben.zstd.ZstdException
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import io.hstream.internal.AppendRequest
import io.hstream.internal.AppendResponse
import io.hstream.internal.BatchHStreamRecords
import io.hstream.internal.BatchedRecord
import io.hstream.internal.CompressionType
import io.hstream.internal.HStreamRecord
import io.hstream.internal.RecordId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.GZIPInputStream

class ServerAppendImpl(
    private val recordsByStreamAndShard: MutableMap<String, MutableMap<Long, MutableList<HStreamRecord>>>,
    private val globalLsnCnt: AtomicInteger
) {
    fun append(request: AppendRequest?, responseObserver: StreamObserver<AppendResponse>?) {
        val streamName = request!!.streamName
        val shardId = request.shardId
        val records: BatchedRecord = request.records
        val hStreamRecords = batchedRecordToRecords(records)
        val shardRecords = recordsByStreamAndShard.getOrPut(streamName) { mutableMapOf() }
            .getOrPut(shardId) { mutableListOf() }
        shardRecords.addAll(hStreamRecords)

        val recordIds = (0 until records.batchSize).map { i ->
            val batchLsm = globalLsnCnt.incrementAndGet()
            buildRecordId(shardId, batchLsm.toLong(), i)
        }

        val appendResponse = AppendResponse.newBuilder()
            .setStreamName(streamName)
            .setShardId(shardId)
            .addAllRecordIds(recordIds)
            .build()

        responseObserver!!.onNext(appendResponse)
        responseObserver.onCompleted()
    }

    private fun buildRecordId(shardId: Long, batchId: Long, batchIndex: Int): RecordId {
        return RecordId.newBuilder()
            .setShardId(shardId)
            .setBatchId(batchId)
            .setBatchIndex(batchIndex)
            .build()
    }

    private fun batchedRecordToRecords(batchedRecord: BatchedRecord): List<HStreamRecord> {
        val compressionType = batchedRecord.compressionType
        val batchSize = batchedRecord.batchSize
        val payload: ByteString = batchedRecord.payload

        val recordsList = decompressPayload(compressionType, payload).recordsList
        assert(recordsList.size == batchSize)
        return recordsList
    }

    private fun decompressPayload(compressionType: CompressionType, payload: ByteString): BatchHStreamRecords {
        val decompressedPayload: ByteString = when (compressionType) {
            CompressionType.None -> payload
            CompressionType.Gzip -> {
                val inputStream = GZIPInputStream(payload.newInput())
                val outputStream = ByteArrayOutputStream()
                inputStream.copyTo(outputStream)
                ByteString.copyFrom(outputStream.toByteArray())
            }
            CompressionType.Zstd -> {
                try {
                    val compressedBytes = payload.toByteArray()
                    val decompressedBytes = Zstd.decompress(compressedBytes, compressedBytes.size)
                    ByteString.copyFrom(decompressedBytes)
                } catch (ex: ZstdException) {
                    throw IOException("Failed to decompress Zstd payload", ex)
                }
            }
            else -> throw IllegalArgumentException("Unsupported compression type: $compressionType")
        }

        return BatchHStreamRecords.parseFrom(decompressedPayload)
    }
}

class ServerAppendImplTest {
    @Test
    @Disabled("debug")
    fun `append adds records to the correct shard and returns RecordIds`() {
        val streamName = "test-stream"
        val shardId = 0L
        val records = BatchedRecord.newBuilder()
            .setCompressionType(CompressionType.None)
            .setBatchSize(2)
            .setPayload(createPayload())
            .build()
        val request = AppendRequest.newBuilder()
            .setStreamName(streamName)
            .setShardId(shardId)
            .setRecords(records)
            .build()

        val recordsByStreamAndShard = mutableMapOf<String, MutableMap<Long, MutableList<HStreamRecord>>>()
        val globalLsnCnt = AtomicInteger(0)
        val serverAppendImpl = ServerAppendImpl(recordsByStreamAndShard, globalLsnCnt)

        val responseObserver = object : StreamObserver<AppendResponse> {
            val receivedRecordIds = mutableListOf<RecordId>()
            override fun onNext(response: AppendResponse) {
                assertEquals(streamName, response.streamName)
                assertEquals(shardId, response.shardId)
                receivedRecordIds.addAll(response.recordIdsList)
            }
            override fun onError(t: Throwable) { throw t }
            override fun onCompleted() {}
        }

        serverAppendImpl.append(request, responseObserver)

        assertEquals(2, recordsByStreamAndShard[streamName]?.get(shardId)?.size)
        assertEquals(
            RecordId.newBuilder()
                .setShardId(shardId)
                .setBatchId(1)
                .setBatchIndex(0)
                .build(),
            responseObserver.receivedRecordIds[0]
        )
        assertEquals(
            RecordId.newBuilder()
                .setShardId(shardId)
                .setBatchId(1)
                .setBatchIndex(1)
                .build(),
            responseObserver.receivedRecordIds[1]
        )
    }

    private fun createPayload(): ByteString {
        val record1 = HStreamRecord.newBuilder().setPayload(ByteString.copyFromUtf8("test1")).build()
        val record2 = HStreamRecord.newBuilder().setPayload(ByteString.copyFromUtf8("test2")).build()
        val batchHStreamRecords = BatchHStreamRecords.newBuilder()
            .addRecords(record1)
            .addRecords(record2)
            .build()
        return batchHStreamRecords.toByteString()
    }
}
