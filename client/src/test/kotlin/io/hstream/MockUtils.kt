package io.hstream

import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.protobuf.kotlin.toByteStringUtf8
import io.hstream.internal.BatchHStreamRecords
import io.hstream.internal.BatchedRecord
import io.hstream.internal.CompressionType
import io.hstream.internal.HStreamRecord
import io.hstream.internal.HStreamRecordHeader
import java.time.Instant
import kotlin.random.Random

fun buildRandomBatchedGenericRecord(numberOfRecords: Int, payload: ByteString, flag: HStreamRecordHeader.Flag): BatchedRecord {
    val time = Instant.now()
    val timestamp = Timestamp.newBuilder().setSeconds(time.epochSecond)
        .setNanos(time.nano).build()
    val records = mutableListOf<HStreamRecord>()
    repeat(numberOfRecords) {
        val record = HStreamRecord.newBuilder()
            .setHeader(
                HStreamRecordHeader.newBuilder()
                    .setFlag(flag)
                    .build()
            )
            .setPayload(payload)
            .build()
        records.add(record)
    }
    val batch = BatchHStreamRecords.newBuilder()
        .addAllRecords(records)
        .build()
    val serializedBatch = batch.toByteString()
    return BatchedRecord.newBuilder()
        .setCompressionType(CompressionType.None)
        .setPublishTime(timestamp)
        .setBatchSize(records.size)
        .setPayload(serializedBatch)
        .build()
}

fun buildRandomBatchedHRecord(numberOfRecords: Int): BatchedRecord {
    val payload = HRecord.newBuilder().put("some", false).put("any", "none").build().toByteString()
    return buildRandomBatchedGenericRecord(numberOfRecords, payload, HStreamRecordHeader.Flag.JSON)
}

fun buildRandomBatchedRawRecord(numberOfRecords: Int): BatchedRecord {
    val payload = ("Record " + Random.nextLong()).toByteStringUtf8()
    return buildRandomBatchedGenericRecord(numberOfRecords, payload, HStreamRecordHeader.Flag.RAW)
}
