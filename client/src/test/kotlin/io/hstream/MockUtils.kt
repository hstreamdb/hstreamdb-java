package io.hstream

import com.google.protobuf.Timestamp
import com.google.protobuf.kotlin.toByteStringUtf8
import io.hstream.internal.BatchHStreamRecords
import io.hstream.internal.BatchedRecord
import io.hstream.internal.CompressionType
import io.hstream.internal.HStreamRecord
import io.hstream.internal.HStreamRecordHeader
import java.time.Instant

fun buildRandomBatchedRecord(numberOfRecords: Int): BatchedRecord {
    val time = Instant.now()
    val timestamp = Timestamp.newBuilder().setSeconds(time.epochSecond)
        .setNanos(time.nano).build()
    val records = mutableListOf<HStreamRecord>()
    repeat(numberOfRecords) {
        val data = "Record $it"
        val record = HStreamRecord.newBuilder()
            .setHeader(
                HStreamRecordHeader.newBuilder()
                    .setFlag(HStreamRecordHeader.Flag.RAW)
                    .build()
            )
            .setPayload(data.toByteStringUtf8())
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
