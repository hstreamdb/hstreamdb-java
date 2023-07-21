package io.hstream.impl

import com.google.common.util.concurrent.AbstractService
import com.google.protobuf.InvalidProtocolBufferException
import io.hstream.HStreamDBClientException
import io.hstream.ReceivedRecord
import io.hstream.Record
import io.hstream.StreamShardOffset
import io.hstream.StreamShardReader
import io.hstream.StreamShardReaderBatchReceiver
import io.hstream.StreamShardReaderReceiver
import io.hstream.internal.LookupShardReaderRequest
import io.hstream.internal.ReadShardStreamRequest
import io.hstream.internal.ReadShardStreamResponse
import io.hstream.util.GrpcUtils
import io.hstream.util.RecordUtils
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Executors
import kotlin.streams.toList

class StreamShardReaderKtImpl(
    private val client: HStreamClientKtImpl,
    private val streamName: String,
    private val shardId: Long,
    private val from: StreamShardOffset,
    private val maxReadBatches: Long,
    private val until: StreamShardOffset?,
    private val receiver: StreamShardReaderReceiver?,
    private val batchReceiver: StreamShardReaderBatchReceiver?,
) : AbstractService(), StreamShardReader {
    private val readerScope = CoroutineScope(Dispatchers.IO)
    private val readerName: String = UUID.randomUUID().toString()
    private val executorService = Executors.newSingleThreadExecutor()

    override fun doStart() {
        Thread {

            try {
                logger.info("streamShardReader $readerName is starting")
                val lookupShardReaderRequest = LookupShardReaderRequest.newBuilder()
                    .setReaderId(readerName).build()
                val lookupShardReaderResp = client.unaryCallBlocked { it.lookupShardReader(lookupShardReaderRequest) }
                val serverUrl = lookupShardReaderResp.serverNode.host + ":" + lookupShardReaderResp.serverNode.port
                val readerBuilder = ReadShardStreamRequest.newBuilder()
                    .setReaderId(readerName)
                    .setShardId(shardId)
                    .setFrom(GrpcUtils.streamShardOffsetToGrpc(from))
                    .setMaxReadBatches(maxReadBatches)
                if (until != null) {
                    readerBuilder.until = GrpcUtils.streamShardOffsetToGrpc(until)
                }
                val respFlow = client.getCoroutineStub(serverUrl).readShardStream(readerBuilder.build())
                notifyStarted()
                readerScope.launch {
                try{
                    respFlow.collect {
                        process(it)
                    }
                    } catch (e: Exception) {
                      logger.error("steamShardReader $readerName failed", e)
                      notifyFailed(HStreamDBClientException(e))
                    }
                }
            } catch (e: Exception) {
                logger.error("steamShardReader $readerName failed to start", e)
                notifyFailed(HStreamDBClientException(e))
            }
        }.start()
    }

    override fun doStop() {
        Thread {
            executorService.shutdownNow()
            notifyStopped()
        }
            .start()
    }

    private fun process(value: ReadShardStreamResponse) {
        if (!isRunning) {
            return
        }

        for (receivedRecord in value.receivedRecordsList) {

            val receivedHStreamRecords = RecordUtils.decompress(receivedRecord)
            val createdTimestamp = receivedRecord.record.publishTime
            val createdTime = Instant.ofEpochSecond(createdTimestamp.seconds, createdTimestamp.nanos.toLong())

            if (batchReceiver != null) {
                executorService.submit {
                    if (!isRunning) {
                        return@submit
                    }

                    try {
                        val records = receivedHStreamRecords.stream().map { toReceivedRecord(it, createdTime) }.toList()
                        batchReceiver.process(records)
                    } catch (e: Exception) {
                        notifyFailed(e)
                    }
                }
            } else {
                for (receivedHStreamRecord in receivedHStreamRecords) {
                    executorService.submit {
                        if (!isRunning) {
                            return@submit
                        }

                        try {
                            receiver!!.process(toReceivedRecord(receivedHStreamRecord, createdTime))
                        } catch (e: Exception) {
                            notifyFailed(e)
                        }
                    }
                }
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(StreamShardReaderKtImpl::class.java)

        private fun toReceivedRecord(receivedHStreamRecord: ReceivedHStreamRecord, createdTime: Instant): ReceivedRecord {
            return try {
                val header = RecordUtils.parseRecordHeaderFromHStreamRecord(receivedHStreamRecord.record)
                if (RecordUtils.isRawRecord(receivedHStreamRecord.record)) {

                    val rawRecord = RecordUtils.parseRawRecordFromHStreamRecord(receivedHStreamRecord.record)
                    ReceivedRecord(
                        GrpcUtils.recordIdFromGrpc(receivedHStreamRecord.recordId),
                        Record.newBuilder().partitionKey(header.partitionKey).rawRecord(rawRecord).build(),
                        createdTime
                    )
                } else {
                    val hRecord = RecordUtils.parseHRecordFromHStreamRecord(receivedHStreamRecord.record)
                    ReceivedRecord(
                        GrpcUtils.recordIdFromGrpc(receivedHStreamRecord.recordId),
                        Record.newBuilder().partitionKey(header.partitionKey).hRecord(hRecord).build(),
                        createdTime
                    )
                }
            } catch (e: InvalidProtocolBufferException) {
                throw HStreamDBClientException.InvalidRecordException("parse HStreamRecord error", e)
            }
        }
    }
}
