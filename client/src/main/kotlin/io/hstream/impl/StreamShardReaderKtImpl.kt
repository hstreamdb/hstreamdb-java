package io.hstream.impl

import com.google.common.util.concurrent.AbstractService
import com.google.protobuf.InvalidProtocolBufferException
import io.hstream.HStreamDBClientException
import io.hstream.ReceivedRecord
import io.hstream.Record
import io.hstream.StreamShardOffset
import io.hstream.StreamShardReader
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

class StreamShardReaderKtImpl(
    private val client: HStreamClientKtImpl,
    private val streamName: String,
    private val shardId: Long,
    private val shardOffset: StreamShardOffset,
    private val receiver: StreamShardReaderReceiver,
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
                val respFlow = client.getCoroutineStub(serverUrl).readShardStream(
                    ReadShardStreamRequest.newBuilder().setReaderId(readerName).setShardId(shardId)
                        .setTimeout(1000)
                        .setShardOffset(GrpcUtils.streamShardOffsetToGrpc(shardOffset)).build()
                )
                notifyStarted()
                readerScope.launch {
                    respFlow.collect {
                        process(it)
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
            for (receivedHStreamRecord in receivedHStreamRecords) {

                executorService.submit {
                    if (!isRunning) {
                        return@submit
                    }

                    try {
                        receiver.process(toReceivedRecord(receivedHStreamRecord, createdTime))
                    } catch (e: Exception) {
                        notifyFailed(e)
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
