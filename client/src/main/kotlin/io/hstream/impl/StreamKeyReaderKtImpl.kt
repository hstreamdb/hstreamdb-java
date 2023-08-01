package io.hstream.impl

import com.google.protobuf.InvalidProtocolBufferException
import io.hstream.*
import io.hstream.ReceivedRecord
import io.hstream.internal.*
import io.hstream.util.GrpcUtils
import io.hstream.util.RecordUtils
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableSharedFlow
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

class StreamKeyReaderKtImpl(
    private val client: HStreamClientKtImpl,
    private val streamName: String,
    private val key: String,
    private val from: StreamShardOffset,
    private val until: StreamShardOffset?,
    private val bufferSize: Int,
) : StreamKeyReader {
    private val readerScope = CoroutineScope(Dispatchers.IO)
    private val readerName: String = UUID.randomUUID().toString()
    private val executorService = Executors.newSingleThreadExecutor()
    private val requestFlow = MutableSharedFlow<ReadStreamByKeyRequest>()
    private val buffer = ArrayBlockingQueue<ReceivedRecord>(bufferSize)
    private val exceptionRef: AtomicReference<Exception> = AtomicReference(null)
    private val isStopped: AtomicBoolean = AtomicBoolean(false)

    init {
       doStart()
    }

    private  fun doStart() {
        logger.info("streamKeyReader $readerName is starting")
        val lookupRequest = LookupKeyRequest.newBuilder()
                .setPartitionKey(key)
                .build()
        val lookupResp = client.unaryCallBlocked { it.lookupKey(lookupRequest) }
        val serverUrl = lookupResp.host + ":" + lookupResp.port
        val requestBuilder = ReadStreamByKeyRequest.newBuilder()
            .setReaderId(readerName)
            .setStreamName(streamName)
            .setKey(key)
            .setFrom(GrpcUtils.streamShardOffsetToGrpc(from))
            .setReadRecordCount(bufferSize.toLong())
        if (until != null) {
            requestBuilder.until = GrpcUtils.streamShardOffsetToGrpc(until)
        }
        val respFlow = client.getCoroutineStub(serverUrl).readStreamByKey(requestFlow)
        readerScope.launch {
           launch {
               // wait until rpc called
               while (requestFlow.subscriptionCount.value == 0) {
                   delay(100)
               }
               try{
                   requestFlow.emit(requestBuilder.build())
               } catch (e: Exception) {
                   logger.error("steamKeyReader $readerName failed", e)
                   exceptionRef.compareAndSet(null, e)
                   isStopped.set(true)
               }
           }
           launch {
               try {
                   respFlow.collect {
                       saveToBuffer(it)
                   }
                } catch (e: Exception) {
                   logger.error("steamKeyReader $readerName failed", e)
                   exceptionRef.compareAndSet(null, e)
                   isStopped.set(true)
                }
               // wait for saveToBuffer complete
               delay(100)
               isStopped.set(true)
               logger.info("server stopped")

           }
        }
    }

    override fun close() {
        readerScope.cancel()
        executorService.shutdownNow()
        logger.info("StreamKeyReader $readerName closed")
    }

    override fun hasNext() : Boolean {
       return !isStopped.get() || !buffer.isEmpty()
    }

    override fun next(): ReceivedRecord? {

        var res: ReceivedRecord? = null

        while (res == null) {
            res = buffer.poll(100, TimeUnit.MILLISECONDS)

            if(res == null) {
                val e = exceptionRef.get()
                if(e != null) throw e

                if(isStopped.get()) return null
            }
        }

        readerScope.launch {
            try{
                requestFlow.emit(ReadStreamByKeyRequest.newBuilder().setReadRecordCount(1).build())
            } catch (e: Exception) {
                logger.error("steamKeyReader $readerName failed", e)
                exceptionRef.compareAndSet(null, e)
                isStopped.set(true)
            }
        }

        return res
    }

    private fun saveToBuffer(value: ReadStreamByKeyResponse) {
        for ((i, receivedRecord) in value.receivedRecordsList.withIndex()) {
            val recordId = value.getRecordIds(i)
            executorService.submit {
                val res = toReceivedRecord(receivedRecord, recordId, Instant.now())
                buffer.put(res)
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(StreamKeyReaderKtImpl::class.java)

        private fun toReceivedRecord(hStreamRecord: HStreamRecord, recordId: RecordId, createdTime: Instant): ReceivedRecord {
            return try {
                val header = RecordUtils.parseRecordHeaderFromHStreamRecord(hStreamRecord)
                if (RecordUtils.isRawRecord(hStreamRecord)) {

                    val rawRecord = RecordUtils.parseRawRecordFromHStreamRecord(hStreamRecord)
                    ReceivedRecord(
                        GrpcUtils.recordIdFromGrpc(recordId),
                        Record.newBuilder().partitionKey(header.partitionKey).rawRecord(rawRecord).build(),
                        createdTime
                    )
                } else {
                    val hRecord = RecordUtils.parseHRecordFromHStreamRecord(hStreamRecord)
                    ReceivedRecord(
                        GrpcUtils.recordIdFromGrpc(recordId),
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
