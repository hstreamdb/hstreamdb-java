package io.hstream.impl

import com.google.common.util.concurrent.AbstractService
import com.google.protobuf.InvalidProtocolBufferException
import io.grpc.Status
import io.hstream.Consumer
import io.hstream.HRecordReceiver
import io.hstream.HStreamDBClientException
import io.hstream.RawRecordReceiver
import io.hstream.ReceivedHRecord
import io.hstream.ReceivedRawRecord
import io.hstream.internal.HStreamApiGrpcKt
import io.hstream.internal.HStreamRecord
import io.hstream.internal.LookupSubscriptionRequest
import io.hstream.internal.ReceivedRecord
import io.hstream.internal.StreamingFetchRequest
import io.hstream.internal.StreamingFetchResponse
import io.hstream.util.GrpcUtils
import io.hstream.util.RecordUtils
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class ConsumerKtImpl(
    private val consumerName: String,
    private val subscriptionId: String,
    private val rawRecordReceiver: RawRecordReceiver?,
    private val hRecordReceiver: HRecordReceiver?
) : AbstractService(), Consumer {
    private lateinit var serverUrl: String
    private val ackFlow = MutableSharedFlow<StreamingFetchRequest>()
    private lateinit var streamingFetchFuture: CompletableFuture<Unit>
    private val executorService = Executors.newSingleThreadExecutor()

    private suspend fun streamingFetchWithRetry(requestFlow: Flow<StreamingFetchRequest>) {
        if (!isRunning) return
        check(serverUrl != null)
        val stub = HStreamApiGrpcKt.HStreamApiCoroutineStub(HStreamClientKtImpl.channelProvider.get(serverUrl))
        try {
            // Send a init request when connecting to a new node.
            val initRequest = StreamingFetchRequest.newBuilder()
                .setSubscriptionId(subscriptionId)
                .setConsumerName(consumerName)
                .build()
            coroutineScope {
                launch {
                    // wait until stub.streamingFetch called
                    while (ackFlow.subscriptionCount.value == 0) {
                        delay(100)
                    }
                    ackFlow.emit(initRequest)
                }
                launch {
                    stub.streamingFetch(requestFlow).collect {
                        process(it)
                    }
                }
            }
        } catch (e: Exception) {
            logger.error("streamingFetch error: ", e)
            val status = Status.fromThrowable(e)
            // WARNING: Use status.code to make comparison because 'Status' contains
            //          extra information which varies from objects to objects.

            // 'status example':     Status{code=UNAVAILABLE, description=Connection closed
            //                       after GOAWAY. HTTP/2 error code: NO_ERROR, debug data:
            //                       Server shutdown, cause=null}
            // 'Status.UNAVAILABLE': Status{code=UNAVAILABLE, description=null, cause=null}
            if (status.code == Status.UNAVAILABLE.code) {
                delay(DefaultSettings.REQUEST_RETRY_INTERVAL_SECONDS * 1000)
                refreshServerUrl()
                streamingFetchWithRetry(requestFlow)
            } else {
                notifyFailed(HStreamDBClientException(e))
            }
        }
    }

    private fun process(value: StreamingFetchResponse) {
        if (!isRunning) {
            return
        }

        val receivedRecords = value.receivedRecordsList
        for (receivedRecord in receivedRecords) {
            val responder = ResponderImpl(
                subscriptionId, ackFlow, consumerName, receivedRecord.recordId
            )

            executorService.submit {
                if (!isRunning) {
                    return@submit
                }

                if (RecordUtils.isRawRecord(receivedRecord)) {
                    logger.info("ready to process rawRecord")
                    try {
                        rawRecordReceiver!!.processRawRecord(toReceivedRawRecord(receivedRecord), responder)
                        logger.info("process rawRecord {} done", receivedRecord.recordId)
                    } catch (e: Exception) {
                        logger.error("process rawRecord error", e)
                    }
                } else {
                    logger.info("ready to process hrecord")
                    try {
                        hRecordReceiver!!.processHRecord(
                            toReceivedHRecord(receivedRecord), responder
                        )
                        logger.info("process hRecord {} done", receivedRecord.recordId)
                    } catch (e: Exception) {
                        logger.error("process hrecord error", e)
                    }
                }
            }
        }
    }

    private suspend fun lookupServerUrl(): String {
        return HStreamClientKtImpl.unaryCallCoroutine {
            val serverNode = it.lookupSubscription(LookupSubscriptionRequest.newBuilder().setSubscriptionId(subscriptionId).build()).serverNode
            return@unaryCallCoroutine "${serverNode.host}:${serverNode.port}"
        }
    }

    private suspend fun refreshServerUrl() {
        serverUrl = lookupServerUrl()
    }

    private fun refreshServerUrlBlocked() {
        serverUrl = HStreamClientKtImpl.unaryCallBlocked {
            val serverNode = it.lookupSubscription(LookupSubscriptionRequest.newBuilder().setSubscriptionId(subscriptionId).build()).serverNode
            return@unaryCallBlocked "${serverNode.host}:${serverNode.port}"
        }
    }

    public override fun doStart() {
        Thread {
            try {
                logger.info("consumer {} is starting", consumerName)
                refreshServerUrlBlocked()
                notifyStarted()
                streamingFetchFuture = futureForIO { streamingFetchWithRetry(ackFlow) }
                logger.info("consumer {} is started", consumerName)
            } catch (e: Exception) {
                logger.error("consumer {} failed to start", consumerName, e)
                notifyFailed(HStreamDBClientException(e))
            }
        }.start()
    }

    public override fun doStop() {
        Thread {
            logger.info("consumer {} is stopping", consumerName)

            streamingFetchFuture.cancel(true)
            executorService.shutdown()
            logger.info("run shutdown done")
            try {
                executorService.awaitTermination(10, TimeUnit.SECONDS)
                logger.info("await terminate done")
            } catch (e: InterruptedException) {
                logger.warn("wait timeout, consumer {} will be closed", consumerName)
            }

            notifyStopped()
            logger.info("consumer {} is stopped", consumerName)
        }
            .start()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ConsumerKtImpl::class.java)
        private fun toReceivedRawRecord(receivedRecord: ReceivedRecord): ReceivedRawRecord {
            return try {
                val hStreamRecord = HStreamRecord.parseFrom(receivedRecord.record)
                val rawRecord = RecordUtils.parseRawRecordFromHStreamRecord(hStreamRecord)
                ReceivedRawRecord(
                    GrpcUtils.recordIdFromGrpc(receivedRecord.recordId), rawRecord
                )
            } catch (e: InvalidProtocolBufferException) {
                throw HStreamDBClientException.InvalidRecordException("parse HStreamRecord error", e)
            }
        }

        private fun toReceivedHRecord(receivedRecord: ReceivedRecord): ReceivedHRecord {
            return try {
                val hStreamRecord = HStreamRecord.parseFrom(receivedRecord.record)
                val hRecord = RecordUtils.parseHRecordFromHStreamRecord(hStreamRecord)
                ReceivedHRecord(GrpcUtils.recordIdFromGrpc(receivedRecord.recordId), hRecord)
            } catch (e: InvalidProtocolBufferException) {
                throw HStreamDBClientException.InvalidRecordException("parse HStreamRecord error", e)
            }
        }
    }
}
