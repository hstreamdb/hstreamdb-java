package io.hstream.impl

import com.google.common.util.concurrent.AbstractService
import com.google.protobuf.InvalidProtocolBufferException
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.hstream.Consumer
import io.hstream.HRecordReceiver
import io.hstream.HStreamDBClientException
import io.hstream.RawRecordReceiver
import io.hstream.ReceivedHRecord
import io.hstream.ReceivedRawRecord
import io.hstream.internal.HStreamApiGrpcKt
import io.hstream.internal.HStreamRecord
import io.hstream.internal.LookupSubscriptionRequest
import io.hstream.internal.LookupSubscriptionWithOrderingKeyRequest
import io.hstream.internal.ReceivedRecord
import io.hstream.internal.StreamingFetchRequest
import io.hstream.internal.StreamingFetchResponse
import io.hstream.internal.WatchSubscriptionRequest
import io.hstream.internal.WatchSubscriptionResponse
import io.hstream.util.GrpcUtils
import io.hstream.util.RecordUtils
import kotlinx.coroutines.Job
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
    private lateinit var watchFuture: CompletableFuture<Unit>
    private val executorService = Executors.newSingleThreadExecutor()

    private suspend fun streamingFetchWithRetry(
        requestFlow: MutableSharedFlow<StreamingFetchRequest>,
        watchServer: String,
        orderingKey: String
    ) {
        // Note: A failed grpc call can throw both 'StatusException' and 'StatusRuntimeException'.
        //       This function is for handling them.
        suspend fun handleGRPCException(e: Throwable) {
            logger.error("streamingFetch error:", e)
            val status = Status.fromThrowable(e)
            // WARNING: Use status.code to make comparison because 'Status' contains
            //          extra information which varies from objects to objects.

            // 'status example':     Status{code=UNAVAILABLE, description=Connection closed
            //                       after GOAWAY. HTTP/2 error code: NO_ERROR, debug data:
            //                       Server shutdown, cause=null}
            // 'Status.UNAVAILABLE': Status{code=UNAVAILABLE, description=null, cause=null}
            if (status.code == Status.UNAVAILABLE.code) {
                delay(DefaultSettings.REQUEST_RETRY_INTERVAL_SECONDS * 1000)
                streamingFetchWithRetry(requestFlow, watchServer, orderingKey)
            } else if (status.code == Status.CANCELLED.code) {
                notifyStopped()
                logger.info("consumer [{}] is stopped", consumerName)
            } else {
                notifyFailed(HStreamDBClientException(e))
            }
        }

        if (!isRunning) return
        val server = lookupSubscriptionWithOrderingKey(orderingKey, watchServer)
        val stub = HStreamApiGrpcKt.HStreamApiCoroutineStub(HStreamClientKtImpl.channelProvider.get(server))
        try {
            // send an empty ack request to trigger streamingFetch.
            val initRequest = StreamingFetchRequest.newBuilder()
                .setSubscriptionId(subscriptionId)
                .setConsumerName(consumerName)
                .build()
            coroutineScope {
                launch {
                    // wait until stub.streamingFetch called
                    while (requestFlow.subscriptionCount.value == 0) {
                        delay(100)
                    }
                    requestFlow.emit(initRequest)
                }
                launch {
                    stub.streamingFetch(requestFlow).collect {
                        process(requestFlow, it)
                    }
                }
            }
        } catch (e: StatusException) {
            handleGRPCException(e)
        } catch (e: StatusRuntimeException) {
            handleGRPCException(e)
        }
    }

    private suspend fun watchSubscription() {
        val server = lookupSubscription()
        val stub = HStreamApiGrpcKt.HStreamApiCoroutineStub(HStreamClientKtImpl.channelProvider.get(server))
        val req = WatchSubscriptionRequest.newBuilder()
            .setSubscriptionId(subscriptionId)
            .setConsumerName(consumerName)
            .build()
        stub.watchSubscription(req).collect {
            val fetchers = HashMap<String, Job>()
            val fetcherFlows = HashMap<String, Flow<StreamingFetchRequest>>()
            if (it.changeCase == WatchSubscriptionResponse.ChangeCase.CHANGEADD) {
                val key = it.changeAdd.orderingKey
                coroutineScope {
                    val flow = MutableSharedFlow<StreamingFetchRequest>()
                    fetcherFlows[key] = flow
                    fetchers[key] = launch {
                        streamingFetchWithRetry(flow, server, key)
                    }
                }
            } else {
                val key = it.changeRemove.orderingKey
                val job = fetchers.remove(key)
                fetcherFlows.remove(key)
                if (job == null) {
                    logger.error("watching key not found: {}", key)
                    return@collect
                }
                job.cancel()
            }
        }
    }

    private suspend fun lookupSubscriptionWithOrderingKey(watchServer: String, orderingKey: String): String {
        val stub = HStreamApiGrpcKt.HStreamApiCoroutineStub(HStreamClientKtImpl.channelProvider.get(watchServer))
        val req = LookupSubscriptionWithOrderingKeyRequest.newBuilder()
            .setSubscriptionId(subscriptionId)
            .setOrderingKey(orderingKey)
            .build()
        val res = stub.lookupSubscriptionWithOrderingKey(req)
        return "${res.serverNode.host}:${res.serverNode.port}"
    }

    private suspend fun lookupSubscription(): String {
        return HStreamClientKtImpl.unaryCallCoroutine {
            val serverNode = it.lookupSubscription(
                LookupSubscriptionRequest.newBuilder().setSubscriptionId(subscriptionId).build()
            ).serverNode
            return@unaryCallCoroutine "${serverNode.host}:${serverNode.port}"
        }
    }

    private fun process(requestFlow: MutableSharedFlow<StreamingFetchRequest>, value: StreamingFetchResponse) {
        if (!isRunning) {
            return
        }

        val receivedRecords = value.receivedRecordsList
        for (receivedRecord in receivedRecords) {
            val responder = ResponderImpl(
                subscriptionId, requestFlow, consumerName, receivedRecord.recordId
            )

            executorService.submit {
                if (!isRunning) {
                    return@submit
                }

                if (RecordUtils.isRawRecord(receivedRecord)) {
                    logger.debug("consumer [{}] ready to process rawRecord [{}]", consumerName, receivedRecord.recordId)
                    try {
                        rawRecordReceiver!!.processRawRecord(toReceivedRawRecord(receivedRecord), responder)
                        logger.debug(
                            "consumer [{}] processes rawRecord [{}] done",
                            consumerName,
                            receivedRecord.recordId
                        )
                    } catch (e: Exception) {
                        logger.error(
                            "consumer [{}] processes rawRecord [{}] error",
                            consumerName,
                            receivedRecord.recordId,
                            e
                        )
                    }
                } else {
                    logger.debug("consumer [{}] ready to process hRecord [{}]", consumerName, receivedRecord.recordId)
                    try {
                        hRecordReceiver!!.processHRecord(
                            toReceivedHRecord(receivedRecord), responder
                        )
                        logger.debug("consumer [{}] processes hRecord [{}] done", consumerName, receivedRecord.recordId)
                    } catch (e: Exception) {
                        logger.error(
                            "consumer [{}] processes hRecord [{}] error",
                            consumerName,
                            receivedRecord.recordId,
                            e
                        )
                    }
                }
            }
        }
    }

    public override fun doStart() {
        Thread {
            try {
                logger.info("consumer [{}] is starting", consumerName)
                notifyStarted()
                watchFuture = futureForIO { watchSubscription() }
                logger.info("consumer [{}] is started", consumerName)
            } catch (e: Exception) {
                logger.error("consumer [{}] failed to start", consumerName, e)
                notifyFailed(HStreamDBClientException(e))
            }
        }.start()
    }

    public override fun doStop() {
        Thread {
            logger.info("consumer [{}] is stopping", consumerName)

            watchFuture.cancel(true)
            executorService.shutdown()
            try {
                executorService.awaitTermination(30, TimeUnit.SECONDS)
            } catch (e: InterruptedException) {
                logger.warn("consumer [{}] waits inner executor to be closed timeout", consumerName)
            }

            notifyStopped()
            logger.info("consumer [{}] is stopped", consumerName)
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
