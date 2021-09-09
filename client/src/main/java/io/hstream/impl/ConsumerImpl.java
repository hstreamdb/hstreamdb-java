package io.hstream.impl;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.stub.StreamObserver;
import io.hstream.*;
import io.hstream.internal.*;
import io.hstream.util.GrpcUtils;
import io.hstream.util.RecordUtils;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerImpl extends AbstractService implements Consumer {
  private static final Logger logger = LoggerFactory.getLogger(ConsumerImpl.class);

  private HStreamApiGrpc.HStreamApiStub grpcStub;
  private HStreamApiGrpc.HStreamApiBlockingStub grpcBlockingStub;
  private String consumerName;
  private String subscriptionId;
  private RawRecordReceiver rawRecordReceiver;
  private HRecordReceiver hRecordReceiver;

  private ExecutorService executorService;

  private final StreamObserver<StreamingFetchResponse> responseStream;
  private final StreamObserver<StreamingFetchRequest> requestStream;

  private final AtomicBoolean inited = new AtomicBoolean(false);

  public ConsumerImpl(
      HStreamApiGrpc.HStreamApiStub grpcStub,
      HStreamApiGrpc.HStreamApiBlockingStub grpcBlockingStub,
      String consumerName,
      String subscriptionId,
      RawRecordReceiver rawRecordReceiver,
      HRecordReceiver hRecordReceiver) {
    this.grpcStub = grpcStub;
    this.grpcBlockingStub = grpcBlockingStub;
    this.consumerName = consumerName;
    this.subscriptionId = subscriptionId;
    this.rawRecordReceiver = rawRecordReceiver;
    this.hRecordReceiver = hRecordReceiver;

    this.executorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("receiver-running-pool-%d").build());

    responseStream =
        new StreamObserver<StreamingFetchResponse>() {
          @Override
          public void onNext(StreamingFetchResponse value) {
            if (inited.compareAndSet(false, true)) {
              // notifyStarted();
            }

            if (!isRunning()) {
              return;
            }

            List<ReceivedRecord> receivedRecords = value.getReceivedRecordsList();
            for (ReceivedRecord receivedRecord : receivedRecords) {
              Responder responder =
                  new ResponderImpl(
                      subscriptionId, requestStream, consumerName, receivedRecord.getRecordId());

              executorService.submit(
                  () -> {
                    if (!isRunning()) {
                      return;
                    }

                    if (RecordUtils.isRawRecord(receivedRecord)) {
                      logger.info("ready to process rawRecord");
                      try {
                        rawRecordReceiver.processRawRecord(
                            toReceivedRawRecord(receivedRecord), responder);
                      } catch (Exception e) {
                        logger.error("process rawRecord error", e);
                      }
                    } else {
                      logger.info("ready to process hrecord");
                      try {
                        hRecordReceiver.processHRecord(
                            toReceivedHRecord(receivedRecord), responder);

                      } catch (Exception e) {
                        logger.error("process hrecord error", e);
                      }
                    }
                  });
            }
          }

          @Override
          public void onError(Throwable t) {
            if (inited.compareAndSet(false, true)) {
              logger.error(
                  "consumer {} attach to subscription {} error: {}",
                  ConsumerImpl.this.consumerName,
                  ConsumerImpl.this.subscriptionId,
                  t);

              notifyFailed(t);
            } else {
              logger.error(
                  "consumer {} receive records from subscription {} error: {}",
                  ConsumerImpl.this.consumerName,
                  ConsumerImpl.this.subscriptionId,
                  t);
            }
          }

          @Override
          public void onCompleted() {}
        };

    this.requestStream = grpcStub.streamingFetch(responseStream);
  }

  @Override
  public void doStart() {

    logger.info("prepare to start consumer");

    StreamingFetchRequest initRequest =
        StreamingFetchRequest.newBuilder().setSubscriptionId(subscriptionId).build();
    requestStream.onNext(initRequest);

    logger.info("consumer {} started", consumerName);
    notifyStarted();
  }

  @Override
  public void doStop() {
    logger.info("prepare to stop consumer");

    new Thread(
            () -> {
              executorService.shutdown();
              logger.info("run shutdown done");
              try {
                executorService.awaitTermination(10, TimeUnit.SECONDS);
                logger.info("await terminate done");
              } catch (InterruptedException e) {
                logger.warn("wait timeout, consumer {} will be closed", consumerName);
              }
              logger.info("ready to notify stop");
              notifyStopped();
              logger.info("notify stop done");
            })
        .start();
  }

  private static ReceivedRawRecord toReceivedRawRecord(ReceivedRecord receivedRecord) {
    try {
      HStreamRecord hStreamRecord = HStreamRecord.parseFrom(receivedRecord.getRecord());
      byte[] rawRecord = RecordUtils.parseRawRecordFromHStreamRecord(hStreamRecord);
      ReceivedRawRecord receivedRawRecord =
          new ReceivedRawRecord(
              GrpcUtils.recordIdFromGrpc(receivedRecord.getRecordId()), rawRecord);
      return receivedRawRecord;
    } catch (InvalidProtocolBufferException e) {
      throw new HStreamDBClientException.InvalidRecordException("parse HStreamRecord error", e);
    }
  }

  private static ReceivedHRecord toReceivedHRecord(ReceivedRecord receivedRecord) {
    try {
      HStreamRecord hStreamRecord = HStreamRecord.parseFrom(receivedRecord.getRecord());
      HRecord hRecord = RecordUtils.parseHRecordFromHStreamRecord(hStreamRecord);
      ReceivedHRecord receivedHRecord =
          new ReceivedHRecord(GrpcUtils.recordIdFromGrpc(receivedRecord.getRecordId()), hRecord);
      return receivedHRecord;
    } catch (InvalidProtocolBufferException e) {
      throw new HStreamDBClientException.InvalidRecordException("parse HStreamRecord error", e);
    }
  }
}
