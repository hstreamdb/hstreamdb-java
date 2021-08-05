package io.hstream.impl;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.stub.StreamObserver;
import io.hstream.*;
import io.hstream.util.RecordUtils;
import java.util.concurrent.*;
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

  private static final long pollTimeoutMs = 1000;
  private static final int maxPollRecords = 1000;

  private ExecutorService executorService;
  private ScheduledExecutorService scheduledExecutorService;

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
    this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
  }

  @Override
  public void doStart() {

    final ConsumerHeartbeatRequest consumerHeartbeatRequest =
        ConsumerHeartbeatRequest.newBuilder().setSubscriptionId(this.subscriptionId).build();
    final StreamObserver<ConsumerHeartbeatResponse> heartbeatObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(ConsumerHeartbeatResponse response) {
            logger.info(
                "consumer {} received heartbeat response for subscription {}",
                consumerName,
                response.getSubscriptionId());
          }

          @Override
          public void onError(Throwable t) {
            logger.error("consumer {} send heartbeat error: {}", ConsumerImpl.this.consumerName, t);
            throw new HStreamDBClientException.ConsumerException("send heartbeat error", t);
          }

          @Override
          public void onCompleted() {}
        };

    FetchRequest fetchRequest =
        FetchRequest.newBuilder()
            .setSubscriptionId(subscriptionId)
            .setTimeout(pollTimeoutMs)
            .setMaxSize(maxPollRecords)
            .build();

    SubscribeRequest subscribeRequest =
        SubscribeRequest.newBuilder().setSubscriptionId(subscriptionId).build();

    final StreamObserver<SubscribeResponse> subscribeResponseStreamObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(SubscribeResponse response) {
            logger.info(
                "consumer {} attach to subscription {} successfully",
                ConsumerImpl.this.consumerName,
                response.getSubscriptionId());

            executorService.submit(
                () -> {
                  do {
                    logger.info("start fetch and processing ...");
                    FetchResponse fetchResponse = grpcBlockingStub.fetch(fetchRequest);
                    logger.info("fetched {} records", fetchResponse.getReceivedRecordsCount());
                    for (ReceivedRecord receivedRecord : fetchResponse.getReceivedRecordsList()) {
                      logger.info("enter for loop");
                      if (RecordUtils.isRawRecord(receivedRecord)) {
                        logger.info("ready to process rawRecord");
                        rawRecordReceiver.processRawRecord(
                            toReceivedRawRecord(receivedRecord),
                            new ResponderImpl(
                                grpcBlockingStub, subscriptionId, receivedRecord.getRecordId()));
                      } else {
                        logger.info("ready to process hrecord");
                        hRecordReceiver.processHRecord(
                            toReceivedHRecord(receivedRecord),
                            new ResponderImpl(
                                grpcBlockingStub, subscriptionId, receivedRecord.getRecordId()));
                      }
                    }
                    logger.info("processed {} records", fetchResponse.getReceivedRecordsCount());
                  } while (isRunning());
                });

            scheduledExecutorService.scheduleAtFixedRate(
                () -> grpcStub.sendConsumerHeartbeat(consumerHeartbeatRequest, heartbeatObserver),
                0,
                1,
                TimeUnit.SECONDS);

            ConsumerImpl.this.notifyStarted();
          }

          @Override
          public void onError(Throwable t) {
            logger.error(
                "consumer {} attach to subscription {} error: {}",
                ConsumerImpl.this.consumerName,
                ConsumerImpl.this.subscriptionId,
                t);
            notifyFailed(
                new HStreamDBClientException.SubscribeException("consumer subscribe error", t));
          }

          @Override
          public void onCompleted() {}
        };

    grpcStub.subscribe(subscribeRequest, subscribeResponseStreamObserver);
  }

  @Override
  public void doStop() {
    logger.info("prepare to stop consumer");

    scheduledExecutorService.shutdownNow();
    executorService.shutdownNow();

    notifyStopped();

    logger.info("consumer has been stopped");
  }

  private static ReceivedRawRecord toReceivedRawRecord(ReceivedRecord receivedRecord) {
    try {
      HStreamRecord hStreamRecord = HStreamRecord.parseFrom(receivedRecord.getRecord());
      byte[] rawRecord = RecordUtils.parseRawRecordFromHStreamRecord(hStreamRecord);
      ReceivedRawRecord receivedRawRecord =
          new ReceivedRawRecord(receivedRecord.getRecordId(), rawRecord);
      return receivedRawRecord;
    } catch (InvalidProtocolBufferException e) {
      throw new HStreamDBClientException.InvalidRecordException("parse HStreamRecord error", e);
    }
  }

  private static ReceivedHRecord toReceivedHRecord(ReceivedRecord receivedRecord) {
    logger.info("enter toHRecord");
    try {
      HStreamRecord hStreamRecord = HStreamRecord.parseFrom(receivedRecord.getRecord());
      logger.info("parse done");
      HRecord hRecord = RecordUtils.parseHRecordFromHStreamRecord(hStreamRecord);
      logger.info("get hrecord done");
      ReceivedHRecord receivedHRecord = new ReceivedHRecord(receivedRecord.getRecordId(), hRecord);
      logger.info("get recv done");
      return receivedHRecord;
    } catch (InvalidProtocolBufferException e) {
      throw new HStreamDBClientException.InvalidRecordException("parse HStreamRecord error", e);
    }
  }
}
