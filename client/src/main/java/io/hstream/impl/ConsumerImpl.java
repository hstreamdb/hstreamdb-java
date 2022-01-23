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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerImpl extends AbstractService implements Consumer {
  private static final Logger logger = LoggerFactory.getLogger(ConsumerImpl.class);

  private final List<String> serverUrls;
  private final ChannelProvider channelProvider;

  private final String consumerName;
  private final String subscriptionId;
  private final RawRecordReceiver rawRecordReceiver;
  private final HRecordReceiver hRecordReceiver;

  private ExecutorService executorService;

  private final ConcurrentHashMap<String, StreamObserver<StreamingFetchRequest>> requestStreams;

  private final AtomicBoolean isInitialized = new AtomicBoolean(false);

  public ConsumerImpl(
      List<String> serverUrls,
      ChannelProvider channelProvider,
      String consumerName,
      String subscriptionId,
      RawRecordReceiver rawRecordReceiver,
      HRecordReceiver hRecordReceiver) {

    this.serverUrls = serverUrls;
    this.channelProvider = channelProvider;

    if (consumerName == null) {
      this.consumerName = UUID.randomUUID().toString();
    } else {
      this.consumerName = consumerName;
    }
    this.subscriptionId = subscriptionId;
    this.rawRecordReceiver = rawRecordReceiver;
    this.hRecordReceiver = hRecordReceiver;

    this.executorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("receiver-running-pool-%d").build());

    this.requestStreams = new ConcurrentHashMap<>();
  }

  @Override
  public void doStart() {

    logger.info("prepare to start consumer");

    StreamObserver<WatchSubscriptionResponse> observer =
        new StreamObserver<WatchSubscriptionResponse>() {

          @Override
          public void onNext(WatchSubscriptionResponse value) {
            if (value.getChangeCase().getNumber()
                == WatchSubscriptionResponse.ChangeCase.CHANGEADD.getNumber()) {
              var partitionKey = value.getChangeAdd().getOrderingKey();
              logger.info("watch recv changeAdd for key {}", partitionKey);
              // call lookup, then streamingFetch
              HStreamApiGrpc.newStub(channelProvider.get(serverUrls.get(0)))
                  .lookupSubscriptionWithOrderingKey(
                      LookupSubscriptionWithOrderingKeyRequest.newBuilder()
                          .setSubscriptionId(subscriptionId)
                          .setOrderingKey(partitionKey)
                          .build(),
                      new StreamObserver<LookupSubscriptionWithOrderingKeyResponse>() {
                        @Override
                        public void onNext(LookupSubscriptionWithOrderingKeyResponse value) {
                          ServerNode serverNode = value.getServerNode();
                          String serverUrl = serverNode.getHost() + ":" + serverNode.getPort();
                          StreamObserver<StreamingFetchRequest> requestStreamObserver =
                              HStreamApiGrpc.newStub(channelProvider.get(serverUrl))
                                  .streamingFetch(
                                      new StreamObserver<StreamingFetchResponse>() {
                                        @Override
                                        public void onNext(StreamingFetchResponse value) {

                                          if (!isRunning()) {
                                            return;
                                          }

                                          List<ReceivedRecord> receivedRecords =
                                              value.getReceivedRecordsList();
                                          for (ReceivedRecord receivedRecord : receivedRecords) {
                                            Responder responder =
                                                new ResponderImpl(
                                                    subscriptionId,
                                                    partitionKey,
                                                    requestStreams.get(partitionKey),
                                                    consumerName,
                                                    receivedRecord.getRecordId());

                                            executorService.submit(
                                                () -> {
                                                  if (!isRunning()) {
                                                    return;
                                                  }

                                                  if (RecordUtils.isRawRecord(receivedRecord)) {
                                                    logger.info("ready to process rawRecord");
                                                    try {
                                                      ConsumerImpl.this.rawRecordReceiver
                                                          .processRawRecord(
                                                              toReceivedRawRecord(receivedRecord),
                                                              responder);
                                                    } catch (Exception e) {
                                                      logger.error("process rawRecord error", e);
                                                    }
                                                  } else {
                                                    logger.info("ready to process hrecord");
                                                    try {
                                                      ConsumerImpl.this.hRecordReceiver
                                                          .processHRecord(
                                                              toReceivedHRecord(receivedRecord),
                                                              responder);

                                                    } catch (Exception e) {
                                                      logger.error("process hrecord error", e);
                                                    }
                                                  }
                                                });
                                          }
                                        }

                                        @Override
                                        public void onError(Throwable t) {
                                          logger.error(
                                              "consumer {} receive records from subscription {}"
                                                  + " error: {}",
                                              ConsumerImpl.this.consumerName,
                                              ConsumerImpl.this.subscriptionId,
                                              t);
                                        }

                                        @Override
                                        public void onCompleted() {
                                          logger.info(
                                              "consumer {} receive records from subscription {}"
                                                  + " stopped",
                                              ConsumerImpl.this.consumerName,
                                              ConsumerImpl.this.subscriptionId);
                                        }
                                      });
                          requestStreams.put(partitionKey, requestStreamObserver);
                          StreamingFetchRequest initRequest =
                              StreamingFetchRequest.newBuilder()
                                  .setSubscriptionId(subscriptionId)
                                  .setOrderingKey(partitionKey)
                                  .setConsumerName(consumerName)
                                  .build();
                          requestStreamObserver.onNext(initRequest);
                        }

                        @Override
                        public void onError(Throwable t) {
                          logger.error("lookupSubscription got error", t);
                        }

                        @Override
                        public void onCompleted() {}
                      });
            } else {
              logger.warn("unsupported Change");
            }
          }

          @Override
          public void onError(Throwable t) {
            logger.error(
                "consumer {} can not watch subscription {}, error: {}",
                consumerName,
                subscriptionId,
                t.getMessage(),
                t);
            // notifyFailed(t);
          }

          @Override
          public void onCompleted() {
            logger.info(
                "consumer {} watch subscription {} successfully", consumerName, subscriptionId);
          }
        };

    HStreamApiGrpc.newStub(channelProvider.get(serverUrls.get(0)))
        .watchSubscription(
            WatchSubscriptionRequest.newBuilder()
                .setSubscriptionId(subscriptionId)
                .setConsumerName(consumerName)
                .build(),
            observer);
    logger.info("consumer {} started", consumerName);
    notifyStarted();
  }

  @Override
  public void doStop() {
    logger.info("prepare to stop consumer");

    new Thread(
            () -> {
              // close the bidistreaming rpc
              requestStreams.forEach(
                  (partition, requestStream) -> {
                    requestStream.onCompleted();
                  });

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
