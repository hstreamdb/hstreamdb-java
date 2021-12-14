package io.hstream.impl;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.hstream.Consumer;
import io.hstream.HRecord;
import io.hstream.HRecordReceiver;
import io.hstream.HStreamDBClientException;
import io.hstream.RawRecordReceiver;
import io.hstream.ReceivedHRecord;
import io.hstream.ReceivedRawRecord;
import io.hstream.Responder;
import io.hstream.internal.HStreamApiGrpc;
import io.hstream.internal.HStreamApiGrpc.HStreamApiStub;
import io.hstream.internal.HStreamRecord;
import io.hstream.internal.LookupSubscriptionRequest;
import io.hstream.internal.ReceivedRecord;
import io.hstream.internal.ServerNode;
import io.hstream.internal.StreamingFetchRequest;
import io.hstream.internal.StreamingFetchResponse;
import io.hstream.util.GrpcUtils;
import io.hstream.util.RecordUtils;
import java.util.List;
import java.util.UUID;
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
  private final StreamObserver<StreamingFetchResponse> responseStream;
  private final StreamObserver<StreamingFetchRequest> requestStream;
  private final AtomicBoolean isInitialized = new AtomicBoolean(false);
  private ExecutorService executorService;

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

    responseStream =
        new StreamObserver<StreamingFetchResponse>() {
          @Override
          public void onNext(StreamingFetchResponse value) {
            if (isInitialized.compareAndSet(false, true)) {
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
            if (isInitialized.compareAndSet(false, true)) {
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

    boolean retryStatus = false;
    StreamObserver<StreamingFetchRequest> ret = null;
    for (int retryAcc = 0; retryAcc < serverUrls.size() && !retryStatus; retryAcc++) {
      logger.info("begin streamingFetch");
      try {
        ServerNode serverNode =
            HStreamApiGrpc.newBlockingStub(
                    ManagedChannelBuilder.forTarget(serverUrls.get(retryAcc))
                        .usePlaintext()
                        .build())
                .lookupSubscription(
                    LookupSubscriptionRequest.newBuilder()
                        .setSubscriptionId(subscriptionId)
                        .build())
                .getServerNode();
        String serverUrl = serverNode.getHost() + ":" + serverNode.getPort();
        HStreamApiStub fetchStub = HStreamApiGrpc.newStub(channelProvider.get(serverUrl));
        ret = fetchStub.streamingFetch(responseStream);
        retryStatus = true;
      } catch (Exception e) {
        logger.warn("retry because of " + e + ", " + "serverUrls = " + serverUrls.get(retryAcc));
        if (!(retryAcc + 1 < serverUrls.size())) {
          logger.error("retry failed, " + "retryAcc = " + retryAcc, e);
          throw e;
        }
      }
    }
    logger.info("end streamingFetch");
    this.requestStream = ret;
  }

  private static ReceivedRawRecord toReceivedRawRecord(ReceivedRecord receivedRecord) {
    try {
      HStreamRecord hStreamRecord = HStreamRecord.parseFrom(receivedRecord.getRecord());
      byte[] rawRecord = RecordUtils.parseRawRecordFromHStreamRecord(hStreamRecord);
      return new ReceivedRawRecord(
          GrpcUtils.recordIdFromGrpc(receivedRecord.getRecordId()), rawRecord);
    } catch (InvalidProtocolBufferException e) {
      throw new HStreamDBClientException.InvalidRecordException("parse HStreamRecord error", e);
    }
  }

  private static ReceivedHRecord toReceivedHRecord(ReceivedRecord receivedRecord) {
    try {
      HStreamRecord hStreamRecord = HStreamRecord.parseFrom(receivedRecord.getRecord());
      HRecord hRecord = RecordUtils.parseHRecordFromHStreamRecord(hStreamRecord);
      return new ReceivedHRecord(GrpcUtils.recordIdFromGrpc(receivedRecord.getRecordId()), hRecord);
    } catch (InvalidProtocolBufferException e) {
      throw new HStreamDBClientException.InvalidRecordException("parse HStreamRecord error", e);
    }
  }

  @Override
  public void doStart() {

    logger.info("prepare to start consumer");

    StreamingFetchRequest initRequest =
        StreamingFetchRequest.newBuilder()
            .setSubscriptionId(subscriptionId)
            .setConsumerName(consumerName)
            .build();
    requestStream.onNext(initRequest);

    logger.info("consumer {} started", consumerName);
    notifyStarted();
  }

  @Override
  public void doStop() {
    logger.info("prepare to stop consumer");

    new Thread(
            () -> {
              // close the bidistreaming rpc
              requestStream.onCompleted();

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
}
