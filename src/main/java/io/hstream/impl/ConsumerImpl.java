package io.hstream.impl;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.hstream.*;
import io.hstream.util.RecordUtils;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerImpl implements Consumer {
  private static final Logger logger = LoggerFactory.getLogger(ConsumerImpl.class);

  private HStreamApiGrpc.HStreamApiStub grpcStub;
  private HStreamApiGrpc.HStreamApiBlockingStub grpcBlockingStub;
  private String subscriptionId;
  private String streamName;
  private long pollTimeoutMs;
  private int maxPollRecords;
  private ScheduledExecutorService scheduledExecutorService;

  public ConsumerImpl(
      HStreamApiGrpc.HStreamApiStub grpcStub,
      HStreamApiGrpc.HStreamApiBlockingStub grpcBlockingStub,
      String subscriptionId,
      String streamName,
      long pollTimeoutMs,
      int maxPollRecords) {
    this.grpcStub = grpcStub;
    this.grpcBlockingStub = grpcBlockingStub;
    this.subscriptionId = subscriptionId;
    this.streamName = streamName;
    this.pollTimeoutMs = pollTimeoutMs;
    this.maxPollRecords = maxPollRecords;
    this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1);

    Subscription subscription =
        Subscription.newBuilder()
            .setSubscriptionId(subscriptionId)
            .setStreamName(streamName)
            .setOffset(
                SubscriptionOffset.newBuilder()
                    .setSpecialOffset(SubscriptionOffset.SpecialOffset.LATEST))
            .build();
    try {
      Subscription subscriptionResponse = grpcBlockingStub.subscribe(subscription);
      logger.info(
          "consumer with subscription {} created", subscriptionResponse.getSubscriptionId());
    } catch (StatusRuntimeException e) {
      throw new HStreamDBClientException.SubscribeException("consumer subscribe error", e);
    }

    final ConsumerHeartbeatRequest consumerHeartbeatRequest =
        ConsumerHeartbeatRequest.newBuilder().setSubscriptionId(this.subscriptionId).build();
    final StreamObserver<ConsumerHeartbeatResponse> heartbeatObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(ConsumerHeartbeatResponse response) {
            logger.info(
                "received consumer heartbeat response for subscription {}",
                response.getSubscriptionId());
          }

          @Override
          public void onError(Throwable t) {
            logger.error("send consumer heartbeat error: ", t);
          }

          @Override
          public void onCompleted() {}
        };

    scheduledExecutorService.scheduleAtFixedRate(
        () -> grpcStub.sendConsumerHeartbeat(consumerHeartbeatRequest, heartbeatObserver),
        0,
        1,
        TimeUnit.SECONDS);
  }

  @Override
  public List<ReceivedHRecord> pollHRecords() {
    FetchRequest fetchRequest =
        FetchRequest.newBuilder()
            .setSubscriptionId(subscriptionId)
            .setTimeout(pollTimeoutMs)
            .setMaxSize(maxPollRecords)
            .build();

    CompletableFuture<FetchResponse> completableFuture = new CompletableFuture<>();
    StreamObserver<FetchResponse> streamObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(FetchResponse fetchResponse) {
            completableFuture.complete(fetchResponse);
          }

          @Override
          public void onError(Throwable t) {
            completableFuture.completeExceptionally(t);
          }

          @Override
          public void onCompleted() {}
        };
    grpcStub.fetch(fetchRequest, streamObserver);

    FetchResponse fetchResponse = completableFuture.join();
    return fetchResponse.getReceivedRecordsList().stream()
        .map(ConsumerImpl::toReceivedHRecord)
        .collect(Collectors.toList());
  }

  @Override
  public List<ReceivedRawRecord> pollRawRecords() {
    FetchRequest fetchRequest =
        FetchRequest.newBuilder()
            .setSubscriptionId(subscriptionId)
            .setTimeout(pollTimeoutMs)
            .setMaxSize(maxPollRecords)
            .build();

    CompletableFuture<FetchResponse> completableFuture = new CompletableFuture<>();
    StreamObserver<FetchResponse> streamObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(FetchResponse fetchResponse) {
            completableFuture.complete(fetchResponse);
          }

          @Override
          public void onError(Throwable t) {
            completableFuture.completeExceptionally(t);
          }

          @Override
          public void onCompleted() {}
        };
    grpcStub.fetch(fetchRequest, streamObserver);

    FetchResponse fetchResponse = completableFuture.join();
    return fetchResponse.getReceivedRecordsList().stream()
        .map(ConsumerImpl::toReceivedRawRecord)
        .collect(Collectors.toList());
  }

  @Override
  public void commit(RecordId recordId) {
    CommittedOffset committedOffset =
        CommittedOffset.newBuilder()
            .setSubscriptionId(subscriptionId)
            .setStreamName(streamName)
            .setOffset(recordId)
            .build();

    CompletableFuture<CommittedOffset> completableFuture = new CompletableFuture<>();
    StreamObserver<CommittedOffset> streamObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(CommittedOffset offset) {
            completableFuture.complete(offset);
          }

          @Override
          public void onError(Throwable t) {
            completableFuture.completeExceptionally(t);
          }

          @Override
          public void onCompleted() {}
        };

    grpcStub.commitOffset(committedOffset, streamObserver);
    completableFuture.join();
  }

  @Override
  public void close() throws Exception {
    logger.info("prepare to close consumer");

    scheduledExecutorService.shutdownNow();

    logger.info("consumer has been closed");
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
    try {
      HStreamRecord hStreamRecord = HStreamRecord.parseFrom(receivedRecord.getRecord());
      HRecord hRecord = RecordUtils.parseHRecordFromHStreamRecord(hStreamRecord);
      ReceivedHRecord receivedHRecord = new ReceivedHRecord(receivedRecord.getRecordId(), hRecord);
      return receivedHRecord;
    } catch (InvalidProtocolBufferException e) {
      throw new HStreamDBClientException.InvalidRecordException("parse HStreamRecord error", e);
    }
  }
}
