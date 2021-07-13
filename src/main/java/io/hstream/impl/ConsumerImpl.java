package io.hstream.impl;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.stub.StreamObserver;
import io.hstream.*;
import io.hstream.util.RecordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ConsumerImpl implements Consumer {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerImpl.class);

    private HStreamApiGrpc.HStreamApiStub grpcStub;
    private String subscriptionId;
    private String streamName;
    private long pollTimeoutMs;
    private int maxPollRecords;

    public ConsumerImpl(HStreamApiGrpc.HStreamApiStub grpcStub, String subscriptionId, String streamName, long pollTimeoutMs, int maxPollRecords) {
        this.grpcStub = grpcStub;
        this.subscriptionId = subscriptionId;
        this.streamName = streamName;
        this.pollTimeoutMs = pollTimeoutMs;
        this.maxPollRecords = maxPollRecords;

        CompletableFuture<Subscription> completableFuture = new CompletableFuture<>();
        StreamObserver<Subscription> streamObserver = new StreamObserver<>() {
            @Override
            public void onNext(Subscription subscription) {
                logger.info("create subscription {} successfully", subscription.getSubscriptionId());
                completableFuture.complete(subscription);
            }

            @Override
            public void onError(Throwable t) {
                logger.error("create subscription error: ", t);
                completableFuture.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
            }
        };

        Subscription subscription = Subscription.newBuilder()
                .setSubscriptionId(subscriptionId)
                .setStreamName(streamName)
                .setOffset(SubscriptionOffset.newBuilder().setSpecialOffset(SubscriptionOffset.SpecialOffset.LATEST))
                .build();

        grpcStub.subscribe(subscription, streamObserver);

        Subscription subscription1 = completableFuture.join();
        logger.info("consumer with subscription {} created", subscription1.getSubscriptionId());
    }

    @Override
    public List<ReceivedHRecord> pollHRecords() {
        FetchRequest fetchRequest = FetchRequest.newBuilder()
                .setSubscriptionId(subscriptionId)
                .setTimeout(pollTimeoutMs)
                .setMaxSize(maxPollRecords)
                .build();

        CompletableFuture<FetchResponse> completableFuture = new CompletableFuture<>();
        StreamObserver<FetchResponse> streamObserver = new StreamObserver<>() {
            @Override
            public void onNext(FetchResponse fetchResponse) {
                completableFuture.complete(fetchResponse);
            }

            @Override
            public void onError(Throwable t) {
                completableFuture.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
            }
        };
        grpcStub.fetch(fetchRequest, streamObserver);

        FetchResponse fetchResponse = completableFuture.join();
        return fetchResponse.getReceivedRecordsList().stream().map(ConsumerImpl::toReceivedHRecord).collect(Collectors.toList());
    }

    @Override
    public List<ReceivedRawRecord> pollRawRecords() {
        FetchRequest fetchRequest = FetchRequest.newBuilder()
                .setSubscriptionId(subscriptionId)
                .setTimeout(pollTimeoutMs)
                .setMaxSize(maxPollRecords)
                .build();

        CompletableFuture<FetchResponse> completableFuture = new CompletableFuture<>();
        StreamObserver<FetchResponse> streamObserver = new StreamObserver<>() {
            @Override
            public void onNext(FetchResponse fetchResponse) {
                completableFuture.complete(fetchResponse);
            }

            @Override
            public void onError(Throwable t) {
                completableFuture.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
            }
        };
        grpcStub.fetch(fetchRequest, streamObserver);

        FetchResponse fetchResponse = completableFuture.join();
        return fetchResponse.getReceivedRecordsList().stream().map(ConsumerImpl::toReceivedRawRecord).collect(Collectors.toList());
    }

    @Override
    public void commit(RecordId recordId) {
        CommittedOffset committedOffset = CommittedOffset.newBuilder()
                .setSubscriptionId(subscriptionId)
                .setStreamName(streamName)
                .setOffset(recordId)
                .build();

        CompletableFuture<CommittedOffset> completableFuture = new CompletableFuture<>();
        StreamObserver<CommittedOffset> streamObserver = new StreamObserver<>() {
            @Override
            public void onNext(CommittedOffset offset) {
                completableFuture.complete(offset);
            }

            @Override
            public void onError(Throwable t) {
                completableFuture.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
            }
        };

        grpcStub.commitOffset(committedOffset, streamObserver);
        completableFuture.join();
    }

    private static ReceivedRawRecord toReceivedRawRecord(ReceivedRecord receivedRecord) {
        try{
            HStreamRecord hStreamRecord = HStreamRecord.parseFrom(receivedRecord.getRecord());
            byte[] rawRecord = RecordUtils.parseRawRecordFromHStreamRecord(hStreamRecord);
            ReceivedRawRecord receivedRawRecord = new ReceivedRawRecord(receivedRecord.getRecordId(), rawRecord);
            return receivedRawRecord;
        } catch (InvalidProtocolBufferException e) {
            throw new HStreamDBClientException.InvalidRecordException("parse HStreamRecord error", e);
        }
    }

    private static ReceivedHRecord toReceivedHRecord(ReceivedRecord receivedRecord) {
        try{
            HStreamRecord hStreamRecord = HStreamRecord.parseFrom(receivedRecord.getRecord());
            HRecord hRecord = RecordUtils.parseHRecordFromHStreamRecord(hStreamRecord);
            ReceivedHRecord receivedHRecord = new ReceivedHRecord(receivedRecord.getRecordId(), hRecord);
            return receivedHRecord;
        } catch (InvalidProtocolBufferException e) {
            throw new HStreamDBClientException.InvalidRecordException("parse HStreamRecord error", e);
        }
    }
}
