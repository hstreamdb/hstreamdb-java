package io.hstream.impl;

import com.google.protobuf.Empty;
import com.google.protobuf.Struct;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.hstream.*;
import io.hstream.HStreamApiGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ClientImpl implements HStreamClient {

    private static final Logger logger = LoggerFactory.getLogger(ClientImpl.class);

    private final ManagedChannel managedChannel;
    private final HStreamApiGrpc.HStreamApiStub stub;
    private final HStreamApiGrpc.HStreamApiBlockingStub blockingStub;

    public ClientImpl(String serviceUrl) {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(serviceUrl)
                .usePlaintext()
                .build();
        this.managedChannel = channel;
        this.stub = HStreamApiGrpc.newStub(channel);
        this.blockingStub = HStreamApiGrpc.newBlockingStub(channel);
    }

    @Override
    public ProducerBuilder newProducer() {
       return new ProducerBuilder(stub);
    }

    @Override
    public ConsumerBuilder newConsumer() {
        return new ConsumerBuilder(stub);
    }

    @Override
    public Publisher<HRecord> streamQuery(String sql) {
        CommandPushQuery pushQuery = CommandPushQuery.newBuilder()
                .setQueryText(sql).build();

        Publisher<HRecord> responsePublisher = new Publisher<HRecord>() {
            @Override
            public void subscribe(Observer<? super HRecord> observer) {
                StreamObserver<Struct> streamObserver = new StreamObserver<>() {
                    @Override
                    public void onNext(Struct value) {
                        observer.onNext(new HRecord(value.getFieldsOrThrow("SELECT").getStructValue()));
                    }

                    @Override
                    public void onError(Throwable t) {
                        observer.onError(t);
                    }

                    @Override
                    public void onCompleted() {
                        observer.onCompleted();
                    }
                };

                stub.executePushQuery(pushQuery, streamObserver);
            }
        };

        return responsePublisher;
    }

    @Override
    public void createStream(String streamName) {
        Stream stream = Stream.newBuilder()
                .setStreamName(streamName)
                .setReplicationFactor(3)
                .build();

        blockingStub.createStream(stream);
    }

    @Override
    public void deleteStream(String streamName) {
        DeleteStreamRequest deleteStreamRequest = DeleteStreamRequest.newBuilder().setStreamName(streamName).build();
        blockingStub.deleteStream(deleteStreamRequest);
        logger.info("delete stream {} done", streamName);
    }

    @Override
    public List<Stream> listStreams() {
        Empty empty = Empty.newBuilder().build();
        ListStreamsResponse listStreamsResponse = blockingStub.listStreams(empty);
        return listStreamsResponse.getStreamsList();
    }

    @Override
    public List<Subscription> listSubscriptions() {
        return blockingStub.listSubscriptions(Empty.newBuilder().build()).getSubscriptionList();
    }

    @Override
    public void deleteSubscription(String subscriptionId) {
        blockingStub.withDeadlineAfter(1000, TimeUnit.MILLISECONDS).deleteSubscription(DeleteSubscriptionRequest.newBuilder().setSubscriptionId(subscriptionId).build());
        logger.info("delete subscription {} done", subscriptionId);
    }

    @Override
    public void close() throws Exception {
        managedChannel.shutdownNow();
    }
}
