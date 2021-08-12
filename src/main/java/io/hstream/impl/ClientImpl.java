package io.hstream.impl;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.hstream.*;
import io.hstream.HStreamApiGrpc;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientImpl implements HStreamClient {

  private static final Logger logger = LoggerFactory.getLogger(ClientImpl.class);

  private static final String STREAM_QUERY_STREAM_PREFIX = "STREAM-QUERY-";

  private static final String STREAM_QUERY_SUBSCRIPTION_PREFIX = "STREAM-QUERY-";

  private final ManagedChannel managedChannel;
  private final HStreamApiGrpc.HStreamApiStub stub;
  private final HStreamApiGrpc.HStreamApiBlockingStub blockingStub;

  public ClientImpl(String serviceUrl) {
    ManagedChannel channel = ManagedChannelBuilder.forTarget(serviceUrl).usePlaintext().build();
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
    return new ConsumerBuilder(stub, blockingStub);
  }

  @Override
  public CompletableFuture<Publisher<HRecord>> streamQuery(String sql) {
    CompletableFuture<Publisher<HRecord>> publisherFuture = new CompletableFuture<>();

    String resultStreamNameSuffix = UUID.randomUUID().toString();
    Publisher<HRecord> responsePublisher =
        new Publisher<HRecord>() {
          @Override
          public void subscribe(Observer<? super HRecord> observer) {
            createSubscription(
                Subscription.newBuilder()
                    .setSubscriptionId(STREAM_QUERY_SUBSCRIPTION_PREFIX + resultStreamNameSuffix)
                    .setStreamName(STREAM_QUERY_STREAM_PREFIX + resultStreamNameSuffix)
                    .setOffset(
                        SubscriptionOffset.newBuilder()
                            .setSpecialOffset(SubscriptionOffset.SpecialOffset.EARLIST)
                            .build())
                    .build());

            Consumer consumer =
                newConsumer()
                    .subscription(STREAM_QUERY_SUBSCRIPTION_PREFIX + resultStreamNameSuffix)
                    .hRecordReceiver(
                        (receivedHRecord, responder) -> {
                          try {
                            observer.onNext(receivedHRecord.getHRecord());
                            responder.ack();
                          } catch (Throwable t) {
                            observer.onError(t);
                          }
                        })
                    .build();
            consumer.startAsync().awaitRunning();
          }
        };

    CreateQueryStreamRequest createQueryStreamRequest =
        CreateQueryStreamRequest.newBuilder()
            .setQueryStream(
                Stream.newBuilder()
                    .setStreamName(STREAM_QUERY_STREAM_PREFIX + resultStreamNameSuffix)
                    .setReplicationFactor(3)
                    .build())
            .setQueryStatements(sql)
            .build();
    stub.createQueryStream(
        createQueryStreamRequest,
        new StreamObserver<CreateQueryStreamResponse>() {
          @Override
          public void onNext(CreateQueryStreamResponse value) {
            logger.info(
                "query [{}] created, related result stream is [{}]",
                value.getStreamQuery().getId(),
                value.getQueryStream().getStreamName());

            publisherFuture.complete(responsePublisher);
          }

          @Override
          public void onError(Throwable t) {
            logger.error("creating stream query happens error: ", t);
            publisherFuture.completeExceptionally(t);
          }

          @Override
          public void onCompleted() {}
        });

    return publisherFuture;
  }

  @Override
  public void createStream(String streamName) {
    Stream stream = Stream.newBuilder().setStreamName(streamName).setReplicationFactor(3).build();

    blockingStub.createStream(stream);
  }

  @Override
  public void deleteStream(String streamName) {
    DeleteStreamRequest deleteStreamRequest =
        DeleteStreamRequest.newBuilder().setStreamName(streamName).build();
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
  public void createSubscription(Subscription subscription) {
    blockingStub.createSubscription(subscription);
  }

  @Override
  public List<Subscription> listSubscriptions() {
    return blockingStub.listSubscriptions(Empty.newBuilder().build()).getSubscriptionList();
  }

  @Override
  public void deleteSubscription(String subscriptionId) {
    blockingStub
        .withDeadlineAfter(1000, TimeUnit.MILLISECONDS)
        .deleteSubscription(
            DeleteSubscriptionRequest.newBuilder().setSubscriptionId(subscriptionId).build());
    logger.info("delete subscription {} done", subscriptionId);
  }

  @Override
  public void close() throws Exception {
    managedChannel.shutdownNow();
  }
}
