package io.hstream.impl;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.hstream.*;
import io.hstream.internal.DeleteStreamRequest;
import io.hstream.internal.DeleteSubscriptionRequest;
import io.hstream.internal.HStreamApiGrpc;
import io.hstream.internal.ListStreamsResponse;
import io.hstream.internal.Stream;
import io.hstream.util.GrpcUtils;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientImpl implements HStreamClient {

  private static final Logger logger = LoggerFactory.getLogger(ClientImpl.class);

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
  public QueryerBuilder newQueryer() {
    return new QueryerBuilder(this, stub);
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
    blockingStub.createSubscription(GrpcUtils.subscriptionToGrpc(subscription));
  }

  @Override
  public List<Subscription> listSubscriptions() {
    return blockingStub.listSubscriptions(Empty.newBuilder().build()).getSubscriptionList().stream()
        .map(GrpcUtils::subscriptionFromGrpc)
        .collect(Collectors.toList());
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
