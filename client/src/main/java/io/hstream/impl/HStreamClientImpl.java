package io.hstream.impl;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.hstream.*;
import io.hstream.internal.DeleteStreamRequest;
import io.hstream.internal.DeleteSubscriptionRequest;
import io.hstream.internal.HStreamApiGrpc;
import io.hstream.internal.ListStreamsResponse;
import io.hstream.util.GrpcUtils;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HStreamClientImpl implements HStreamClient {

  private static final Logger logger = LoggerFactory.getLogger(HStreamClientImpl.class);

  private final ManagedChannel managedChannel;
  private final HStreamApiGrpc.HStreamApiStub stub;
  private final HStreamApiGrpc.HStreamApiBlockingStub blockingStub;

  private static final short DEFAULT_STREAM_REPLICATOR = 3;

  public HStreamClientImpl(String serviceUrl) {
    ManagedChannel channel = ManagedChannelBuilder.forTarget(serviceUrl).usePlaintext().build();
    this.managedChannel = channel;
    this.stub = HStreamApiGrpc.newStub(channel);
    this.blockingStub = HStreamApiGrpc.newBlockingStub(channel);
  }

  @Override
  public ProducerBuilder newProducer() {
    return new ProducerBuilderImpl(stub);
  }

  @Override
  public ConsumerBuilder newConsumer() {
    return new ConsumerBuilderImpl(stub, blockingStub);
  }

  @Override
  public QueryerBuilder newQueryer() {
    return new QueryerBuilderImpl(this, stub);
  }

  @Override
  public void createStream(String streamName) {
    createStream(streamName, DEFAULT_STREAM_REPLICATOR);
  }

  @Override
  public void createStream(String streamName, short replicationFactor) {
    Stream stream = new Stream(streamName, replicationFactor);
    blockingStub.createStream(GrpcUtils.streamToGrpc(stream));
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
    return listStreamsResponse.getStreamsList().stream()
        .map(GrpcUtils::streamFromGrpc)
        .collect(Collectors.toList());
  }

  @Override
  public void createSubscription(Subscription subscription) {
    blockingStub.createSubscription(GrpcUtils.subscriptionToGrpc(subscription));
  }

  @Override
  public List<Subscription> listSubscriptions() {
    return blockingStub
        .withDeadlineAfter(100, TimeUnit.SECONDS)
        .listSubscriptions(Empty.newBuilder().build())
        .getSubscriptionList()
        .stream()
        .map(GrpcUtils::subscriptionFromGrpc)
        .collect(Collectors.toList());
  }

  @Override
  public void deleteSubscription(String subscriptionId) {
    blockingStub
        .withDeadlineAfter(100, TimeUnit.SECONDS)
        .deleteSubscription(
            DeleteSubscriptionRequest.newBuilder().setSubscriptionId(subscriptionId).build());
    logger.info("delete subscription {} done", subscriptionId);
  }

  @Override
  public void close() throws Exception {
    managedChannel.shutdownNow();
  }
}
