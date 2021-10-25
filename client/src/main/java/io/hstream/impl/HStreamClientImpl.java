package io.hstream.impl;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.hstream.*;
import io.hstream.Stream;
import io.hstream.Subscription;
import io.hstream.internal.*;
import io.hstream.util.GrpcUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HStreamClientImpl implements HStreamClient {

  private static final Logger logger = LoggerFactory.getLogger(HStreamClientImpl.class);

  private static final short DEFAULT_STREAM_REPLICATOR = 3;

  private final ChannelProvider channelProvider;
  private final List<String> bootstrapServerUrls;
  private final List<String> initializedServerUrls;

  private HStreamApiGrpc.HStreamApiBlockingStub blockingStub;

  public HStreamClientImpl(List<String> bootstrapServerUrls) {
    this.bootstrapServerUrls = bootstrapServerUrls;
    channelProvider = new ChannelProvider();

    ManagedChannel channel = channelProvider.get(bootstrapServerUrls.get(0));
    blockingStub = HStreamApiGrpc.newBlockingStub(channel);
    DescribeClusterResponse describeClusterResponse =
        blockingStub.describeCluster(Empty.newBuilder().build());
    List<ServerNode> serverNodes = describeClusterResponse.getServerNodesList();
    initializedServerUrls = new ArrayList<>(serverNodes.size());
    for (ServerNode serverNode : serverNodes) {
      String host = serverNode.getHost();
      int port = serverNode.getPort();
      initializedServerUrls.add(host + ":" + port);
    }
  }

  @Override
  public ProducerBuilder newProducer() {
    return new ProducerBuilderImpl(initializedServerUrls, channelProvider);
  }

  @Override
  public ConsumerBuilder newConsumer() {
    return new ConsumerBuilderImpl(initializedServerUrls, channelProvider);
  }

  @Override
  public QueryerBuilder newQueryer() {
    return new QueryerBuilderImpl(this, initializedServerUrls, channelProvider);
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
  public void close() {
    channelProvider.close();
  }
}
