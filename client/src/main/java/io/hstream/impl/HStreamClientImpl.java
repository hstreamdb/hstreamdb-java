package io.hstream.impl;

import com.google.protobuf.Empty;
import io.hstream.ConsumerBuilder;
import io.hstream.HStreamClient;
import io.hstream.ProducerBuilder;
import io.hstream.QueryerBuilder;
import io.hstream.Stream;
import io.hstream.Subscription;
import io.hstream.internal.DeleteStreamRequest;
import io.hstream.internal.DeleteSubscriptionRequest;
import io.hstream.internal.DescribeClusterResponse;
import io.hstream.internal.ListStreamsResponse;
import io.hstream.internal.ServerNode;
import io.hstream.util.GrpcUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HStreamClientImpl implements HStreamClient {

  private static final Logger logger = LoggerFactory.getLogger(HStreamClientImpl.class);

  private static final short DEFAULT_STREAM_REPLICATOR = 3;

  private final ChannelProvider channelProvider;
  private final List<String> initializedServerUrls;

  public HStreamClientImpl(List<String> bootstrapServerUrls) {
    channelProvider = new ChannelProvider();

    AtomicReference<List<ServerNode>> serverNodes = new AtomicReference<>();
    List<String> initializedServerUrls;

    new Retry(bootstrapServerUrls, logger)
        .withRetriesBlocking(
            stub -> {
              DescribeClusterResponse describeClusterResponse =
                  stub.describeCluster(Empty.newBuilder().build());
              serverNodes.set(describeClusterResponse.getServerNodesList());
            });

    initializedServerUrls = new ArrayList<>(serverNodes.get().size());
    this.initializedServerUrls = initializedServerUrls;
    for (ServerNode serverNode : serverNodes.get()) {
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
    final Stream stream = new Stream(streamName, replicationFactor);

    new Retry(initializedServerUrls, logger)
        .withRetriesBlocking(
            (stub) -> {
              stub.createStream(GrpcUtils.streamToGrpc(stream));
            });
  }

  @Override
  public void deleteStream(String streamName) {
    final DeleteStreamRequest deleteStreamRequest =
        DeleteStreamRequest.newBuilder().setStreamName(streamName).build();

    new Retry(initializedServerUrls, logger)
        .withRetriesBlocking(
            stub -> {
              stub.deleteStream(deleteStreamRequest);
            });

    logger.info("delete stream {} done", streamName);
  }

  @Override
  public List<Stream> listStreams() {
    Empty empty = Empty.newBuilder().build();
    AtomicReference<ListStreamsResponse> listStreamsResponse = new AtomicReference<>();

    new Retry(initializedServerUrls, logger)
        .withRetriesBlocking(
            stub -> {
              listStreamsResponse.set(stub.listStreams(empty));
            });

    return listStreamsResponse.get().getStreamsList().stream()
        .map(GrpcUtils::streamFromGrpc)
        .collect(Collectors.toList());
  }

  @Override
  public void createSubscription(Subscription subscription) {
    new Retry(initializedServerUrls, logger)
        .withRetriesBlocking(
            stub -> {
              stub.createSubscription(GrpcUtils.subscriptionToGrpc(subscription));
            });
  }

  @Override
  public List<Subscription> listSubscriptions() {
    AtomicReference<List<Subscription>> listSubscriptionsResponse = new AtomicReference<>();

    new Retry(initializedServerUrls, logger)
        .withRetriesBlocking(
            stub -> {
              listSubscriptionsResponse.set(
                  stub
                      .withDeadlineAfter(100, TimeUnit.SECONDS)
                      .listSubscriptions(Empty.newBuilder().build())
                      .getSubscriptionList()
                      .stream()
                      .map(GrpcUtils::subscriptionFromGrpc)
                      .collect(Collectors.toList()));
            });

    return listSubscriptionsResponse.get();
  }

  @Override
  public void deleteSubscription(String subscriptionId) {
    new Retry(initializedServerUrls, logger)
        .withRetriesBlocking(
            stub -> {
              stub.withDeadlineAfter(100, TimeUnit.SECONDS)
                  .deleteSubscription(
                      DeleteSubscriptionRequest.newBuilder()
                          .setSubscriptionId(subscriptionId)
                          .build());
            });
  }

  @Override
  public void close() {
    channelProvider.close();
  }
}
