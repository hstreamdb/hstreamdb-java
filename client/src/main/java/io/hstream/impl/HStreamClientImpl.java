package io.hstream.impl;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.hstream.ConsumerBuilder;
import io.hstream.HStreamClient;
import io.hstream.ProducerBuilder;
import io.hstream.QueryerBuilder;
import io.hstream.Stream;
import io.hstream.Subscription;
import io.hstream.internal.DeleteStreamRequest;
import io.hstream.internal.DeleteSubscriptionRequest;
import io.hstream.internal.DescribeClusterResponse;
import io.hstream.internal.HStreamApiGrpc;
import io.hstream.internal.ListStreamsResponse;
import io.hstream.internal.ServerNode;
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
  private final List<String> initializedServerUrls;

  public HStreamClientImpl(List<String> bootstrapServerUrls) {
    channelProvider = new ChannelProvider();

    boolean retryStatus = false;
    List<ServerNode> serverNodes = null;
    List<String> initializedServerUrls = null;

    for (int retryAcc = 0; retryAcc < bootstrapServerUrls.size() && !retryStatus; retryAcc++) {
      logger.info("begin describeCluster");
      try {
        ManagedChannel channel = channelProvider.get(bootstrapServerUrls.get(retryAcc));
        HStreamApiGrpc.HStreamApiBlockingStub blockingStub =
            HStreamApiGrpc.newBlockingStub(channel);
        DescribeClusterResponse describeClusterResponse =
            blockingStub.describeCluster(Empty.newBuilder().build());

        serverNodes = describeClusterResponse.getServerNodesList();
        initializedServerUrls = new ArrayList<>(serverNodes.size());
        retryStatus = true;
      } catch (Exception e) {
        logger.warn(
            "retry because of "
                + e
                + ", "
                + "bootstrapServerUrl = "
                + bootstrapServerUrls.get(retryAcc)
                + " retryAcc = "
                + retryAcc);
        if (!(retryAcc + 1 < bootstrapServerUrls.size())) {
          logger.error("retry failed, " + "retryAcc = " + retryAcc, e);
          throw e;
        }
      }
    }
    logger.info("end describeCluster");
    this.initializedServerUrls = initializedServerUrls;
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
    final Stream stream = new Stream(streamName, replicationFactor);

    boolean retryStatus = false;
    for (int retryAcc = 0; retryAcc < initializedServerUrls.size() && !retryStatus; retryAcc++) {
      logger.info("begin createStream");
      try {
        HStreamApiGrpc.HStreamApiBlockingStub blockingStub =
            HStreamApiGrpc.newBlockingStub(
                channelProvider.get(initializedServerUrls.get(retryAcc)));
        blockingStub.createStream(GrpcUtils.streamToGrpc(stream));
      } catch (Exception e) {
        logger.warn(
            "retry because of "
                + e
                + ", "
                + "initializedServerUrl = "
                + initializedServerUrls.get(retryAcc)
                + " retryAcc = "
                + retryAcc);
        if (!(retryAcc + 1 < initializedServerUrls.size())) {
          logger.error("retry failed, " + "retryAcc = " + retryAcc, e);
          throw e;
        }
      }
    }
    logger.info("end createStream");
  }

  @Override
  public void deleteStream(String streamName) {
    final DeleteStreamRequest deleteStreamRequest =
        DeleteStreamRequest.newBuilder().setStreamName(streamName).build();

    boolean retryStatus = false;
    for (int retryAcc = 0; retryAcc < initializedServerUrls.size() && !retryStatus; retryAcc++) {
      logger.info("begin deleteStream");
      try {
        HStreamApiGrpc.HStreamApiBlockingStub blockingStub =
            HStreamApiGrpc.newBlockingStub(
                channelProvider.get(initializedServerUrls.get(retryAcc)));
        blockingStub.deleteStream(deleteStreamRequest);
      } catch (Exception e) {
        logger.warn(
            "retry because of "
                + e
                + ", "
                + "initializedServerUrl = "
                + initializedServerUrls.get(retryAcc)
                + " retryAcc = "
                + retryAcc);
        if (!(retryAcc + 1 < initializedServerUrls.size())) {
          logger.error("retry failed, " + "retryAcc = " + retryAcc, e);
          throw e;
        }
      }
    }
    logger.info("delete stream {} done", streamName);
  }

  @Override
  public List<Stream> listStreams() {
    Empty empty = Empty.newBuilder().build();

    ListStreamsResponse listStreamsResponse = null;

    boolean retryStatus = false;
    for (int retryAcc = 0; retryAcc < initializedServerUrls.size() && !retryStatus; retryAcc++) {
      logger.info("begin listStreams");
      try {
        HStreamApiGrpc.HStreamApiBlockingStub blockingStub =
            HStreamApiGrpc.newBlockingStub(
                channelProvider.get(initializedServerUrls.get(retryAcc)));
        listStreamsResponse = blockingStub.listStreams(empty);
      } catch (Exception e) {
        logger.warn(
            "retry because of "
                + e
                + ", "
                + "initializedServerUrl = "
                + initializedServerUrls.get(retryAcc)
                + " retryAcc = "
                + retryAcc);
        if (!(retryAcc + 1 < initializedServerUrls.size())) {
          logger.error("retry failed, " + "retryAcc = " + retryAcc, e);
          throw e;
        }
      }
    }
    logger.info("end listStreams");
    return listStreamsResponse.getStreamsList().stream()
        .map(GrpcUtils::streamFromGrpc)
        .collect(Collectors.toList());
  }

  @Override
  public void createSubscription(Subscription subscription) {
    boolean retryStatus = false;
    for (int retryAcc = 0; retryAcc < initializedServerUrls.size() && !retryStatus; retryAcc++) {
      logger.info("begin createSubscription");
      try {
        HStreamApiGrpc.HStreamApiBlockingStub blockingStub =
            HStreamApiGrpc.newBlockingStub(
                channelProvider.get(initializedServerUrls.get(retryAcc)));
        blockingStub.createSubscription(GrpcUtils.subscriptionToGrpc(subscription));
      } catch (Exception e) {
        logger.warn(
            "retry because of "
                + e
                + ", "
                + "initializedServerUrl = "
                + initializedServerUrls.get(retryAcc)
                + " retryAcc = "
                + retryAcc);
        if (!(retryAcc + 1 < initializedServerUrls.size())) {
          logger.error("retry failed, " + "retryAcc = " + retryAcc, e);
          throw e;
        }
      }
    }
    logger.info("end createSubscription");
  }

  @Override
  public List<Subscription> listSubscriptions() {
    List<Subscription> ret = null;

    boolean retryStatus = false;
    for (int retryAcc = 0; retryAcc < initializedServerUrls.size() && !retryStatus; retryAcc++) {
      logger.info("begin listSubscriptions");
      try {
        HStreamApiGrpc.HStreamApiBlockingStub blockingStub =
            HStreamApiGrpc.newBlockingStub(
                channelProvider.get(initializedServerUrls.get(retryAcc)));
        ret =
            blockingStub
                .withDeadlineAfter(100, TimeUnit.SECONDS)
                .listSubscriptions(Empty.newBuilder().build())
                .getSubscriptionList()
                .stream()
                .map(GrpcUtils::subscriptionFromGrpc)
                .collect(Collectors.toList());
      } catch (Exception e) {
        logger.warn(
            "retry because of "
                + e
                + ", "
                + "initializedServerUrl = "
                + initializedServerUrls.get(retryAcc)
                + " retryAcc = "
                + retryAcc);
        if (!(retryAcc + 1 < initializedServerUrls.size())) {
          logger.error("retry failed, " + "retryAcc = " + retryAcc, e);
          throw e;
        }
      }
    }
    logger.info("end listSubscriptions");
    return ret;
  }

  @Override
  public void deleteSubscription(String subscriptionId) {
    boolean retryStatus = false;
    for (int retryAcc = 0; retryAcc < initializedServerUrls.size() && !retryStatus; retryAcc++) {
      logger.info("begin deleteSubscription");
      try {
        HStreamApiGrpc.HStreamApiBlockingStub blockingStub =
            HStreamApiGrpc.newBlockingStub(
                channelProvider.get(initializedServerUrls.get(retryAcc)));
        blockingStub
            .withDeadlineAfter(100, TimeUnit.SECONDS)
            .deleteSubscription(
                DeleteSubscriptionRequest.newBuilder().setSubscriptionId(subscriptionId).build());
      } catch (Exception e) {
        logger.warn(
            "retry because of "
                + e
                + ", "
                + "initializedServerUrl = "
                + initializedServerUrls.get(retryAcc)
                + " retryAcc = "
                + retryAcc);
        if (!(retryAcc + 1 < initializedServerUrls.size())) {
          logger.error("retry failed, " + "retryAcc = " + retryAcc, e);
          throw e;
        }
      }
    }
    logger.info("delete subscription {} done", subscriptionId);
  }

  @Override
  public void close() {
    channelProvider.close();
  }
}
