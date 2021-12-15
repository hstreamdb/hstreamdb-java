package io.hstream.impl;

import io.grpc.ManagedChannelBuilder;
import io.hstream.internal.HStreamApiGrpc;
import io.hstream.internal.HStreamApiGrpc.HStreamApiBlockingStub;
import io.hstream.internal.HStreamApiGrpc.HStreamApiStub;
import io.hstream.internal.LookupStreamRequest;
import io.hstream.internal.LookupSubscriptionRequest;
import io.hstream.internal.ServerNode;
import java.util.List;
import org.slf4j.Logger;

interface Retryable {
  void exec(HStreamApiStub stub);
}

interface RetryableBlocking {
  void exec(HStreamApiBlockingStub stub);
}

public class Retry {
  private final ChannelProvider channelProvider;
  private final List<String> serverUrls;
  private final Logger logger;

  public Retry(List<String> serverUrls, Logger logger) {
    this.serverUrls = serverUrls;
    this.logger = logger;

    channelProvider = new ChannelProvider();
  }

  public void withRetriesStream(Retryable xs, String stream) {
    logger.info(
        "begin " + Thread.currentThread().getStackTrace()[2].getMethodName() + " with retry");
    boolean retryStatus = false;
    for (int retryAcc = 0; retryAcc < serverUrls.size() && !retryStatus; retryAcc++) {
      try {
        ServerNode serverNode =
            HStreamApiGrpc.newBlockingStub(
                    ManagedChannelBuilder.forTarget(serverUrls.get(retryAcc))
                        .usePlaintext()
                        .build())
                .lookupStream(LookupStreamRequest.newBuilder().setStreamName(stream).build())
                .getServerNode();
        String serverUrl = serverNode.getHost() + ":" + serverNode.getPort();
        HStreamApiStub stub = HStreamApiGrpc.newStub(channelProvider.get(serverUrl));

        xs.exec(stub);
        retryStatus = true;
      } catch (Exception e) {
        logger.warn(
            "retry because of "
                + e
                + ", "
                + "serverUrl = "
                + serverUrls.get(retryAcc)
                + " retryAcc = "
                + retryAcc);
        if (!(retryAcc + 1 < serverUrls.size())) {
          logger.error("retry failed, " + "retryAcc = " + retryAcc, e);
          throw e;
        }
      }
    }
  }

  public void withRetriesSubscription(Retryable xs, String subscriptionId) {
    logger.info(
        "begin " + Thread.currentThread().getStackTrace()[2].getMethodName() + " with retry");
    boolean retryStatus = false;
    for (int retryAcc = 0; retryAcc < serverUrls.size() && !retryStatus; retryAcc++) {
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
        HStreamApiStub stub = HStreamApiGrpc.newStub(channelProvider.get(serverUrl));

        xs.exec(stub);
        retryStatus = true;
      } catch (Exception e) {
        logger.warn(
            "retry because of "
                + e
                + ", "
                + "serverUrl = "
                + serverUrls.get(retryAcc)
                + " retryAcc = "
                + retryAcc);
        if (!(retryAcc + 1 < serverUrls.size())) {
          logger.error("retry failed, " + "retryAcc = " + retryAcc, e);
          throw e;
        }
      }
    }
  }

  public void withRetriesBlocking(RetryableBlocking xs) {
    logger.info(
        "begin " + Thread.currentThread().getStackTrace()[2].getMethodName() + " with retry");
    boolean retryStatus = false;
    for (int retryAcc = 0; retryAcc < serverUrls.size() && !retryStatus; retryAcc++) {
      try {
        HStreamApiGrpc.HStreamApiBlockingStub stub =
            HStreamApiGrpc.newBlockingStub(channelProvider.get(serverUrls.get(retryAcc)));

        xs.exec(stub);
        retryStatus = true;
      } catch (Exception e) {
        logger.warn(
            "retry because of "
                + e
                + ", "
                + "serverUrl = "
                + serverUrls.get(retryAcc)
                + " retryAcc = "
                + retryAcc);
        if (!(retryAcc + 1 < serverUrls.size())) {
          logger.error("retry failed, " + "retryAcc = " + retryAcc, e);
          throw e;
        }
      }
    }
  }
}
