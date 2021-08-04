package io.hstream.impl;

import io.grpc.StatusRuntimeException;
import io.hstream.CommittedOffset;
import io.hstream.HStreamApiGrpc;
import io.hstream.RecordId;
import io.hstream.Responder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponderImpl implements Responder {
  private static final Logger logger = LoggerFactory.getLogger(ResponderImpl.class);

  private final HStreamApiGrpc.HStreamApiBlockingStub blockingStub;
  private final String subscriptionId;
  private final RecordId recordId;

  public ResponderImpl(
      HStreamApiGrpc.HStreamApiBlockingStub blockingStub,
      String subscriptionId,
      RecordId recordId) {
    this.blockingStub = blockingStub;
    this.subscriptionId = subscriptionId;
    this.recordId = recordId;
  }

  @Override
  public void ack() {
    CommittedOffset committedOffset =
        CommittedOffset.newBuilder().setSubscriptionId(subscriptionId).setOffset(recordId).build();

    try {
      blockingStub.commitOffset(committedOffset);
    } catch (StatusRuntimeException e) {
      logger.error("commit offset failed: {}", e);
      throw new RuntimeException(e);
    }
    logger.info("committed offset {} for subscription {}", recordId, subscriptionId);
  }
}
