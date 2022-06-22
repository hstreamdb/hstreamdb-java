package io.hstream.util;

import io.hstream.*;
import io.hstream.internal.SpecialOffset;

/**
 * A class of utility functions to convert between the GRPC generated classes and the custom classes
 * e.g. {@link String}, {@link Subscription}, and {@link Stream}
 */
public class GrpcUtils {

  public static String recordIdFromGrpc(io.hstream.internal.RecordId recordId) {
    return String.format(
        "%s-%s-%s", recordId.getBatchIndex(), recordId.getBatchId(), recordId.getShardId());
  }

  public static SpecialOffset subscriptionOffsetToGrpc(Subscription.SubscriptionOffset offset) {
    switch (offset) {
      case EARLEST:
        return io.hstream.internal.SpecialOffset.EARLIEST;
      case LATEST:
        return io.hstream.internal.SpecialOffset.LATEST;
      default:
        throw new IllegalArgumentException("Unknown subscription offset: " + offset);
    }
  }

  public static Subscription.SubscriptionOffset subscriptionOffsetFromGrpc(SpecialOffset offset) {
    switch (offset) {
      case EARLIEST:
        return Subscription.SubscriptionOffset.EARLEST;
      case LATEST:
        return Subscription.SubscriptionOffset.LATEST;
      default:
        throw new IllegalArgumentException("Unknown subscription offset: " + offset);
    }
  }

  public static io.hstream.internal.Subscription subscriptionToGrpc(Subscription subscription) {
    return io.hstream.internal.Subscription.newBuilder()
        .setSubscriptionId(subscription.getSubscriptionId())
        .setStreamName(subscription.getStreamName())
        .setAckTimeoutSeconds(subscription.getAckTimeoutSeconds())
        .setMaxUnackedRecords(subscription.getMaxUnackedRecords())
        .setOffset(subscriptionOffsetToGrpc(subscription.getOffset()))
        .build();
  }

  public static Subscription subscriptionFromGrpc(io.hstream.internal.Subscription subscription) {
    return Subscription.newBuilder().subscription(subscription.getSubscriptionId()).stream(
            subscription.getStreamName())
        .ackTimeoutSeconds(subscription.getAckTimeoutSeconds())
        .offset(subscriptionOffsetFromGrpc(subscription.getOffset()))
        .build();
  }

  public static io.hstream.internal.Stream streamToGrpc(Stream stream) {
    return io.hstream.internal.Stream.newBuilder()
        .setStreamName(stream.getStreamName())
        .setReplicationFactor(stream.getReplicationFactor())
        .setBacklogDuration(stream.getBacklogDuration())
        .setShardCount(stream.getShardCount())
        .build();
  }

  public static Stream streamFromGrpc(io.hstream.internal.Stream stream) {
    return new Stream(
        stream.getStreamName(),
        stream.getReplicationFactor(),
        stream.getBacklogDuration(),
        stream.getShardCount());
  }
}
