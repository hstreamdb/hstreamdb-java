package io.hstream.util;

import io.hstream.*;

/**
 * A class of utility functions to convert between the GRPC generated classes and the custom classes
 * e.g. {@link RecordId}, {@link Subscription}, and {@link Stream}
 */
public class GrpcUtils {

  public static io.hstream.internal.RecordId recordIdToGrpc(RecordId recordId) {
    return io.hstream.internal.RecordId.newBuilder()
        .setBatchId(recordId.getBatchId())
        .setBatchIndex(recordId.getBatchIndex())
        .build();
  }

  public static RecordId recordIdFromGrpc(io.hstream.internal.RecordId recordId) {
    return new RecordId(recordId.getBatchId(), recordId.getBatchIndex());
  }

  public static io.hstream.internal.SubscriptionOffset.SpecialOffset specialOffsetToGrpc(
      SubscriptionOffset.SpecialOffset offset) {
    switch (offset) {
      case EARLIEST:
        return io.hstream.internal.SubscriptionOffset.SpecialOffset.EARLIST;
      case LATEST:
        return io.hstream.internal.SubscriptionOffset.SpecialOffset.LATEST;
      default:
        throw new IllegalArgumentException();
    }
  }

  public static SubscriptionOffset.SpecialOffset specialOffsetFromGrpc(
      io.hstream.internal.SubscriptionOffset.SpecialOffset offset) {
    switch (offset) {
      case EARLIST:
        return SubscriptionOffset.SpecialOffset.EARLIEST;
      case LATEST:
        return SubscriptionOffset.SpecialOffset.LATEST;
      default:
        throw new IllegalArgumentException();
    }
  }

  public static io.hstream.internal.SubscriptionOffset subscriptionOffsetToGrpc(
      SubscriptionOffset offset) {
    if (offset.isSpecialOffset()) {
      return io.hstream.internal.SubscriptionOffset.newBuilder()
          .setSpecialOffset(specialOffsetToGrpc(offset.getSpecialOffset()))
          .build();

    } else {
      return io.hstream.internal.SubscriptionOffset.newBuilder()
          .setRecordOffset(recordIdToGrpc(offset.getNormalOffset()))
          .build();
    }
  }

  public static SubscriptionOffset subscriptionOffsetFromGrpc(
      io.hstream.internal.SubscriptionOffset offset) {
    if (offset.hasRecordOffset()) {
      return new SubscriptionOffset(recordIdFromGrpc(offset.getRecordOffset()));
    } else {
      return new SubscriptionOffset(specialOffsetFromGrpc(offset.getSpecialOffset()));
    }
  }

  public static io.hstream.internal.Subscription subscriptionToGrpc(Subscription subscription) {
    return io.hstream.internal.Subscription.newBuilder()
        .setSubscriptionId(subscription.getSubscriptionId())
        .setStreamName(subscription.getStreamName())
        .setOffset(subscriptionOffsetToGrpc(subscription.getSubscriptionOffset()))
        .setAckTimeoutSeconds(subscription.getAckTimeoutSeconds())
        .build();
  }

  public static Subscription subscriptionFromGrpc(io.hstream.internal.Subscription subscription) {
    return Subscription.newBuilder().subscription(subscription.getSubscriptionId()).stream(
            subscription.getStreamName())
        .offset(subscriptionOffsetFromGrpc(subscription.getOffset()))
        .ackTimeoutSeconds(subscription.getAckTimeoutSeconds())
        .build();
  }

  public static io.hstream.internal.Stream streamToGrpc(Stream stream) {
    return io.hstream.internal.Stream.newBuilder()
        .setStreamName(stream.getStreamName())
        .setReplicationFactor(stream.getReplicationFactor())
        .build();
  }

  public static Stream streamFromGrpc(io.hstream.internal.Stream stream) {
    return new Stream(stream.getStreamName(), stream.getReplicationFactor());
  }
}