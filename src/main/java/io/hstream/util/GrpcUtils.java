package io.hstream.util;

import io.hstream.*;

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
      case UNRECOGNIZED:
        return io.hstream.internal.SubscriptionOffset.SpecialOffset.UNRECOGNIZED;
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
      case UNRECOGNIZED:
        return SubscriptionOffset.SpecialOffset.UNRECOGNIZED;
      default:
        throw new IllegalArgumentException();
    }
  }

  public static io.hstream.internal.SubscriptionOffset subscriptionOffsetToGrpc(
      SubscriptionOffset offset) {
    return io.hstream.internal.SubscriptionOffset.newBuilder()
        .setSpecialOffset(specialOffsetToGrpc(offset.getSpecialOffset()))
        .build();
  }

  public static SubscriptionOffset subscriptionOffsetFromGrpc(
      io.hstream.internal.SubscriptionOffset offset) {
    return new SubscriptionOffset(specialOffsetFromGrpc(offset.getSpecialOffset()));
  }

  public static io.hstream.internal.Subscription subscriptionToGrpc(Subscription subscription) {
    return io.hstream.internal.Subscription.newBuilder()
        .setSubscriptionId(subscription.getSubscriptionId())
        .setStreamName(subscription.getStreamName())
        .setOffset(subscriptionOffsetToGrpc(subscription.getSubscriptionOffset()))
        .build();
  }

  public static Subscription subscriptionFromGrpc(io.hstream.internal.Subscription subscription) {
    return new Subscription(
        subscription.getSubscriptionId(),
        subscription.getStreamName(),
        subscriptionOffsetFromGrpc(subscription.getOffset()));
  }
}
