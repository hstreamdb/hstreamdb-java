package io.hstream.util;

import io.hstream.*;

/**
 * A class of utility functions to convert between the GRPC generated classes and the custom classes
 * e.g. {@link String}, {@link Subscription}, and {@link Stream}
 */
public class GrpcUtils {

  public static String recordIdFromGrpc(io.hstream.internal.RecordId recordId) {
    return String.format(
        "%s-%s-%s", recordId.getBatchIndex(), recordId.getBatchId(), recordId.getShardId());
  }

  public static io.hstream.internal.Subscription subscriptionToGrpc(Subscription subscription) {
    return io.hstream.internal.Subscription.newBuilder()
        .setSubscriptionId(subscription.getSubscriptionId())
        .setStreamName(subscription.getStreamName())
        .setAckTimeoutSeconds(subscription.getAckTimeoutSeconds())
        .setMaxUnackedRecords(subscription.getMaxUnackedRecords())
        .build();
  }

  public static Subscription subscriptionFromGrpc(io.hstream.internal.Subscription subscription) {
    return Subscription.newBuilder().subscription(subscription.getSubscriptionId()).stream(
            subscription.getStreamName())
        .ackTimeoutSeconds(subscription.getAckTimeoutSeconds())
        .build();
  }

  public static io.hstream.internal.Stream streamToGrpc(Stream stream) {
    return io.hstream.internal.Stream.newBuilder()
        .setStreamName(stream.getStreamName())
        .setReplicationFactor(stream.getReplicationFactor())
        .setBacklogDuration(stream.getBacklogDuration())
        .build();
  }

  public static Stream streamFromGrpc(io.hstream.internal.Stream stream) {
    return new Stream(
        stream.getStreamName(), stream.getReplicationFactor(), stream.getBacklogDuration());
  }
}
