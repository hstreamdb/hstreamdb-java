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

  public static io.hstream.internal.Subscription subscriptionToGrpc(Subscription subscription) {
    return io.hstream.internal.Subscription.newBuilder()
        .setSubscriptionId(subscription.getSubscriptionId())
        .setStreamName(subscription.getStreamName())
        .setAckTimeoutSeconds(subscription.getAckTimeoutSeconds())
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
        .build();
  }

  public static Stream streamFromGrpc(io.hstream.internal.Stream stream) {
    return new Stream(
        stream.getStreamName(), stream.getReplicationFactor(), stream.getBacklogDuration());
  }
}
