package io.hstream.util;

import static com.google.common.base.Preconditions.*;

import io.hstream.*;
import io.hstream.internal.RecordId;
import io.hstream.internal.SpecialOffset;

/**
 * A class of utility functions to convert between the GRPC generated classes and the custom classes
 * e.g. {@link String}, {@link Subscription}, and {@link Stream}
 */
public class GrpcUtils {

  public static String recordIdFromGrpc(io.hstream.internal.RecordId recordId) {
    return String.format(
        "%s-%s-%s", recordId.getShardId(), recordId.getBatchId(), recordId.getBatchIndex());
  }

  public static RecordId recordIdToGrpc(String recordId) {
    String[] res = recordId.split("-");
    checkArgument(res.length == 3);
    return RecordId.newBuilder()
        .setShardId(Long.parseLong(res[0]))
        .setBatchId(Long.parseLong(res[1]))
        .setBatchIndex(Integer.parseInt(res[2]))
        .build();
  }

  public static SpecialOffset subscriptionOffsetToGrpc(Subscription.SubscriptionOffset offset) {
    switch (offset) {
      case EARLIEST:
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
        return Subscription.SubscriptionOffset.EARLIEST;
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

  public static StreamShardOffset streamShardOffsetFromGrpc(
      io.hstream.internal.ShardOffset shardOffset) {
    if (shardOffset.hasSpecialOffset()) {
      switch (shardOffset.getSpecialOffset()) {
        case EARLIEST:
          return new StreamShardOffset(StreamShardOffset.SpecialOffset.EARLIEST);
        case LATEST:
          return new StreamShardOffset(StreamShardOffset.SpecialOffset.LATEST);
        default:
          throw new IllegalArgumentException("Unknown ShardOffset : " + shardOffset);
      }
    } else if (shardOffset.hasRecordOffset()) {
      return new StreamShardOffset(recordIdFromGrpc(shardOffset.getRecordOffset()));
    } else {
      throw new IllegalArgumentException("Unknown ShardOffset : " + shardOffset);
    }
  }

  public static io.hstream.internal.ShardOffset streamShardOffsetToGrpc(
      StreamShardOffset shardOffset) {
    if (shardOffset.isSpecialOffset()) {
      switch (shardOffset.getSpecialOffset()) {
        case EARLIEST:
          return io.hstream.internal.ShardOffset.newBuilder()
              .setSpecialOffset(SpecialOffset.EARLIEST)
              .build();
        case LATEST:
          return io.hstream.internal.ShardOffset.newBuilder()
              .setSpecialOffset(SpecialOffset.LATEST)
              .build();
        default:
          throw new IllegalArgumentException("Unknown streamShardOffset : " + shardOffset);
      }
    } else if (shardOffset.isNormalOffset()) {
      return io.hstream.internal.ShardOffset.newBuilder()
          .setRecordOffset(recordIdToGrpc(shardOffset.getNormalOffset()))
          .build();
    } else {
      throw new IllegalArgumentException("Unknown streamShardOffset : " + shardOffset);
    }
  }

  public static io.hstream.internal.CompressionType compressionTypeToInternal(
      io.hstream.CompressionType compressionType) {
    switch (compressionType) {
      case NONE:
        return io.hstream.internal.CompressionType.None;
      case GZIP:
        return io.hstream.internal.CompressionType.Gzip;
      default:
        throw new IllegalArgumentException("Unknown compressionType: " + compressionType);
    }
  }

  public static io.hstream.CompressionType compressionTypeFromInternal(
      io.hstream.internal.CompressionType compressionType) {

    switch (compressionType) {
      case None:
        return io.hstream.CompressionType.NONE;
      case Gzip:
        return io.hstream.CompressionType.GZIP;
      default:
        throw new IllegalArgumentException("Unknown compressionType: " + compressionType);
    }
  }
}
