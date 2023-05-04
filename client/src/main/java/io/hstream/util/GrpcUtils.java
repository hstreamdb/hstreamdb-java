package io.hstream.util;

import static com.google.common.base.Preconditions.*;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.hstream.*;
import io.hstream.internal.RecordId;
import io.hstream.internal.ShardOffset;
import io.hstream.internal.SpecialOffset;
import io.hstream.internal.TaskStatusPB;
import io.hstream.internal.TimestampOffset;
import java.time.Instant;
import java.util.stream.Collectors;

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
    var createdTime = subscription.getCreationTime();
    return Subscription.newBuilder().subscription(subscription.getSubscriptionId()).stream(
            subscription.getStreamName())
        .ackTimeoutSeconds(subscription.getAckTimeoutSeconds())
        .maxUnackedRecords(subscription.getMaxUnackedRecords())
        .offset(subscriptionOffsetFromGrpc(subscription.getOffset()))
        .createdTime(Instant.ofEpochSecond(createdTime.getSeconds(), createdTime.getNanos()))
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
    var createdTime = stream.getCreationTime();
    return Stream.newBuilder()
        .streamName(stream.getStreamName())
        .replicationFactor(stream.getReplicationFactor())
        .backlogDuration(stream.getBacklogDuration())
        .shardCount(stream.getShardCount())
        .createdTime(Instant.ofEpochSecond(createdTime.getSeconds(), createdTime.getNanos()))
        .build();
  }

  public static CompressionType compressionTypeFromGrpc(
      io.hstream.internal.CompressionType compressionType) {
    switch (compressionType) {
      case None:
        return CompressionType.NONE;
      case Gzip:
        return CompressionType.GZIP;
      case Zstd:
        return CompressionType.ZSTD;
      case UNRECOGNIZED:
        throw new IllegalArgumentException();
    }
    throw new IllegalArgumentException();
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
    } else if (shardOffset.isTimestampOffset()) {
      return ShardOffset.newBuilder()
          .setTimestampOffset(
              TimestampOffset.newBuilder()
                  .setTimestampInMs(shardOffset.getTimestampOffset())
                  .setStrictAccuracy(true)
                  .build())
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
      case ZSTD:
        return io.hstream.internal.CompressionType.Zstd;
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
      case Zstd:
        return io.hstream.CompressionType.ZSTD;
      default:
        throw new IllegalArgumentException("Unknown compressionType: " + compressionType);
    }
  }

  public static Query queryFromInternal(io.hstream.internal.Query query) {
    return Query.newBuilder()
        .name(query.getId())
        .type(Query.QueryType.valueOf(query.getType().name()))
        .status(taskStatusFromInternal(query.getStatus()))
        .createdTime(query.getCreatedTime())
        .queryText(query.getQueryText())
        .sourceStreams(query.getSourcesList())
        .resultName(query.getResultName())
        .build();
  }

  public static View viewFromInternal(io.hstream.internal.View view) {
    return View.newBuilder()
        .name(view.getViewId())
        .status(taskStatusFromInternal(view.getStatus()))
        .queryName(view.getQueryName())
        .sql(view.getSql())
        .createdTime(view.getCreatedTime())
        .schema(view.getSchemaList())
        .build();
  }

  public static TaskStatus taskStatusFromInternal(TaskStatusPB statusPB) {
    switch (statusPB) {
      case TASK_CREATING:
        return TaskStatus.CREATING;
      case TASK_RUNNING:
        return TaskStatus.RUNNING;
      case TASK_ABORTED:
        return TaskStatus.ABORTED;
      case TASK_TERMINATED:
        return TaskStatus.TERMINATED;
      default:
        return TaskStatus.UNKNOWN;
    }
  }

  public static ConsumerInformation consumerInformationFromGrpc(
      io.hstream.internal.Consumer consumer) {
    return ConsumerInformation.newBuilder()
        .name(consumer.getName())
        .uri(consumer.getUri())
        .userAgent(consumer.getUserAgent())
        .build();
  }

  public static GetSubscriptionResponse GetSubscriptionResponseFromGrpc(
      io.hstream.internal.GetSubscriptionResponse response) {
    return GetSubscriptionResponse.newBuilder()
        .subscription(subscriptionFromGrpc(response.getSubscription()))
        .offsets(
            response.getOffsetsList().stream()
                .map(GrpcUtils::subscriptionOffsetFromGrpc)
                .collect(Collectors.toList()))
        .build();
  }

  public static GetStreamResponse GetStreamResponseFromGrpc(
      io.hstream.internal.GetStreamResponse response) {
    return GetStreamResponse.newBuilder().setStream(streamFromGrpc(response.getStream())).build();
  }

  public static SubscriptionOffset subscriptionOffsetFromGrpc(
      io.hstream.internal.SubscriptionOffset offset) {
    return SubscriptionOffset.newBuilder()
        .withShardId(offset.getShardId())
        .withBatchId(offset.getBatchId())
        .build();
  }

  // == Connector
  public static Connector ConnectorFromGrpc(io.hstream.internal.Connector connector) {
    var createdTime = connector.getCreationTime();
    return Connector.newBuilder()
        .name(connector.getName())
        .type(ConnectorType.valueOf(connector.getType()))
        .target(connector.getTarget())
        .status(connector.getStatus())
        .createdTime(Instant.ofEpochSecond(createdTime.getSeconds(), createdTime.getNanos()))
        .config(connector.getConfig())
        .offsets(
            connector.getOffsetsList().stream()
                .map(GrpcUtils::structToString)
                .collect(Collectors.toList()))
        .build();
  }

  public static String structToString(Struct struct) {
    try {
      return JsonFormat.printer().omittingInsignificantWhitespace().print(struct);
    } catch (InvalidProtocolBufferException e) {
      throw new HStreamDBClientException(e);
    }
  }
}
