package io.hstream.impl;

import static com.google.common.base.Preconditions.*;

import io.hstream.Reader;
import io.hstream.ReaderBuilder;
import io.hstream.StreamShardOffset;
import java.util.UUID;

public class ReaderBuilderImpl implements ReaderBuilder {

  private final HStreamClientKtImpl client;

  private String streamName;
  private Long shardId;
  private StreamShardOffset shardOffset =
      new StreamShardOffset(StreamShardOffset.SpecialOffset.EARLIEST);
  private int timeoutMs = 0;
  private String readerId = UUID.randomUUID().toString();

  private long requestTimeoutMs = DefaultSettings.GRPC_CALL_TIMEOUT_MS;

  public ReaderBuilderImpl(HStreamClientKtImpl client) {
    this.client = client;
  }

  @Override
  public ReaderBuilder streamName(String streamName) {
    this.streamName = streamName;
    return this;
  }

  @Override
  public ReaderBuilder shardId(long shardId) {
    this.shardId = shardId;
    return this;
  }

  @Override
  public ReaderBuilder shardOffset(StreamShardOffset shardOffset) {
    this.shardOffset = shardOffset;
    return this;
  }

  @Override
  public ReaderBuilder timeoutMs(int timeoutMs) {
    this.timeoutMs = timeoutMs;
    return this;
  }

  @Override
  public ReaderBuilder readerId(String readerId) {
    this.readerId = readerId;
    return this;
  }

  @Override
  public ReaderBuilder requestTimeoutMs(long timeoutMs) {
    this.requestTimeoutMs = timeoutMs;
    return this;
  }

  @Override
  public Reader build() {
    checkNotNull(client);
    checkArgument(streamName != null, "ReaderBuilder: `streamName` should not be null");
    checkArgument(shardId != null, "ReaderBuilder: `shardId` should not be null");
    checkState(shardId >= 0);
    checkArgument(shardOffset != null, "ReaderBuilder: `shardOffset` should not be null");
    checkState(timeoutMs >= 0);
    checkArgument(readerId != null, "ReaderBuilder: `readerId` should not be null");
    checkArgument(requestTimeoutMs > 0);
    return new ReaderKtImpl(
        client, streamName, shardId, shardOffset, timeoutMs, readerId, requestTimeoutMs);
  }
}
