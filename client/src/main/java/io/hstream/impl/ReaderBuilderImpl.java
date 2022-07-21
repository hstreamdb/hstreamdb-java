package io.hstream.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import io.hstream.Reader;
import io.hstream.ReaderBuilder;
import io.hstream.StreamShardOffset;
import java.util.UUID;

public class ReaderBuilderImpl implements ReaderBuilder {

  private final HStreamClientKtImpl client;

  private String streamName;
  private long shardId;
  private StreamShardOffset shardOffset =
      new StreamShardOffset(StreamShardOffset.SpecialOffset.EARLIEST);
  private int timeoutMs = 0;
  private String readerId = UUID.randomUUID().toString();

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
  public Reader build() {
    checkNotNull(client);
    checkNotNull(streamName);
    checkState(shardId >= 0);
    checkNotNull(shardOffset);
    checkState(timeoutMs >= 0);
    checkNotNull(readerId);
    return new ReaderKtImpl(client, streamName, shardId, shardOffset, timeoutMs, readerId);
  }
}
