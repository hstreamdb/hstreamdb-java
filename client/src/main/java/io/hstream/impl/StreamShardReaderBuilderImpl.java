package io.hstream.impl;

import static com.google.common.base.Preconditions.*;
import static com.google.common.base.Preconditions.checkArgument;

import io.hstream.StreamShardOffset;
import io.hstream.StreamShardReader;
import io.hstream.StreamShardReaderBuilder;
import io.hstream.StreamShardReaderReceiver;

public class StreamShardReaderBuilderImpl implements StreamShardReaderBuilder {

  private final HStreamClientKtImpl client;
  private String streamName;
  private long shardId;
  private StreamShardOffset shardOffset;

  private StreamShardReaderReceiver receiver;

  public StreamShardReaderBuilderImpl(HStreamClientKtImpl client) {
    this.client = client;
  }

  @Override
  public StreamShardReaderBuilder streamName(String streamName) {
    this.streamName = streamName;
    return this;
  }

  @Override
  public StreamShardReaderBuilder shardId(long shardId) {
    this.shardId = shardId;
    return this;
  }

  @Override
  public StreamShardReaderBuilder shardOffset(StreamShardOffset shardOffset) {
    this.shardOffset = shardOffset;
    return this;
  }

  @Override
  public StreamShardReaderBuilder receiver(StreamShardReaderReceiver streamShardReaderReceiver) {
    this.receiver = streamShardReaderReceiver;
    return this;
  }

  @Override
  public StreamShardReader build() {
    checkNotNull(client);
    checkArgument(streamName != null, "StreamShardReaderBuilder: `streamName` should not be null");
    checkArgument(shardId > 0, "StreamShardReaderBuilder: `shardId` error");
    checkArgument(
        shardOffset != null, "StreamShardReaderBuilder: `shardOffset` should not be null");
    checkArgument(receiver != null, "StreamShardReaderBuilder: `receiver` should not be null");
    return new StreamShardReaderKtImpl(client, streamName, shardId, shardOffset, receiver);
  }
}
