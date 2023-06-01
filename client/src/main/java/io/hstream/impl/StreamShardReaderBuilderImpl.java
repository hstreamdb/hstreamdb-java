package io.hstream.impl;

import static com.google.common.base.Preconditions.*;
import static com.google.common.base.Preconditions.checkArgument;

import io.hstream.*;

public class StreamShardReaderBuilderImpl implements StreamShardReaderBuilder {

  private final HStreamClientKtImpl client;
  private String streamName;
  private long shardId;
  private StreamShardOffset shardOffset;

  private StreamShardReaderReceiver receiver;
  private StreamShardReaderBatchReceiver batchReceiver;

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
  public StreamShardReaderBuilder batchReceiver(StreamShardReaderBatchReceiver batchReceiver) {
    this.batchReceiver = batchReceiver;
    return this;
  }

  @Override
  public StreamShardReader build() {
    checkNotNull(client);
    checkArgument(streamName != null, "StreamShardReaderBuilder: `streamName` should not be null");
    checkArgument(shardId > 0, "StreamShardReaderBuilder: `shardId` error");
    checkArgument(
        shardOffset != null, "StreamShardReaderBuilder: `shardOffset` should not be null");
    checkArgument(receiver != null || batchReceiver != null,
            "StreamShardReaderBuilder: `receiver` should not be both null");
    return new StreamShardReaderKtImpl(client, streamName, shardId, shardOffset, receiver, batchReceiver);
  }
}
