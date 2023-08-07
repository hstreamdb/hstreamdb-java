package io.hstream.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import io.hstream.StreamKeyReader;
import io.hstream.StreamKeyReaderBuilder;
import io.hstream.StreamShardOffset;

public class StreamKeyReaderBuilderImpl implements StreamKeyReaderBuilder {

  private final HStreamClientKtImpl client;
  private String streamName;
  private String key;
  private StreamShardOffset from = new StreamShardOffset(StreamShardOffset.SpecialOffset.EARLIEST);
  private StreamShardOffset until = new StreamShardOffset(StreamShardOffset.SpecialOffset.LATEST);

  int bufferSize = 100;

  public StreamKeyReaderBuilderImpl(HStreamClientKtImpl client) {
    this.client = client;
  }

  @Override
  public StreamKeyReaderBuilder streamName(String streamName) {
    this.streamName = streamName;
    return this;
  }

  @Override
  public StreamKeyReaderBuilder key(String key) {
    this.key = key;
    return this;
  }

  @Override
  public StreamKeyReaderBuilder from(StreamShardOffset shardOffset) {
    this.from = shardOffset;
    return this;
  }

  @Override
  public StreamKeyReaderBuilder until(StreamShardOffset until) {
    this.until = until;
    return this;
  }

  @Override
  public StreamKeyReaderBuilder bufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
    return this;
  }

  @Override
  public StreamKeyReader build() {
    checkNotNull(client);
    checkArgument(streamName != null, "StreamKeyReaderBuilder: `streamName` should not be null");
    checkArgument(key != null, "StreamKeyReaderBuilder: `key` should not be null");
    checkArgument(from != null, "StreamKeyReaderBuilder: `from` should not be null");
    checkArgument(until != null, "StreamKeyReaderBuilder: `from` should not be null");
    checkArgument(bufferSize > 0);
    return new StreamKeyReaderKtImpl(client, streamName, key, from, until, bufferSize);
  }
}
