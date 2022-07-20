package io.hstream;

/** A builder for {@link Reader}s. */
public interface ReaderBuilder {

  ReaderBuilder streamName(String streamName);

  ReaderBuilder shardId(long shardId);

  ReaderBuilder shardOffset(StreamShardOffset shardOffset);

  ReaderBuilder timeoutMs(int timeoutMs);

  ReaderBuilder readerId(String readerId);

  Reader build();
}
