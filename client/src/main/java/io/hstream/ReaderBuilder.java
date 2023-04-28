package io.hstream;

/** A builder for {@link Reader}s. */
@Deprecated
public interface ReaderBuilder {

  ReaderBuilder streamName(String streamName);

  ReaderBuilder shardId(long shardId);

  ReaderBuilder shardOffset(StreamShardOffset shardOffset);

  ReaderBuilder timeoutMs(int timeoutMs);

  ReaderBuilder readerId(String readerId);

  ReaderBuilder requestTimeoutMs(long timeoutMs);

  Reader build();
}
