package io.hstream;

public interface StreamShardReaderBuilder {

  StreamShardReaderBuilder streamName(String streamName);

  StreamShardReaderBuilder shardId(long shardId);

  StreamShardReaderBuilder shardOffset(StreamShardOffset shardOffset);

  StreamShardReaderBuilder receiver(StreamShardReaderReceiver streamShardReaderReceiver);
  StreamShardReaderBuilder batchReceiver(StreamShardReaderBatchReceiver streamShardReaderReceiver);

  StreamShardReader build();
}
