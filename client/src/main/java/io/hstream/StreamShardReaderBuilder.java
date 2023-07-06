package io.hstream;

public interface StreamShardReaderBuilder {

  StreamShardReaderBuilder streamName(String streamName);

  StreamShardReaderBuilder shardId(long shardId);

  StreamShardReaderBuilder from(StreamShardOffset shardOffset);
  StreamShardReaderBuilder maxReadBatches(long maxReadBatches);
  StreamShardReaderBuilder until(StreamShardOffset until);

  StreamShardReaderBuilder receiver(StreamShardReaderReceiver streamShardReaderReceiver);
  StreamShardReaderBuilder batchReceiver(StreamShardReaderBatchReceiver streamShardReaderReceiver);

  StreamShardReader build();
}
