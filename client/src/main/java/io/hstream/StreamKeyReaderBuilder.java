package io.hstream;

public interface StreamKeyReaderBuilder {

  StreamKeyReaderBuilder streamName(String streamName);

  StreamKeyReaderBuilder key(String key);

  StreamKeyReaderBuilder from(StreamShardOffset from);

  StreamKeyReaderBuilder until(StreamShardOffset until);
  StreamKeyReaderBuilder bufferSize(int bufferSize);

  StreamKeyReader build();
}
