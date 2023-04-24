package io.hstream;

public interface StreamShardReaderReceiver {

  void process(ReceivedRecord receivedRecord);
}
