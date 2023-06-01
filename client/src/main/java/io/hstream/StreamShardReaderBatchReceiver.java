package io.hstream;

import java.util.List;

public interface StreamShardReaderBatchReceiver {

  void process(List<ReceivedRecord> receivedRecords);
}
