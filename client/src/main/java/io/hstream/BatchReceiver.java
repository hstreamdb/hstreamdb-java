package io.hstream;

import java.util.List;

public interface BatchReceiver {
  void processRecords(List<ReceivedRecord> records, BatchAckResponder batchAckResponder);
}
