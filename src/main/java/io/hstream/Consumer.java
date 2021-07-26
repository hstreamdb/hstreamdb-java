package io.hstream;

import java.util.List;

public interface Consumer extends AutoCloseable {

  List<ReceivedHRecord> pollHRecords();

  List<ReceivedRawRecord> pollRawRecords();

  void commit(RecordId recordId);
}
