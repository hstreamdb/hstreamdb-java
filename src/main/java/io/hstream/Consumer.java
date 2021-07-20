package io.hstream;

import java.util.List;

public interface Consumer {

  List<ReceivedHRecord> pollHRecords();

  List<ReceivedRawRecord> pollRawRecords();

  void commit(RecordId recordId);
}
