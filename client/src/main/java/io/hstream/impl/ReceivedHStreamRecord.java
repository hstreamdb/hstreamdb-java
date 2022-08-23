package io.hstream.impl;

import io.hstream.internal.HStreamRecord;
import io.hstream.internal.RecordId;

public class ReceivedHStreamRecord {
  private RecordId recordId;
  private HStreamRecord record;

  public ReceivedHStreamRecord(RecordId recordId, HStreamRecord record) {
    this.recordId = recordId;
    this.record = record;
  }

  public RecordId getRecordId() {
    return recordId;
  }

  public HStreamRecord getRecord() {
    return record;
  }
}
