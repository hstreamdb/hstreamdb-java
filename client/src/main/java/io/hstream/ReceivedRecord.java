package io.hstream;

import java.time.Instant;

public class ReceivedRecord {
  String recordId;
  Record record;
  Instant createdTime;

  public ReceivedRecord(String recordId, Record record, Instant createdTime) {
    this.recordId = recordId;
    this.record = record;
    this.createdTime = createdTime;
  }

  public String getRecordId() {
    return recordId;
  }

  public void setRecordId(String recordId) {
    this.recordId = recordId;
  }

  public Record getRecord() {
    return record;
  }

  public void setRecord(Record record) {
    this.record = record;
  }

  public Instant getCreatedTime() {
    return createdTime;
  }
}
