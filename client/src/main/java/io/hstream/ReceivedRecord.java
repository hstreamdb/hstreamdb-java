package io.hstream;

public class ReceivedRecord {
  String recordId;
  Record record;

  public ReceivedRecord(String recordId, Record record) {
    this.recordId = recordId;
    this.record = record;
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
}
