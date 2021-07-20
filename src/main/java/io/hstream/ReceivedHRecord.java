package io.hstream;

public class ReceivedHRecord {

  private RecordId recordId;

  private HRecord hRecord;

  public ReceivedHRecord(RecordId recordId, HRecord hRecord) {
    this.recordId = recordId;
    this.hRecord = hRecord;
  }

  public RecordId getRecordId() {
    return recordId;
  }

  public HRecord getHRecord() {
    return hRecord;
  }
}
