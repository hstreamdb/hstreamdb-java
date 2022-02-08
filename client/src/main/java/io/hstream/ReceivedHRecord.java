package io.hstream;

/** An object that represents a received {@link HRecord} format record. */
public class ReceivedHRecord {

  private RecordId recordId;

  private RecordHeader header;

  private HRecord hRecord;

  public ReceivedHRecord(RecordId recordId, RecordHeader header, HRecord hRecord) {
    this.recordId = recordId;
    this.header = header;
    this.hRecord = hRecord;
  }

  public RecordId getRecordId() {
    return recordId;
  }

  public RecordHeader getHeader() {
    return header;
  }

  public HRecord getHRecord() {
    return hRecord;
  }
}
