package io.hstream;

/** An object that represents a received {@link HRecord} format record. */
public class ReceivedHRecord {

  private String recordId;

  private RecordHeader header;

  private HRecord hRecord;

  public ReceivedHRecord(String recordId, RecordHeader header, HRecord hRecord) {
    this.recordId = recordId;
    this.header = header;
    this.hRecord = hRecord;
  }

  public String getRecordId() {
    return recordId;
  }

  public RecordHeader getHeader() {
    return header;
  }

  public HRecord getHRecord() {
    return hRecord;
  }
}
