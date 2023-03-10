package io.hstream;

import java.time.Instant;

/** An object that represents a received {@link HRecord} format record. */
public class ReceivedHRecord {

  private String recordId;

  private RecordHeader header;

  private HRecord hRecord;

  private Instant createdTime;

  public ReceivedHRecord(
      String recordId, RecordHeader header, HRecord hRecord, Instant createdTime) {
    this.recordId = recordId;
    this.header = header;
    this.hRecord = hRecord;
    this.createdTime = createdTime;
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

  public Instant getCreatedTime() {
    return createdTime;
  }
}
