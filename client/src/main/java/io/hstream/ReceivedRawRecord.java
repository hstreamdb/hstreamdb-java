package io.hstream;

import java.time.Instant;

/** An object that represents a received raw record. */
public class ReceivedRawRecord {

  private String recordId;

  private RecordHeader header;

  private byte[] rawRecord;

  private Instant createdTime;

  public ReceivedRawRecord(
      String recordId, RecordHeader header, byte[] rawRecord, Instant createdTime) {
    this.recordId = recordId;
    this.header = header;
    this.rawRecord = rawRecord;
    this.createdTime = createdTime;
  }

  public String getRecordId() {
    return recordId;
  }

  public RecordHeader getHeader() {
    return header;
  }

  public byte[] getRawRecord() {
    return rawRecord;
  }

  public Instant getCreatedTime() {
    return createdTime;
  }
}
