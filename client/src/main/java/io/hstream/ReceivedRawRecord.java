package io.hstream;

/** An object that represents a received raw record. */
public class ReceivedRawRecord {

  private RecordId recordId;

  private RecordHeader header;

  private byte[] rawRecord;

  public ReceivedRawRecord(RecordId recordId, RecordHeader header, byte[] rawRecord) {
    this.recordId = recordId;
    this.header = header;
    this.rawRecord = rawRecord;
  }

  public RecordId getRecordId() {
    return recordId;
  }

  public RecordHeader getHeader() {
    return header;
  }

  public byte[] getRawRecord() {
    return rawRecord;
  }
}
