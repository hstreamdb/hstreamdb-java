package io.hstream;

import static com.google.common.base.Preconditions.checkArgument;

public class Record {

  private String partitionKey;
  private byte[] rawRecord;
  private HRecord hRecord;
  private boolean isRawRecord;

  public static Builder newBuilder() {
    return new Builder();
  }

  private Record(String partitionKey, byte[] rawRecord) {
    isRawRecord = true;
    this.partitionKey = partitionKey;
    this.rawRecord = rawRecord;
  }

  private Record(String partitionKey, HRecord hRecord) {
    isRawRecord = false;
    this.partitionKey = partitionKey;
    this.hRecord = hRecord;
  }

  public String getPartitionKey() {
    return partitionKey;
  }

  public void setPartitionKey(String partitionKey) {
    this.partitionKey = partitionKey;
  }

  public byte[] getRawRecord() {
    checkArgument(isRawRecord);
    return rawRecord;
  }

  public HRecord getHRecord() {
    checkArgument(!isRawRecord);
    return hRecord;
  }

  public boolean isRawRecord() {
    return isRawRecord;
  }

  public static class Builder {
    private String partitionKey;
    private byte[] rawRecord;
    private HRecord hRecord;

    public Builder partitionKey(String partitionKey) {
      this.partitionKey = partitionKey;
      return this;
    }

    public Builder rawRecord(byte[] rawRecord) {
      this.rawRecord = rawRecord;
      return this;
    }

    public Builder hRecord(HRecord hRecord) {
      this.hRecord = hRecord;
      return this;
    }

    public Record build() {
      checkArgument(
          (rawRecord != null && hRecord == null) || (rawRecord == null && hRecord != null));
      if (rawRecord != null) {
        return new Record(partitionKey, rawRecord);
      } else {
        return new Record(partitionKey, hRecord);
      }
    }
  }
}
