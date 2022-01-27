package io.hstream;

import static com.google.common.base.Preconditions.checkArgument;

public class Record {

  private String orderingKey;
  private byte[] rawRecord;
  private HRecord hRecord;
  private boolean isRawRecord;

  public static Builder newBuilder() {
    return new Builder();
  }

  private Record(String orderingKey, byte[] rawRecord) {
    isRawRecord = true;
    this.orderingKey = orderingKey;
    this.rawRecord = rawRecord;
  }

  private Record(String orderingKey, HRecord hRecord) {
    isRawRecord = false;
    this.orderingKey = orderingKey;
    this.hRecord = hRecord;
  }

  public String getOrderingKey() {
    return orderingKey;
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
    private String orderingKey;
    private byte[] rawRecord;
    private HRecord hRecord;

    public Builder orderingKey(String key) {
      this.orderingKey = key;
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
        return new Record(orderingKey, rawRecord);
      } else {
        return new Record(orderingKey, hRecord);
      }
    }
  }
}
