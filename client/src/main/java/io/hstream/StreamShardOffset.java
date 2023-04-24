package io.hstream;

public class StreamShardOffset {

  public enum SpecialOffset {
    EARLIEST,
    LATEST;
  }

  public enum OffsetType {
    SPECIAL,
    TIMESTAMP,
    NORMAL;
  }

  private SpecialOffset specialOffset;
  private String recordId;
  private long timestamp;

  private OffsetType offsetType;

  public StreamShardOffset(SpecialOffset specialOffset) {
    this.specialOffset = specialOffset;
    this.offsetType = OffsetType.SPECIAL;
  }

  public StreamShardOffset(String recordId) {
    this.recordId = recordId;
    this.offsetType = OffsetType.NORMAL;
  }

  public StreamShardOffset(long timestamp) {
    this.timestamp = timestamp;
    this.offsetType = OffsetType.TIMESTAMP;
  }

  public boolean isSpecialOffset() {
    return offsetType.equals(OffsetType.SPECIAL);
  }

  public boolean isNormalOffset() {
    return offsetType.equals(OffsetType.NORMAL);
  }

  public boolean isTimestampOffset() {
    return offsetType.equals(OffsetType.TIMESTAMP);
  }

  public OffsetType getOffsetType() {
    return offsetType;
  }

  public SpecialOffset getSpecialOffset() {
    if (isSpecialOffset()) {
      return specialOffset;
    } else {
      throw new IllegalStateException("subscriptionOffset is not specialOffset");
    }
  }

  public String getNormalOffset() {
    if (isNormalOffset()) {
      return recordId;
    } else {
      throw new IllegalStateException("subscriptionOffset is not normal offset");
    }
  }

  public long getTimestampOffset() {
    if (isTimestampOffset()) {
      return timestamp;
    } else {
      throw new IllegalStateException("subscriptionOffset is not timestamp offset");
    }
  }
}
