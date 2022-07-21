package io.hstream;

public class StreamShardOffset {

  public enum SpecialOffset {
    EARLIEST,
    LATEST;
  }

  private enum OffsetType {
    SPECIAL,
    NORMAL;
  }

  private SpecialOffset specialOffset;
  private String recordId;
  private OffsetType offsetType;

  public StreamShardOffset(SpecialOffset specialOffset) {
    this.specialOffset = specialOffset;
    this.offsetType = OffsetType.SPECIAL;
  }

  public StreamShardOffset(String recordId) {
    this.recordId = recordId;
    this.offsetType = OffsetType.NORMAL;
  }

  public boolean isSpecialOffset() {
    return offsetType.equals(OffsetType.SPECIAL);
  }

  public boolean isNormalOffset() {
    return offsetType.equals(OffsetType.NORMAL);
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
}
