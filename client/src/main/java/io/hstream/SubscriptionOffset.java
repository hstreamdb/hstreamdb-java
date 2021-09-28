package io.hstream;

public class SubscriptionOffset {

  public enum SpecialOffset {
    EARLIEST,
    LATEST;
  }

  private enum OffsetType {
    SPECIAL,
    NORMAL;
  }

  private SpecialOffset specialOffset;
  private RecordId recordId;
  private OffsetType offsetType;

  public SubscriptionOffset(SpecialOffset specialOffset) {
    this.specialOffset = specialOffset;
    this.offsetType = OffsetType.SPECIAL;
  }

  public SubscriptionOffset(RecordId recordId) {
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

  public RecordId getNormalOffset() {
    if (isSpecialOffset()) {
      return recordId;
    } else {
      throw new IllegalStateException("subscriptionOffset is not normal offset");
    }
  }
}
