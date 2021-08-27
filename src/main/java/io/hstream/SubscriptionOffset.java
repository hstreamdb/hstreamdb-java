package io.hstream;

public class SubscriptionOffset {
  public enum SpecialOffset {
    EARLIEST,
    LATEST,
    UNRECOGNIZED;
  }

  private SpecialOffset specialOffset;

  public SubscriptionOffset(SpecialOffset specialOffset) {
    this.specialOffset = specialOffset;
  }

  public SpecialOffset getSpecialOffset() {
    return specialOffset;
  }

  public void setSpecialOffset(SpecialOffset specialOffset) {
    this.specialOffset = specialOffset;
  }
}
