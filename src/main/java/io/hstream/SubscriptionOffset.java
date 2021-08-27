package io.hstream;

import java.util.Objects;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SubscriptionOffset that = (SubscriptionOffset) o;
    return specialOffset == that.specialOffset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(specialOffset);
  }
}
