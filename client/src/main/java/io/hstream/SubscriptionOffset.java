package io.hstream;

import java.util.Objects;

/** A class with information about subscription offset */
public class SubscriptionOffset {
  /** Subscription offsets that can be used */
  public enum SpecialOffset {
    /** Start consuming messages from the earliest position in the stream */
    EARLIEST,
    /**
     * Ignore the history messages up to the moment of subscription and ead the messages thereafter.
     */
    LATEST,
    UNRECOGNIZED;
  }

  private SpecialOffset specialOffset;

  /**
   * A constructor for subscription offset
   *
   * @param specialOffset One of the offsets defined in {@link SpecialOffset}
   */
  public SubscriptionOffset(SpecialOffset specialOffset) {
    this.specialOffset = specialOffset;
  }

  /** get the offset */
  public SpecialOffset getSpecialOffset() {
    return specialOffset;
  }

  /** update the offset */
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
