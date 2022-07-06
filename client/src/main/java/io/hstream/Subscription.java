package io.hstream;

import static com.google.common.base.Preconditions.*;

import java.util.Objects;

public class Subscription {

  private String subscriptionId;
  private String streamName;
  private int ackTimeoutSeconds;

  private int maxUnackedRecords;

  private SubscriptionOffset offset;

  public enum SubscriptionOffset {
    EARLEST,
    LATEST,
  }

  private Subscription(
      String subscriptionId,
      String streamName,
      int ackTimeoutSeconds,
      int maxUnackedRecords,
      SubscriptionOffset offset) {
    this.subscriptionId = subscriptionId;
    this.streamName = streamName;
    this.ackTimeoutSeconds = ackTimeoutSeconds;
    this.maxUnackedRecords = maxUnackedRecords;
    this.offset = offset;
  }

  /** @return {@link Subscription.Builder} */
  public static Builder newBuilder() {
    return new Builder();
  }

  public String getSubscriptionId() {
    return subscriptionId;
  }

  public String getStreamName() {
    return streamName;
  }

  public int getAckTimeoutSeconds() {
    return ackTimeoutSeconds;
  }

  public int getMaxUnackedRecords() {
    return maxUnackedRecords;
  }

  public SubscriptionOffset getOffset() {
    return offset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Subscription that = (Subscription) o;
    return ackTimeoutSeconds == that.ackTimeoutSeconds
        && maxUnackedRecords == that.maxUnackedRecords
        && subscriptionId.equals(that.subscriptionId)
        && streamName.equals(that.streamName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subscriptionId, streamName, ackTimeoutSeconds, maxUnackedRecords);
  }

  public static class Builder {

    private String subscriptionId;
    private String streamName;
    private int ackTimeoutSeconds = 600;
    private int maxUnackedRecords = 10000;
    private SubscriptionOffset offset = SubscriptionOffset.LATEST;

    public Builder subscription(String subscriptionId) {
      this.subscriptionId = subscriptionId;
      return this;
    }

    public Builder stream(String streamName) {
      this.streamName = streamName;
      return this;
    }

    public Builder ackTimeoutSeconds(int ackTimeoutSeconds) {
      this.ackTimeoutSeconds = ackTimeoutSeconds;
      return this;
    }

    public Builder maxUnackedRecords(int maxUnackedRecords) {
      this.maxUnackedRecords = maxUnackedRecords;
      return this;
    }

    public Builder offset(SubscriptionOffset offset) {
      this.offset = offset;
      return this;
    }

    public Subscription build() {
      checkNotNull(subscriptionId);
      checkNotNull(streamName);
      checkState(ackTimeoutSeconds > 0 && ackTimeoutSeconds < 36000);
      checkState(maxUnackedRecords > 0);
      return new Subscription(
          subscriptionId, streamName, ackTimeoutSeconds, maxUnackedRecords, offset);
    }
  }
}
