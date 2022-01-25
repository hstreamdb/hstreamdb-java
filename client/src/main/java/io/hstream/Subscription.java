package io.hstream;

import static com.google.common.base.Preconditions.*;

import java.util.Objects;

public class Subscription {

  private String subscriptionId;
  private String streamName;
  private int ackTimeoutSeconds;

  private Subscription(
      String subscriptionId,
      String streamName,
      int ackTimeoutSeconds) {
    this.subscriptionId = subscriptionId;
    this.streamName = streamName;
    this.ackTimeoutSeconds = ackTimeoutSeconds;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Subscription that = (Subscription) o;
    return ackTimeoutSeconds == that.ackTimeoutSeconds
        && subscriptionId.equals(that.subscriptionId)
        && streamName.equals(that.streamName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subscriptionId, streamName, ackTimeoutSeconds);
  }

  public static class Builder {

    private String subscriptionId;
    private String streamName;
    private int ackTimeoutSeconds = 600;

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

    public Subscription build() {
      checkNotNull(subscriptionId);
      checkNotNull(streamName);
      checkState(ackTimeoutSeconds > 0 && ackTimeoutSeconds < 36000);
      return new Subscription(subscriptionId, streamName, ackTimeoutSeconds);
    }
  }
}
