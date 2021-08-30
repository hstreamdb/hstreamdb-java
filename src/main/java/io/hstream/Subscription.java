package io.hstream;

import java.util.Objects;

public class Subscription {

  private String subscriptionId;
  private String streamName;
  private SubscriptionOffset subscriptionOffset;

  public Subscription(
      String subscriptionId, String streamName, SubscriptionOffset subscriptionOffset) {
    this.subscriptionId = subscriptionId;
    this.streamName = streamName;
    this.subscriptionOffset = subscriptionOffset;
  }

  public String getSubscriptionId() {
    return subscriptionId;
  }

  public String getStreamName() {
    return streamName;
  }

  public SubscriptionOffset getSubscriptionOffset() {
    return subscriptionOffset;
  }

  public void setSubscriptionId(String subscriptionId) {
    this.subscriptionId = subscriptionId;
  }

  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  public void setSubscriptionOffset(SubscriptionOffset subscriptionOffset) {
    this.subscriptionOffset = subscriptionOffset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Subscription that = (Subscription) o;
    return Objects.equals(subscriptionId, that.subscriptionId)
        && Objects.equals(streamName, that.streamName)
        && Objects.equals(subscriptionOffset, that.subscriptionOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subscriptionId, streamName, subscriptionOffset);
  }
}
