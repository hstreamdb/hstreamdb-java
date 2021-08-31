package io.hstream;

import java.util.Objects;

/** A class for storing information about subscriptions */
public class Subscription {

  /** An identifier for the subscription */
  private String subscriptionId;
  /** The name of the stream being subscribed to */
  private String streamName;
  /** The offset that indicates the position to start consuming data from the stream */
  private SubscriptionOffset subscriptionOffset;

  /**
   * A constructor for subscriptions
   *
   * @param subscriptionId An identifier for the subscription
   * @param streamName The name of the stream being subscribed to
   * @param subscriptionOffset A {@link SubscriptionOffset} to indicate the position to start
   *     consuming data
   */
  public Subscription(
      String subscriptionId, String streamName, SubscriptionOffset subscriptionOffset) {
    this.subscriptionId = subscriptionId;
    this.streamName = streamName;
    this.subscriptionOffset = subscriptionOffset;
  }

  /** get the identifier of the subscription */
  public String getSubscriptionId() {
    return subscriptionId;
  }

  /** get the name of stream being subscribed to */
  public String getStreamName() {
    return streamName;
  }

  /** get the subscription offset */
  public SubscriptionOffset getSubscriptionOffset() {
    return subscriptionOffset;
  }

  /** update the identifier of the subscription */
  public void setSubscriptionId(String subscriptionId) {
    this.subscriptionId = subscriptionId;
  }

  /** update the name of the stream */
  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  /** update the subscription offset */
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
