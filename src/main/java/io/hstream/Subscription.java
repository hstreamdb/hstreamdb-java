package io.hstream;

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
}
