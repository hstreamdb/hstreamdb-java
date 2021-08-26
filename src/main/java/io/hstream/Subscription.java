package io.hstream;

public class Subscription {

  private final io.hstream.internal.Subscription rep;

  public Subscription(io.hstream.internal.Subscription rep) {
    this.rep = rep;
  }

  public Subscription(String subscriptionId, String streamName, SubscriptionOffset offset) {
    this.rep =
        io.hstream.internal.Subscription.newBuilder()
            .setSubscriptionId(subscriptionId)
            .setStreamName(streamName)
            .setOffset(offset.getRep())
            .build();
  }

  public static Subscription subscriptionFromGrpc(io.hstream.internal.Subscription subscription) {
    return new Subscription(subscription);
  }

  public io.hstream.internal.Subscription getRep() {
    return this.rep;
  }

  public String getSubscriptionId() {
    return this.rep.getSubscriptionId();
  }

  public String getStreamName() {
    return this.rep.getStreamName();
  }

  public SubscriptionOffset getOffset() {
    return SubscriptionOffset.subscriptionOffsetFromGrpc(this.rep.getOffset());
  }
}
