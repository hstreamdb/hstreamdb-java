package io.hstream;

import java.util.List;

public class GetSubscriptionResponse {
  Subscription subscription;
  List<SubscriptionOffset> offsets;

  public Subscription getSubscription() {
    return subscription;
  }

  public List<SubscriptionOffset> getOffsets() {
    return offsets;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private Subscription subscription;
    private List<SubscriptionOffset> offsets;

    public Builder subscription(Subscription subscription) {
      this.subscription = subscription;
      return this;
    }

    public Builder offsets(List<SubscriptionOffset> offsets) {
      this.offsets = offsets;
      return this;
    }

    public GetSubscriptionResponse build() {
      GetSubscriptionResponse getSubscriptionResponse = new GetSubscriptionResponse();
      getSubscriptionResponse.offsets = this.offsets;
      getSubscriptionResponse.subscription = this.subscription;
      return getSubscriptionResponse;
    }
  }
}
