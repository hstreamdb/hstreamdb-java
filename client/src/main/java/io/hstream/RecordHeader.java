package io.hstream;

public class RecordHeader {
  private String orderingKey;

  private RecordHeader(Builder builder) {
    this.orderingKey = builder.orderingKey;
  }

  public String getOrderingKey() {
    return orderingKey;
  }

  public void setOrderingKey(String orderingKey) {
    this.orderingKey = orderingKey;
  }

  public static Builder newBuild() {
    return new Builder();
  }

  public static class Builder {

    private String orderingKey;

    public Builder orderingKey(String orderingKey) {
      this.orderingKey = orderingKey;
      return Builder.this;
    }

    public RecordHeader build() {
      return new RecordHeader(this);
    }
  }
}
