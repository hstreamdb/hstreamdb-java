package io.hstream;

public class FlowControlSetting {

  private int bytesLimit = 1048576;

  public FlowControlSetting() {}

  public int getBytesLimit() {
    return bytesLimit;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private int bytesLimit;

    public Builder() {}

    public Builder bytesLimit(int bytesLimit) {
      this.bytesLimit = bytesLimit;
      return Builder.this;
    }

    public FlowControlSetting build() {

      return new FlowControlSetting(this);
    }
  }

  public FlowControlSetting(Builder builder) {
    this.bytesLimit = builder.bytesLimit;
  }
}
