package io.hstream;

import static com.google.common.base.Preconditions.checkState;

public class FlowControlSetting {

  private int bytesLimit;

  public int getBytesLimit() {
    return bytesLimit;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private int bytesLimit = 104857600;

    public Builder() {}

    /**
     * Optional, the default value is 100MB, total bytes limit, including buffered batch records and
     * sending records, the value should be greater than keys size * batchSetting.bytesLimit
     *
     * @param bytesLimit total bytes limit
     * @return the FlowControlSetting Builder instance
     */
    public Builder bytesLimit(int bytesLimit) {
      this.bytesLimit = bytesLimit;
      return Builder.this;
    }

    public FlowControlSetting build() {
      checkState(bytesLimit > 0);
      return new FlowControlSetting(this);
    }
  }

  private FlowControlSetting(Builder builder) {
    this.bytesLimit = builder.bytesLimit;
  }
}
