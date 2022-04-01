package io.hstream;

public class FlowControlSetting {

  private int bytesLimit = 104857600;

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

      return new FlowControlSetting(this);
    }
  }

  public FlowControlSetting(Builder builder) {
    this.bytesLimit = builder.bytesLimit;
  }
}
