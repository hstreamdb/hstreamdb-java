package io.hstream;

public class BatchSetting {

  private int recordCountLimit;
  private int bytesLimit;
  private long ageLimit;

  public int getRecordCountLimit() {
    return recordCountLimit;
  }

  public int getBytesLimit() {
    return bytesLimit;
  }

  public long getAgeLimit() {
    return ageLimit;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private int recordCountLimit = 100;
    private int bytesLimit = 4096;
    private long ageLimit = 100;

    public Builder() {}

    /**
     * @param recordCountLimit optional, default: 100, disabled if recordCountLimit {@literal <=} 0
     * @return the {@link BatchSetting.Builder} instance
     */
    public Builder recordCountLimit(int recordCountLimit) {
      this.recordCountLimit = recordCountLimit;
      return Builder.this;
    }

    /**
     * @param bytesLimit optional, default: 4096(Bytes), disabled if bytesLimit {@literal <=} 0
     * @return the {@link BatchSetting.Builder} instance
     */
    public Builder bytesLimit(int bytesLimit) {
      this.bytesLimit = bytesLimit;
      return Builder.this;
    }

    /**
     * @param ageLimit optional, default: 100(ms), disabled if ageLimit {@literal <=} 0
     * @return the {@link BatchSetting.Builder} instance
     */
    public Builder ageLimit(long ageLimit) {
      this.ageLimit = ageLimit;
      return Builder.this;
    }

    /**
     * There should exist more than one enabled option, otherwise throws a {@link
     * HStreamDBClientException} exception.
     *
     * @return the {@link BatchSetting} instance
     */
    public BatchSetting build() {
      if (recordCountLimit < 1 && bytesLimit < 1 && ageLimit < 1) {
        throw new HStreamDBClientException(
            "disabled all options, at least one option should be enabled");
      }
      return new BatchSetting(this);
    }
  }

  private BatchSetting(Builder builder) {
    this.recordCountLimit = builder.recordCountLimit;
    this.bytesLimit = builder.bytesLimit;
    this.ageLimit = builder.ageLimit;
  }
}
