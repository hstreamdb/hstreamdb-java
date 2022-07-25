package io.hstream;

public class RecordHeader {
  private String partitionKey;

  private RecordHeader(Builder builder) {
    this.partitionKey = builder.partitionKey;
  }

  public String getPartitionKey() {
    return partitionKey;
  }

  public void setPartitionKey(String partitionKey) {
    this.partitionKey = partitionKey;
  }

  public static Builder newBuild() {
    return new Builder();
  }

  public static class Builder {

    private String partitionKey;

    public Builder partitionKey(String partitionKey) {
      this.partitionKey = partitionKey;
      return Builder.this;
    }

    public RecordHeader build() {
      return new RecordHeader(this);
    }
  }
}
