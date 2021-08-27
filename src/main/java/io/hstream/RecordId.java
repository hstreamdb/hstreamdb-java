package io.hstream;

public class RecordId {

  private long batchId;
  private int batchIndex;

  public RecordId(long batchId, int batchIndex) {
    this.batchId = batchId;
    this.batchIndex = batchIndex;
  }

  public long getBatchId() {
    return batchId;
  }

  public int getBatchIndex() {
    return batchIndex;
  }

  public void setBatchId(long batchId) {
    this.batchId = batchId;
  }

  public void setBatchIndex(int batchIndex) {
    this.batchIndex = batchIndex;
  }
}
