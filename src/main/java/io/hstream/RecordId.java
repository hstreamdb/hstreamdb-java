package io.hstream;

import java.util.Objects;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RecordId recordId = (RecordId) o;
    return batchId == recordId.batchId && batchIndex == recordId.batchIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(batchId, batchIndex);
  }
}
