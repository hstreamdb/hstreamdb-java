package io.hstream;

import java.util.Objects;

/** An object represents the id of received record. */
public class RecordId implements Comparable<RecordId> {

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

  @Override
  public String toString() {
    return "RecordId{" + "batchId=" + batchId + ", batchIndex=" + batchIndex + '}';
  }

  @Override
  public int compareTo(RecordId o) {
    if (batchId == o.batchId) {
      return Integer.compare(batchIndex, o.batchIndex);
    } else {
      return Long.compare(batchId, o.batchId);
    }
  }
}
