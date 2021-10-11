package io.hstream;

import java.util.Objects;

/** A class for storing identification information of data records */
public class RecordId {

  /** An identification number for the data record that is unique among all the data records */
  private long batchId;
  /** An index number identifies the data record in its own batch */
  private int batchIndex;

  /**
   * A constructor for RecordId
   *
   * @param batchId the unique identification number of the data record in the database
   * @param batchIndex the index of the data record in its own batch
   */
  public RecordId(long batchId, int batchIndex) {
    this.batchId = batchId;
    this.batchIndex = batchIndex;
  }

  /** get the unique identification number of the data record */
  public long getBatchId() {
    return batchId;
  }

  /** get the index of the data record in its own batch */
  public int getBatchIndex() {
    return batchIndex;
  }

  /** update the identification number */
  public void setBatchId(long batchId) {
    this.batchId = batchId;
  }

  /** update the data record index */
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
}
