package io.hstream;

public class RecordId {
  private final io.hstream.internal.RecordId rep;

  public RecordId(io.hstream.internal.RecordId rep) {
    this.rep = rep;
  }

  public RecordId(long batchId, int batchIndex) {
    this.rep =
        io.hstream.internal.RecordId.newBuilder()
            .setBatchId(batchId)
            .setBatchIndex(batchIndex)
            .build();
  }

  public io.hstream.internal.RecordId getRep() {
    return this.rep;
  }

  public static RecordId RecordIdFromGrpc(io.hstream.internal.RecordId recordId) {
    return new RecordId(recordId);
  }

  public long getBatchId() {
    return this.rep.getBatchId();
  }

  public int getBatchIndex() {
    return this.rep.getBatchIndex();
  }
}
