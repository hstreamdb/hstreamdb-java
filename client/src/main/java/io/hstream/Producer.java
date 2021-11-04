package io.hstream;

import java.util.concurrent.CompletableFuture;

/** The interface for the HStream producer. */
public interface Producer {

  /**
   * Write a raw record.
   *
   * @param rawRecord raw format record
   * @return the {@link RecordId} wrapped in a {@link CompletableFuture}
   */
  CompletableFuture<RecordId> write(byte[] rawRecord);

  /**
   * Write a {@link HRecord}.
   *
   * @param hRecord {@link HRecord}
   * @return the {@link RecordId} wrapped in a {@link CompletableFuture}
   */
  CompletableFuture<RecordId> write(HRecord hRecord);
}
