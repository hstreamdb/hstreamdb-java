package io.hstream;

import java.util.concurrent.CompletableFuture;

/** The interface for the HStream producer, all of methods are thread-safe */
public interface Producer {

  /**
   * Write a raw record
   *
   * @param rawRecord raw format record
   * @return the record id wrapped in a {@link CompletableFuture}
   */
  @Deprecated
  CompletableFuture<String> write(byte[] rawRecord);

  /**
   * Write a {@link HRecord}.
   *
   * @param hRecord {@link HRecord}
   * @return the record id wrapped in a {@link CompletableFuture}
   */
  @Deprecated
  CompletableFuture<String> write(HRecord hRecord);

  /**
   * Write a {@link Record}.
   *
   * @param record {@link Record}
   * @return the record id wrapped in a {@link CompletableFuture}
   */
  CompletableFuture<String> write(Record record);
}
