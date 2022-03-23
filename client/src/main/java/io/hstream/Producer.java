package io.hstream;

import java.util.concurrent.CompletableFuture;

/** The interface for the HStream producer. */
public interface Producer {

  /**
   * Write a raw record.
   *
   * @param rawRecord raw format record
   * @return the {@link String} wrapped in a {@link CompletableFuture}
   */
  @Deprecated
  CompletableFuture<String> write(byte[] rawRecord);

  /**
   * Write a {@link HRecord}.
   *
   * @param hRecord {@link HRecord}
   * @return the {@link String} wrapped in a {@link CompletableFuture}
   */
  @Deprecated
  CompletableFuture<String> write(HRecord hRecord);

  /**
   * Write a {@link Record}.
   *
   * @param record {@link Record}
   * @return the {@link String} wrapped in a {@link CompletableFuture}
   */
  CompletableFuture<String> write(Record record);
}
