package io.hstream;

import java.util.concurrent.CompletableFuture;

/** the interface of hstream producer */
public interface Producer {

  /**
   * Sync method to generate a raw format message.
   *
   * @param rawRecord raw format message.
   * @return the {@link RecordId} of generated message.
   */
  RecordId write(byte[] rawRecord);

  /**
   * Sync method to generate a {@link HRecord} format message.
   *
   * @param hRecord HRecord format message.
   * @return the {@link RecordId} of generated message.
   */
  RecordId write(HRecord hRecord);

  /**
   * Async method to generate a raw format message.
   *
   * @param rawRecord raw format message.
   * @return the {@link RecordId} of generated message which wrapped in a {@link CompletableFuture}
   *     object.
   */
  CompletableFuture<RecordId> writeAsync(byte[] rawRecord);

  /**
   * Async method to generate a {@link HRecord} format message.
   *
   * @param hRecord HRecord format message.
   * @return the {@link RecordId} of generated message which wrapped in a {@link CompletableFuture}
   *     object.
   */
  CompletableFuture<RecordId> writeAsync(HRecord hRecord);

  /** Flush buffed message. */
  void flush();
}
