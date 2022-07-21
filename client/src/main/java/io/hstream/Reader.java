package io.hstream;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/** The interface for the HStream shard reader */
public interface Reader extends AutoCloseable {

  /**
   * Read {@link Record}s from a stream shard.
   *
   * @param maxRecords the max number of the returned records
   * @return the {@link Record}s wrapped in a {@link CompletableFuture}
   */
  CompletableFuture<List<Record>> read(int maxRecords);
}
