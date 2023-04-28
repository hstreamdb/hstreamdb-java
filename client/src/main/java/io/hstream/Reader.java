package io.hstream;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/** The interface for the HStream shard reader */
@Deprecated
public interface Reader extends AutoCloseable {

  /**
   * Read {@link ReceivedRecord}s from a stream shard.
   *
   * @param maxRecords the max number of the returned records
   * @return the {@link ReceivedRecord}s wrapped in a {@link CompletableFuture}
   */
  CompletableFuture<List<ReceivedRecord>> read(int maxRecords);
}
