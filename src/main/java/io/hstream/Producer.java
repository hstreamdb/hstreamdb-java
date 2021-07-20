package io.hstream;

import java.util.concurrent.CompletableFuture;

public interface Producer {

  RecordId write(byte[] rawRecord);

  RecordId write(HRecord hRecord);

  CompletableFuture<RecordId> writeAsync(byte[] rawRecord);

  CompletableFuture<RecordId> writeAsync(HRecord hRecord);

  void flush();
}
