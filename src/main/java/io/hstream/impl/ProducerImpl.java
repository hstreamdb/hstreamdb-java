package io.hstream.impl;

import io.grpc.stub.StreamObserver;
import io.hstream.*;
import io.hstream.util.RecordUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerImpl implements Producer {

  private static final Logger logger = LoggerFactory.getLogger(ProducerImpl.class);

  private final HStreamApiGrpc.HStreamApiStub grpcStub;
  private final String stream;
  private final boolean enableBatch;
  private final int recordCountLimit;

  private final Semaphore semaphore;
  private final Lock lock;
  private final List<Object> recordBuffer;
  private final List<CompletableFuture<RecordId>> futures;

  public ProducerImpl(
      HStreamApiGrpc.HStreamApiStub stub,
      String stream,
      boolean enableBatch,
      int recordCountLimit) {
    this.grpcStub = stub;
    this.stream = stream;
    this.enableBatch = enableBatch;
    this.recordCountLimit = recordCountLimit;

    if (enableBatch) {
      this.semaphore = new Semaphore(recordCountLimit);
      this.lock = new ReentrantLock();
      this.recordBuffer = new ArrayList<>(recordCountLimit);
      this.futures = new ArrayList<>(recordCountLimit);
    } else {
      this.semaphore = null;
      this.lock = null;
      this.recordBuffer = null;
      this.futures = null;
    }
  }

  @Override
  public RecordId write(byte[] rawRecord) {
    CompletableFuture<List<RecordId>> future = writeRawRecordsAsync(List.of(rawRecord));
    return future.join().get(0);
  }

  @Override
  public RecordId write(HRecord hRecord) {
    CompletableFuture<List<RecordId>> future = writeHRecordsAsync(List.of(hRecord));
    return future.join().get(0);
  }

  @Override
  public CompletableFuture<RecordId> writeAsync(byte[] rawRecord) {
    if (!enableBatch) {
      return writeRawRecordsAsync(List.of(rawRecord)).thenApply(list -> list.get(0));
    } else {
      try {
        semaphore.acquire();
      } catch (InterruptedException e) {
        throw new HStreamDBClientException(e);
      }

      lock.lock();
      try {
        CompletableFuture<RecordId> completableFuture = new CompletableFuture<>();
        recordBuffer.add(rawRecord);
        futures.add(completableFuture);

        if (recordBuffer.size() == recordCountLimit) {
          flush();
        }
        return completableFuture;
      } finally {
        lock.unlock();
      }
    }
  }

  @Override
  public CompletableFuture<RecordId> writeAsync(HRecord hRecord) {
    if (!enableBatch) {
      return writeHRecordsAsync(List.of(hRecord)).thenApply(list -> list.get(0));
    } else {
      try {
        semaphore.acquire();
      } catch (InterruptedException e) {
        throw new HStreamDBClientException(e);
      }

      lock.lock();
      try {
        CompletableFuture<RecordId> completableFuture = new CompletableFuture<>();
        recordBuffer.add(hRecord);
        futures.add(completableFuture);

        if (recordBuffer.size() == recordCountLimit) {
          flush();
        }
        return completableFuture;
      } finally {
        lock.unlock();
      }
    }
  }

  @Override
  public void flush() {
    flushSync();
  }

  private CompletableFuture<List<RecordId>> writeRawRecordsAsync(List<byte[]> rawRecords) {

    CompletableFuture<List<RecordId>> completableFuture = new CompletableFuture<>();

    AppendRequest appendRequest =
        AppendRequest.newBuilder()
            .setStreamName(this.stream)
            .addAllRecords(
                rawRecords.stream()
                    .map(rawRecord -> RecordUtils.buildHStreamRecordFromRawRecord(rawRecord))
                    .collect(Collectors.toList()))
            .build();

    StreamObserver<AppendResponse> streamObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(AppendResponse appendResponse) {
            completableFuture.complete(appendResponse.getRecordIdsList());
          }

          @Override
          public void onError(Throwable t) {
            throw new HStreamDBClientException(t);
          }

          @Override
          public void onCompleted() {}
        };

    grpcStub.append(appendRequest, streamObserver);

    return completableFuture;
  }

  private CompletableFuture<List<RecordId>> writeHRecordsAsync(List<HRecord> hRecords) {
    CompletableFuture<List<RecordId>> completableFuture = new CompletableFuture<>();

    AppendRequest appendRequest =
        AppendRequest.newBuilder()
            .setStreamName(this.stream)
            .addAllRecords(
                hRecords.stream()
                    .map(hRecord -> RecordUtils.buildHStreamRecordFromHRecord(hRecord))
                    .collect(Collectors.toList()))
            .build();

    StreamObserver<AppendResponse> streamObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(AppendResponse appendResponse) {
            completableFuture.complete(appendResponse.getRecordIdsList());
          }

          @Override
          public void onError(Throwable t) {
            throw new HStreamDBClientException(t);
          }

          @Override
          public void onCompleted() {}
        };

    grpcStub.append(appendRequest, streamObserver);

    return completableFuture;
  }

  private void flushSync() {
    lock.lock();
    try {
      if (recordBuffer.isEmpty()) {
        return;
      } else {
        final int recordBufferCount = recordBuffer.size();

        logger.info("start flush recordBuffer, current buffer size is: {}", recordBufferCount);

        List<ImmutablePair<Integer, byte[]>> rawRecords =
            IntStream.range(0, recordBufferCount)
                .filter(index -> recordBuffer.get(index) instanceof byte[])
                .mapToObj(index -> ImmutablePair.of(index, (byte[]) (recordBuffer.get(index))))
                .collect(Collectors.toList());

        List<ImmutablePair<Integer, HRecord>> hRecords =
            IntStream.range(0, recordBufferCount)
                .filter(index -> recordBuffer.get(index) instanceof HRecord)
                .mapToObj(index -> ImmutablePair.of(index, (HRecord) (recordBuffer.get(index))))
                .collect(Collectors.toList());

        List<RecordId> rawRecordIds =
            writeRawRecordsAsync(
                    rawRecords.stream().map(pair -> pair.getRight()).collect(Collectors.toList()))
                .join();
        List<RecordId> hRecordIds =
            writeHRecordsAsync(
                    hRecords.stream().map(ImmutablePair::getRight).collect(Collectors.toList()))
                .join();

        IntStream.range(0, rawRecords.size())
            .mapToObj(i -> ImmutablePair.of(i, rawRecords.get(i).getLeft()))
            .forEach(p -> futures.get(p.getRight()).complete(rawRecordIds.get(p.getLeft())));

        IntStream.range(0, hRecords.size())
            .mapToObj(i -> ImmutablePair.of(i, hRecords.get(i).getLeft()))
            .forEach(p -> futures.get(p.getRight()).complete(hRecordIds.get(p.getLeft())));

        recordBuffer.clear();
        futures.clear();

        logger.info("finish clearing record buffer");

        semaphore.release(recordBufferCount);
      }
    } finally {
      lock.unlock();
    }
  }
}
