package io.hstream.impl;

import io.grpc.stub.StreamObserver;
import io.hstream.*;
import io.hstream.RecordId;
import io.hstream.internal.*;
import io.hstream.util.GrpcUtils;
import io.hstream.util.RecordUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerImpl implements Producer {

  private static final Logger logger = LoggerFactory.getLogger(ProducerImpl.class);

  private final List<String> serverUrls;
  private final ChannelProvider channelProvider;
  private final String stream;
  private final boolean enableBatch;
  private final int recordCountLimit;

  private final Semaphore semaphore;
  private final Lock lock;
  private final List<HStreamRecord> recordBuffer;
  private final List<CompletableFuture<RecordId>> futures;

  private HStreamApiGrpc.HStreamApiStub appendStub;

  public ProducerImpl(
      List<String> serverUrls,
      ChannelProvider channelProvider,
      String stream,
      boolean enableBatch,
      int recordCountLimit) {

    this.serverUrls = serverUrls;
    this.channelProvider = channelProvider;
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

    appendStub = createAppendStub();
  }

  private synchronized HStreamApiGrpc.HStreamApiStub createAppendStub() {
    ServerNode serverNode =
        HStreamApiGrpc.newBlockingStub(channelProvider.get(serverUrls.get(0)))
            .lookupStream(LookupStreamRequest.newBuilder().setStreamName(stream).build())
            .getServerNode();

    String serverUrl = serverNode.getHost() + ":" + serverNode.getPort();
    return HStreamApiGrpc.newStub(channelProvider.get(serverUrl));
  }

  @Override
  public CompletableFuture<RecordId> write(byte[] rawRecord) {
    HStreamRecord hStreamRecord = RecordUtils.buildHStreamRecordFromRawRecord(rawRecord);
    return writeInternal(hStreamRecord);
  }

  @Override
  public CompletableFuture<RecordId> write(HRecord hRecord) {
    HStreamRecord hStreamRecord = RecordUtils.buildHStreamRecordFromHRecord(hRecord);
    return writeInternal(hStreamRecord);
  }

  private CompletableFuture<RecordId> writeInternal(HStreamRecord hStreamRecord) {
    if (!enableBatch) {
      CompletableFuture<RecordId> future = new CompletableFuture<>();
      writeHStreamRecords(List.of(hStreamRecord))
          .handle(
              (recordIds, exception) -> {
                if (exception == null) {
                  future.complete(recordIds.get(0));
                } else {
                  future.completeExceptionally(exception);
                }
                return null;
              });
      return future;
    } else {
      return addToBuffer(hStreamRecord);
    }
  }

  private void flush() {
    lock.lock();
    try {
      if (recordBuffer.isEmpty()) {
        return;
      } else {
        final int recordBufferCount = recordBuffer.size();

        logger.info("start flush recordBuffer, current buffer size is: {}", recordBufferCount);

        writeHStreamRecords(recordBuffer)
            .handle(
                (recordIds, exception) -> {
                  if (exception == null) {
                    for (int i = 0; i < recordIds.size(); ++i) {
                      futures.get(i).complete(recordIds.get(i));
                    }
                  } else {
                    for (int i = 0; i < futures.size(); ++i) {
                      futures.get(i).completeExceptionally(exception);
                    }
                  }
                  return null;
                })
            .join();

        recordBuffer.clear();
        futures.clear();

        logger.info("finish clearing record buffer");

        semaphore.release(recordBufferCount);
      }
    } finally {
      lock.unlock();
    }
  }

  private CompletableFuture<List<RecordId>> writeHStreamRecords(
      List<HStreamRecord> hStreamRecords) {
    CompletableFuture<List<RecordId>> completableFuture = new CompletableFuture<>();

    AppendRequest appendRequest =
        AppendRequest.newBuilder().setStreamName(stream).addAllRecords(hStreamRecords).build();

    StreamObserver<AppendResponse> streamObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(AppendResponse appendResponse) {
            completableFuture.complete(
                appendResponse.getRecordIdsList().stream()
                    .map(GrpcUtils::recordIdFromGrpc)
                    .collect(Collectors.toList()));
          }

          @Override
          public void onError(Throwable t) {
            logger.warn("write records error: ", t);
            completableFuture.completeExceptionally(new HStreamDBClientException(t));
          }

          @Override
          public void onCompleted() {}
        };

    appendStub.append(appendRequest, streamObserver);

    return completableFuture;
  }

  private CompletableFuture<RecordId> addToBuffer(HStreamRecord hStreamRecord) {
    try {
      semaphore.acquire();
    } catch (InterruptedException e) {
      throw new HStreamDBClientException(e);
    }

    lock.lock();
    try {
      CompletableFuture<RecordId> completableFuture = new CompletableFuture<>();
      recordBuffer.add(hStreamRecord);
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
