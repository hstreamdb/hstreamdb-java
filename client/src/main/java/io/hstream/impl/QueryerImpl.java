package io.hstream.impl;

import com.google.common.util.concurrent.AbstractService;
import io.grpc.stub.StreamObserver;
import io.hstream.*;
import io.hstream.internal.CreateQueryStreamRequest;
import io.hstream.internal.CreateQueryStreamResponse;
import io.hstream.internal.HStreamApiGrpc;
import io.hstream.internal.Stream;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryerImpl extends AbstractService implements Queryer {

  private static final Logger logger = LoggerFactory.getLogger(QueryerImpl.class);

  private static final String STREAM_QUERY_STREAM_PREFIX = "STREAM-QUERY-";

  private static final String STREAM_QUERY_SUBSCRIPTION_PREFIX = "STREAM-QUERY-";

  private final HStreamClient client;
  private final HStreamApiGrpc.HStreamApiStub grpcStub;
  private final String sql;
  private final Observer<HRecord> resultObserver;

  private Consumer queryInnerConsumer;

  public QueryerImpl(
      HStreamClient client,
      HStreamApiGrpc.HStreamApiStub grpcStub,
      String sql,
      Observer<HRecord> resultObserver) {
    this.client = client;
    this.grpcStub = grpcStub;
    this.sql = sql;
    this.resultObserver = resultObserver;
  }

  @Override
  protected void doStart() {
    String resultStreamNameSuffix = UUID.randomUUID().toString();

    CreateQueryStreamRequest createQueryStreamRequest =
        CreateQueryStreamRequest.newBuilder()
            .setQueryStream(
                Stream.newBuilder()
                    .setStreamName(STREAM_QUERY_STREAM_PREFIX + resultStreamNameSuffix)
                    .setReplicationFactor(3)
                    .build())
            .setQueryStatements(sql)
            .build();
    grpcStub.createQueryStream(
        createQueryStreamRequest,
        new StreamObserver<CreateQueryStreamResponse>() {
          @Override
          public void onNext(CreateQueryStreamResponse value) {
            logger.info(
                "query [{}] created, related result stream is [{}]",
                value.getStreamQuery().getId(),
                value.getQueryStream().getStreamName());

            client.createSubscription(
                new Subscription(
                    STREAM_QUERY_SUBSCRIPTION_PREFIX + resultStreamNameSuffix,
                    STREAM_QUERY_STREAM_PREFIX + resultStreamNameSuffix,
                    new SubscriptionOffset(SubscriptionOffset.SpecialOffset.EARLIEST)));

            queryInnerConsumer =
                client
                    .newConsumer()
                    .subscription(STREAM_QUERY_SUBSCRIPTION_PREFIX + resultStreamNameSuffix)
                    .hRecordReceiver(
                        (receivedHRecord, responder) -> {
                          try {
                            resultObserver.onNext(receivedHRecord.getHRecord());
                            responder.ack();
                          } catch (Throwable t) {
                            resultObserver.onError(t);
                          }
                        })
                    .build();
            queryInnerConsumer.startAsync().awaitRunning();

            notifyStarted();
          }

          @Override
          public void onError(Throwable t) {
            logger.error("creating stream query happens error: ", t);
            notifyFailed(t);
          }

          @Override
          public void onCompleted() {}
        });
  }

  @Override
  protected void doStop() {
    new Thread(
            () -> {
              queryInnerConsumer.stopAsync().awaitTerminated();
              notifyStopped();
            })
        .start();
  }
}
