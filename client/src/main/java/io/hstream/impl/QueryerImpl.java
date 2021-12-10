package io.hstream.impl;

import com.google.common.util.concurrent.AbstractService;
import io.grpc.stub.StreamObserver;
import io.hstream.*;
import io.hstream.internal.CreateQueryStreamRequest;
import io.hstream.internal.CreateQueryStreamResponse;
import io.hstream.internal.HStreamApiGrpc;
import io.hstream.internal.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class QueryerImpl extends AbstractService implements Queryer {

    private static final Logger logger = LoggerFactory.getLogger(QueryerImpl.class);

    private static final String STREAM_QUERY_STREAM_PREFIX = "STREAM-QUERY-";

    private static final String STREAM_QUERY_SUBSCRIPTION_PREFIX = "STREAM-QUERY-";

    private final HStreamClient client;
    private final List<String> serverUrls;
    private final ChannelProvider channelProvider;

    private HStreamApiGrpc.HStreamApiStub queryStub;
    private final String sql;
    private final Observer<HRecord> resultObserver;

    private Consumer queryInnerConsumer;

    public QueryerImpl(
            HStreamClient client,
            List<String> serverUrls,
            ChannelProvider channelProvider,
            String sql,
            Observer<HRecord> resultObserver) {
        this.client = client;
        this.serverUrls = serverUrls;
        this.channelProvider = channelProvider;
        this.sql = sql;
        this.resultObserver = resultObserver;

        queryStub = createQueryStub();
    }

    private HStreamApiGrpc.HStreamApiStub createQueryStub() {
        return HStreamApiGrpc.newStub(channelProvider.get(serverUrls.get(0)));
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
        queryStub.createQueryStream(
                createQueryStreamRequest,
                new StreamObserver<CreateQueryStreamResponse>() {
                    @Override
                    public void onNext(CreateQueryStreamResponse value) {
                        logger.info(
                                "query [{}] created, related result stream is [{}]",
                                value.getStreamQuery().getId(),
                                value.getQueryStream().getStreamName());

                        Subscription subscription =
                                Subscription.newBuilder()
                                        .subscription(STREAM_QUERY_SUBSCRIPTION_PREFIX + resultStreamNameSuffix)
                                        .stream(STREAM_QUERY_STREAM_PREFIX + resultStreamNameSuffix)
                                        .ackTimeoutSeconds(10)
                                        .build();
                        client.createSubscription(subscription);

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
                    public void onCompleted() {
                    }
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
