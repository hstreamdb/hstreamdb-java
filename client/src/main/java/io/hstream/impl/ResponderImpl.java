package io.hstream.impl;

import io.grpc.stub.StreamObserver;
import io.hstream.Responder;
import io.hstream.internal.RecordId;
import io.hstream.internal.StreamingFetchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponderImpl implements Responder {
  private static final Logger logger = LoggerFactory.getLogger(ResponderImpl.class);

  private final String subscriptionId;
  private final String consumerId;
  private final RecordId recordId;
  private final StreamObserver<StreamingFetchRequest> requestStream;

  public ResponderImpl(
      String subscriptionId,
      StreamObserver<StreamingFetchRequest> requestStream,
      String consumerId,
      RecordId recordId) {
    this.subscriptionId = subscriptionId;
    this.requestStream = requestStream;
    this.consumerId = consumerId;
    this.recordId = recordId;
  }

  @Override
  public void ack() {
    StreamingFetchRequest request =
        StreamingFetchRequest.newBuilder()
            .setSubscriptionId(subscriptionId)
            .addAckedRecordIds(recordId)
            .build();
    requestStream.onNext(request);
  }
}
