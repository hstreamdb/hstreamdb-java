package io.hstream;

import io.hstream.impl.ProducerImpl;
import io.hstream.internal.HStreamApiGrpc;

/** used to construct a producer */
public class ProducerBuilder {

  private HStreamApiGrpc.HStreamApiStub grpcStub;

  private String streamName;

  private boolean enableBatch = false;

  private int recordCountLimit = 1;

  public ProducerBuilder(HStreamApiGrpc.HStreamApiStub stub) {
    this.grpcStub = stub;
  }

  public ProducerBuilder stream(String streamName) {
    this.streamName = streamName;
    return this;
  }

  public ProducerBuilder enableBatch() {
    this.enableBatch = true;
    return this;
  }

  public ProducerBuilder recordCountLimit(int recordCountLimit) {
    this.recordCountLimit = recordCountLimit;
    return this;
  }

  public Producer build() {
    return new ProducerImpl(grpcStub, streamName, enableBatch, recordCountLimit);
  }
}
