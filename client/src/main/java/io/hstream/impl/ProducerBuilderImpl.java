package io.hstream.impl;

import static com.google.common.base.Preconditions.*;

import io.hstream.Producer;
import io.hstream.ProducerBuilder;
import io.hstream.internal.HStreamApiGrpc;

public class ProducerBuilderImpl implements ProducerBuilder {

  private HStreamApiGrpc.HStreamApiStub grpcStub;

  private String streamName;

  private boolean enableBatch = false;

  private int recordCountLimit = 1;

  public ProducerBuilderImpl(HStreamApiGrpc.HStreamApiStub stub) {
    this.grpcStub = stub;
  }

  @Override
  public ProducerBuilder stream(String streamName) {
    this.streamName = streamName;
    return this;
  }

  @Override
  public ProducerBuilder enableBatch() {
    this.enableBatch = true;
    return this;
  }

  @Override
  public ProducerBuilder recordCountLimit(int recordCountLimit) {
    this.recordCountLimit = recordCountLimit;
    return this;
  }

  @Override
  public Producer build() {
    checkNotNull(grpcStub);
    checkNotNull(streamName);
    return new ProducerImpl(grpcStub, streamName, enableBatch, recordCountLimit);
  }
}
