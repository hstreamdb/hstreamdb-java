package io.hstream.impl;

import io.hstream.Consumer;
import io.hstream.ConsumerBuilder;
import io.hstream.HRecordReceiver;
import io.hstream.RawRecordReceiver;
import io.hstream.internal.HStreamApiGrpc;

public class ConsumerBuilderImpl implements ConsumerBuilder {

  private HStreamApiGrpc.HStreamApiStub grpcStub;
  private HStreamApiGrpc.HStreamApiBlockingStub grpcBlockingStub;
  private String name;
  private String subscription;
  private RawRecordReceiver rawRecordReceiver;
  private HRecordReceiver hRecordReceiver;

  public ConsumerBuilderImpl(
      HStreamApiGrpc.HStreamApiStub grpcStub,
      HStreamApiGrpc.HStreamApiBlockingStub grpcBlockingStub) {
    this.grpcStub = grpcStub;
    this.grpcBlockingStub = grpcBlockingStub;
  }

  @Override
  public ConsumerBuilder name(String name) {
    this.name = name;
    return this;
  }

  @Override
  public ConsumerBuilder subscription(String subscription) {
    this.subscription = subscription;
    return this;
  }

  @Override
  public ConsumerBuilder rawRecordReceiver(RawRecordReceiver rawRecordReceiver) {
    this.rawRecordReceiver = rawRecordReceiver;
    return this;
  }

  @Override
  public ConsumerBuilder hRecordReceiver(HRecordReceiver hRecordReceiver) {
    this.hRecordReceiver = hRecordReceiver;
    return this;
  }

  @Override
  public Consumer build() {
    return new ConsumerImpl(
        grpcStub, grpcBlockingStub, name, subscription, rawRecordReceiver, hRecordReceiver);
  }
}
