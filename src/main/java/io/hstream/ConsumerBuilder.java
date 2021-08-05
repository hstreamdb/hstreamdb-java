package io.hstream;

import io.hstream.impl.ConsumerImpl;

/** used to construct a consumer */
public class ConsumerBuilder {

  private HStreamApiGrpc.HStreamApiStub grpcStub;
  private HStreamApiGrpc.HStreamApiBlockingStub grpcBlockingStub;
  private String name;
  private String subscription;
  private RawRecordReceiver rawRecordReceiver;
  private HRecordReceiver hRecordReceiver;

  public ConsumerBuilder(
      HStreamApiGrpc.HStreamApiStub grpcStub,
      HStreamApiGrpc.HStreamApiBlockingStub grpcBlockingStub) {
    this.grpcStub = grpcStub;
    this.grpcBlockingStub = grpcBlockingStub;
  }

  public ConsumerBuilder name(String name) {
    this.name = name;
    return this;
  }

  public ConsumerBuilder subscription(String subscription) {
    this.subscription = subscription;
    return this;
  }

  public ConsumerBuilder rawRecordReceiver(RawRecordReceiver rawRecordReceiver) {
    this.rawRecordReceiver = rawRecordReceiver;
    return this;
  }

  public ConsumerBuilder hRecordReceiver(HRecordReceiver hRecordReceiver) {
    this.hRecordReceiver = hRecordReceiver;
    return this;
  }

  public Consumer build() {
    return new ConsumerImpl(
        grpcStub, grpcBlockingStub, name, subscription, rawRecordReceiver, hRecordReceiver);
  }
}
