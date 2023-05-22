package io.hstream.impl;

import static com.google.common.base.Preconditions.checkArgument;

import io.hstream.*;

import java.util.UUID;

public class ConsumerBuilderImpl implements ConsumerBuilder {

  private final HStreamClientKtImpl client;
  private String name;
  private String subscription;
  private RawRecordReceiver rawRecordReceiver;
  private HRecordReceiver hRecordReceiver;
  private BatchReceiver batchReceiver;
  private int ackBufferSize = 100;
  private long ackAgeLimit = 100;

  public ConsumerBuilderImpl(HStreamClientKtImpl client) {
    this.client = client;
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
  public ConsumerBuilder batchReceiver(BatchReceiver batchReceiver) {
    this.batchReceiver = batchReceiver;
    return this;
  }

  @Override
  public ConsumerBuilder ackBufferSize(int ackBufferSize) {
    this.ackBufferSize = ackBufferSize;
    return this;
  }

  @Override
  public ConsumerBuilder ackAgeLimit(long ackAgeLimit) {
    this.ackAgeLimit = ackAgeLimit;
    return this;
  }

  @Override
  public Consumer build() {
    checkArgument(subscription != null, "ConsumerBuilder: `subscription` should not be null");
    checkArgument(
        rawRecordReceiver != null || hRecordReceiver != null || batchReceiver != null,
        "ConsumerBuilder: record(s) receiver not found");
    if (name == null) {
      name = UUID.randomUUID().toString();
    }
    checkArgument(name != null, "ConsumerBuilder: `name` should not be null");
    if (ackBufferSize < 1) {
      ackBufferSize = 1;
    }
    return new ConsumerKtImpl(
        client, name, subscription, rawRecordReceiver, hRecordReceiver, batchReceiver, ackBufferSize, ackAgeLimit);
  }
}
