package io.hstream.impl;

import static com.google.common.base.Preconditions.*;

import io.hstream.Producer;
import io.hstream.ProducerBuilder;

public class ProducerBuilderImpl implements ProducerBuilder {

  private final HStreamClientKtImpl client;
  private String streamName;
  private long requestTimeoutMs = DefaultSettings.GRPC_CALL_TIMEOUT_MS;

  public ProducerBuilderImpl(HStreamClientKtImpl client) {
    this.client = client;
  }

  @Override
  public ProducerBuilder stream(String streamName) {
    this.streamName = streamName;
    return this;
  }

  @Override
  public ProducerBuilder requestTimeoutMs(long timeoutMs) {
    this.requestTimeoutMs = timeoutMs;
    return this;
  }

  @Override
  public Producer build() {
    checkNotNull(streamName);
    checkArgument(requestTimeoutMs > 0);
    return new ProducerKtImpl(client, streamName, requestTimeoutMs);
  }
}
