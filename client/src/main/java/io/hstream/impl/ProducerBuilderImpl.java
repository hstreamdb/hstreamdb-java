package io.hstream.impl;

import static com.google.common.base.Preconditions.*;

import io.hstream.Producer;
import io.hstream.ProducerBuilder;

public class ProducerBuilderImpl implements ProducerBuilder {

  private final HStreamClientKtImpl client;
  private String streamName;

  public ProducerBuilderImpl(HStreamClientKtImpl client) {
    this.client = client;
  }

  @Override
  public ProducerBuilder stream(String streamName) {
    this.streamName = streamName;
    return this;
  }

  @Override
  public Producer build() {
    checkNotNull(streamName);
    return new ProducerKtImpl(client, streamName);
  }
}
