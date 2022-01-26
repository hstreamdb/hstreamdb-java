package io.hstream.impl;

import static com.google.common.base.Preconditions.*;

import io.hstream.Producer;
import io.hstream.ProducerBuilder;

public class ProducerBuilderImpl implements ProducerBuilder {

  private String streamName;

  @Override
  public ProducerBuilder stream(String streamName) {
    this.streamName = streamName;
    return this;
  }

  @Override
  public Producer build() {
    checkNotNull(streamName);
    return new ProducerKtImpl(streamName);
  }
}
