package io.hstream.impl;

import io.hstream.BatchSetting;
import io.hstream.BufferedProducer;
import io.hstream.BufferedProducerBuilder;
import io.hstream.FlowControlSetting;

public class BufferedProducerBuilderImpl implements BufferedProducerBuilder {

  private String streamName;
  private BatchSetting batchSetting = new BatchSetting();
  private FlowControlSetting flowControlSetting = new FlowControlSetting();

  @Override
  public BufferedProducerBuilder stream(String streamName) {
    this.streamName = streamName;
    return this;
  }

  @Override
  public BufferedProducerBuilder batchSetting(BatchSetting batchSetting) {
    this.batchSetting = batchSetting;
    return this;
  }

  @Override
  public BufferedProducerBuilder flowControlSetting(FlowControlSetting flowControlSetting) {
    this.flowControlSetting = flowControlSetting;
    return this;
  }

  @Override
  public BufferedProducer build() {
    return new BufferedProducerKtImpl(streamName, batchSetting, flowControlSetting);
  }
}
