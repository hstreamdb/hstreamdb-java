package io.hstream.impl;

import io.hstream.BatchSetting;
import io.hstream.BufferedProducer;
import io.hstream.BufferedProducerBuilder;
import io.hstream.FlowControlSetting;
import io.hstream.HStreamDBClientException;

public class BufferedProducerBuilderImpl implements BufferedProducerBuilder {

  private String streamName;
  private BatchSetting batchSetting;
  private FlowControlSetting flowControlSetting;

  @Override
  public BufferedProducerBuilder stream(String streamName) {
    this.streamName = streamName;
    return this;
  }

  @Override
  public BufferedProducerBuilder batchSetting(BatchSetting batchSetting) {
    return null;
  }

  @Override
  public BufferedProducerBuilder flowControlSetting(FlowControlSetting flowControlSetting) {
    return null;
  }

  @Override
  public BufferedProducer build() {
//    if (recordCountLimit < 1) {
//      throw new HStreamDBClientException(
//          String.format(
//              "build buffedProducer failed, recordCountLimit(%d) can NOT be less than 1",
//              recordCountLimit));
//    }
//    if (maxBatchSize < 1) {
//      maxBatchSize = 1;
//    }
    return new BufferedProducerKtImpl(streamName, batchSetting, flowControlSetting);
  }
}
