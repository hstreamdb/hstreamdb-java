package io.hstream.impl;

import io.hstream.BatchSetting;
import io.hstream.BufferedProducer;
import io.hstream.BufferedProducerBuilder;
import io.hstream.FlowControlSetting;
import io.hstream.HStreamDBClientException;

public class BufferedProducerBuilderImpl implements BufferedProducerBuilder {

  private HStreamClientKtImpl client;
  private String streamName;
  private BatchSetting batchSetting = BatchSetting.newBuilder().build();
  private FlowControlSetting flowControlSetting = FlowControlSetting.newBuilder().build();

  public BufferedProducerBuilderImpl(HStreamClientKtImpl client) {
    this.client = client;
  }

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
    if (streamName == null) {
      throw new HStreamDBClientException("Positional option:[stream] is not set");
    }
    var batchBytes = batchSetting.getBytesLimit();
    var flowBytes = flowControlSetting.getBytesLimit();
    if (batchBytes > 0 && flowBytes > 0 && batchBytes > flowBytes) {
      throw new HStreamDBClientException(
          String.format(
              "BatchSetting.ageLimit:[%d] should not be greater than flowControlSetting.bytesLimit:[%d]",
              batchBytes, flowBytes));
    }
    return new BufferedProducerKtImpl(client, streamName, batchSetting, flowControlSetting);
  }
}
