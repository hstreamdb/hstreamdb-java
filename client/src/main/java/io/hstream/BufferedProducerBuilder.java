package io.hstream;

/** A builder for {@link BufferedProducer}s. */
public interface BufferedProducerBuilder {

  BufferedProducerBuilder stream(String streamName);

  BufferedProducerBuilder batchSetting(BatchSetting batchSetting);
  BufferedProducerBuilder flowControlSetting(FlowControlSetting flowControlSetting);

  BufferedProducer build();
}
