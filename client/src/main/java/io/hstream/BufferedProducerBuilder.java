package io.hstream;

/** A builder for {@link BufferedProducer}s. */
public interface BufferedProducerBuilder {

  BufferedProducerBuilder stream(String streamName);

  /**
   * BufferedProducer will buffer records for each shard as a batch to send to servers, so {@link
   * BatchSetting} is to control the batch buffer.
   *
   * @param batchSetting setting for batch buffer
   * @return the BufferedProducerBuilder instance
   */
  BufferedProducerBuilder batchSetting(BatchSetting batchSetting);

  /**
   * {@link FlowControlSetting} is to control total records(including buffered batch records for all
   * keys and sending records) through {@link BufferedProducer}.
   *
   * @param flowControlSetting setting for flow control
   * @return the BufferedProducerBuilder instance
   */
  BufferedProducerBuilder flowControlSetting(FlowControlSetting flowControlSetting);

  /**
   * Set the compression type.
   *
   * @param compressionType type of compression
   * @return the BufferedProducerBuilder instance
   */
  BufferedProducerBuilder compressionType(CompressionType compressionType);

  BufferedProducerBuilder requestTimeoutMs(long timeoutMs);

  BufferedProducer build();
}
