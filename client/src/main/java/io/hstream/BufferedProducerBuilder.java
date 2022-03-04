package io.hstream;

/** A builder for {@link BufferedProducer}s. */
public interface BufferedProducerBuilder {

  BufferedProducerBuilder stream(String streamName);

  /** @param recordCountLimit optional, default: 100, it MUST be greater than 0 */
  BufferedProducerBuilder recordCountLimit(int recordCountLimit);

  /**
   * @param flushIntervalMs optional, default: 100(ms), disabled if flushIntervalMs {@literal <=} 0
   */
  BufferedProducerBuilder flushIntervalMs(long flushIntervalMs);

  /**
   * @param maxBytesSize optional, default: 4096(Bytes), disabled if maxBytesSize {@literal <=} 0
   */
  BufferedProducerBuilder maxBytesSize(int maxBytesSize);

  /**
   * @param throwExceptionIfFull optional, default: false, if throwExceptionIfFull is true, throw
   *     HStreamDBClientException when buffer is full, otherwise, block thread and wait the buffer
   *     to be flushed.
   */
  BufferedProducerBuilder throwExceptionIfFull(boolean throwExceptionIfFull);

  /** @param maxBatchSize optional, default: 0 */
  BufferedProducerBuilder maxBatchSize(int maxBatchSize);

  BufferedProducer build();
}
