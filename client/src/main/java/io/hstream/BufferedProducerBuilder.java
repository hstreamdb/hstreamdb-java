package io.hstream;

/** A builder for {@link BufferedProducer}s. */
public interface BufferedProducerBuilder {

  BufferedProducerBuilder stream(String streamName);

  BufferedProducerBuilder recordCountLimit(int recordCountLimit);

  BufferedProducerBuilder flushIntervalMs(long flushIntervalMs);

  BufferedProducerBuilder maxBytesSize(int maxBytesSize);

  BufferedProducerBuilder throwExceptionIfFull(boolean throwExceptionIfFull);

  BufferedProducer build();
}
