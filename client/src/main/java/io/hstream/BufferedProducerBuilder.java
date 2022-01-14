package io.hstream;

/** A builder for {@link BufferedProducer}s. */
public interface BufferedProducerBuilder {

  BufferedProducerBuilder recordCountLimit(int recordCountLimit);

  BufferedProducerBuilder flushIntervalMs(long flushIntervalMs);

  BufferedProducerBuilder maxBytesSize(int maxBytesSize);

  BufferedProducer build();
}
