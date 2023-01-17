package io.hstream;

/** A builder for {@link Producer}s. */
public interface ProducerBuilder {

  ProducerBuilder stream(String streamName);

  ProducerBuilder requestTimeoutMs(long timeoutMs);

  Producer build();
}
