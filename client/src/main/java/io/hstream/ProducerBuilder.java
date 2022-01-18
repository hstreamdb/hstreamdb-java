package io.hstream;

/** A builder for {@link Producer}s. */
public interface ProducerBuilder {

  ProducerBuilder stream(String streamName);

  Producer build();
}
