package io.hstream;

public interface ProducerBuilder {

  ProducerBuilder stream(String streamName);

  ProducerBuilder enableBatch();

  ProducerBuilder recordCountLimit(int recordCountLimit);

  Producer build();
}
