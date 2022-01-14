package io.hstream.impl;

import io.hstream.BufferedProducer;
import io.hstream.BufferedProducerBuilder;

public class BufferedProducerBuilderImpl implements BufferedProducerBuilder {

  private final String streamName;

  private int recordCountLimit = 100;
  private long flushIntervalMs = -1;
  private int maxBytesSize = 4096;

  public BufferedProducerBuilderImpl(String streamName) {
    this.streamName = streamName;
  }

  /** @param recordCountLimit default value is 100, it can NOT be less than 1 */
  @Override
  public BufferedProducerBuilder recordCountLimit(int recordCountLimit) {
    this.recordCountLimit = recordCountLimit;
    return this;
  }

  /**
   * @param flushIntervalMs set flushIntervalMs(milliseconds) > 0 to enable timed based flush
   *     strategy
   */
  @Override
  public BufferedProducerBuilder flushIntervalMs(long flushIntervalMs) {
    this.flushIntervalMs = flushIntervalMs;
    return this;
  }

  /** @param maxBytesSize default value is 4K(4096) */
  @Override
  public BufferedProducerBuilder maxBytesSize(int maxBytesSize) {
    this.maxBytesSize = maxBytesSize;
    return this;
  }

  @Override
  public BufferedProducer build() {
    recordCountLimit = Math.max(recordCountLimit, 1);
    return new BufferedProducerKtImpl(streamName, recordCountLimit, flushIntervalMs, maxBytesSize);
  }
}
