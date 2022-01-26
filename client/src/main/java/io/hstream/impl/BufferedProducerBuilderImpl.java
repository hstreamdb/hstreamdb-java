package io.hstream.impl;

import io.hstream.BufferedProducer;
import io.hstream.BufferedProducerBuilder;
import io.hstream.HStreamDBClientException;

public class BufferedProducerBuilderImpl implements BufferedProducerBuilder {

  private String streamName;
  private int recordCountLimit = 100;
  private long flushIntervalMs = 100;
  private int maxBytesSize = 4096;
  private boolean throwExceptionIfFull = false;

  @Override
  public BufferedProducerBuilder stream(String streamName) {
    this.streamName = streamName;
    return this;
  }

  /** @param recordCountLimit default value is 100, it can NOT be less than 1 */
  @Override
  public BufferedProducerBuilder recordCountLimit(int recordCountLimit) {
    this.recordCountLimit = recordCountLimit;
    return this;
  }

  /**
   * @param flushIntervalMs default value is 100ms, if flushIntervalMs <= 0, disables timed based
   *     flush strategy.
   */
  @Override
  public BufferedProducerBuilder flushIntervalMs(long flushIntervalMs) {
    this.flushIntervalMs = flushIntervalMs;
    return this;
  }

  /**
   * @param maxBytesSize default value is 4K(4096), if maxBytesSize <= 0, does not limit bytes size
   */
  @Override
  public BufferedProducerBuilder maxBytesSize(int maxBytesSize) {
    this.maxBytesSize = maxBytesSize;
    return this;
  }

  @Override
  public BufferedProducerBuilder throwExceptionIfFull(boolean throwExceptionIfFull) {
    this.throwExceptionIfFull = throwExceptionIfFull;
    return this;
  }

  @Override
  public BufferedProducer build() {
    if (recordCountLimit < 1) {
      throw new HStreamDBClientException(
          String.format(
              "build buffedProducer failed, recordCountLimit(%d) can NOT be less than 1",
              recordCountLimit));
    }
    return new BufferedProducerKtImpl(
        streamName, recordCountLimit, flushIntervalMs, maxBytesSize, throwExceptionIfFull);
  }
}
