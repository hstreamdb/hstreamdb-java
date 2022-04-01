package io.hstream;

/** A builder for {@link Consumer}s. */
public interface ConsumerBuilder {

  /**
   * Optional, if not provided, the ConsumerBuilder will generate a unique name.
   *
   * @param name consumer name
   * @return the ConsumerBuilder instance
   */
  ConsumerBuilder name(String name);

  /**
   * Positional
   *
   * @param subscription subscription name
   * @return the ConsumerBuilder instance
   */
  ConsumerBuilder subscription(String subscription);

  /**
   * Optional, when the consumer receives raw records, rawRecordReceiver is executed sequentially
   * for each record, if rawRecordReceiver throws an exception, the consumer will ignore it and
   * consume the next record.
   *
   * @param rawRecordReceiver raw record receiver
   * @return the ConsumerBuilder instance
   */
  ConsumerBuilder rawRecordReceiver(RawRecordReceiver rawRecordReceiver);

  /**
   * Same as rawRecordReceiver except receiving HRecord instead of raw record.
   *
   * @param hRecordReceiver HRecord receiver
   * @return the ConsumerBuilder instance
   */
  ConsumerBuilder hRecordReceiver(HRecordReceiver hRecordReceiver);

  /**
   * When {@link Responder}.ack() is called, the consumer will not send it to servers immediately,
   * the ack request will be buffered until the ack count reaches ackBufferSize or the consumer is
   * stopping.
   *
   * @param ackBufferSize ack buffer size
   * @return the ConsumerBuilder instance
   */
  ConsumerBuilder ackBufferSize(int ackBufferSize);

  ConsumerBuilder ackAgeLimit(long ackAgeLimit);

  Consumer build();
}
