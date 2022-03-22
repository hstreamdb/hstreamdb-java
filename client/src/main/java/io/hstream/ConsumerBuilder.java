package io.hstream;

/** A builder for {@link Consumer}s. */
public interface ConsumerBuilder {

  ConsumerBuilder name(String name);

  ConsumerBuilder subscription(String subscription);

  ConsumerBuilder rawRecordReceiver(RawRecordReceiver rawRecordReceiver);

  ConsumerBuilder hRecordReceiver(HRecordReceiver hRecordReceiver);

  ConsumerBuilder ackBufferSize(int ackBufferSize);

  Consumer build();
}
