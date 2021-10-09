package io.hstream;

public interface ConsumerBuilder {

  ConsumerBuilder name(String name);

  ConsumerBuilder subscription(String subscription);

  ConsumerBuilder rawRecordReceiver(RawRecordReceiver rawRecordReceiver);

  ConsumerBuilder hRecordReceiver(HRecordReceiver hRecordReceiver);

  Consumer build();
}
