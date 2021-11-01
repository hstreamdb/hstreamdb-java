package io.hstream.impl;

import static com.google.common.base.Preconditions.*;

import io.hstream.Consumer;
import io.hstream.ConsumerBuilder;
import io.hstream.HRecordReceiver;
import io.hstream.RawRecordReceiver;
import java.util.List;

public class ConsumerBuilderImpl implements ConsumerBuilder {

  private final List<String> serverUrls;
  private final ChannelProvider channelProvider;

  private String name;
  private String subscription;
  private RawRecordReceiver rawRecordReceiver;
  private HRecordReceiver hRecordReceiver;

  public ConsumerBuilderImpl(List<String> serverUrls, ChannelProvider channelProvider) {
    this.serverUrls = serverUrls;
    this.channelProvider = channelProvider;
  }

  @Override
  public ConsumerBuilder name(String name) {
    this.name = name;
    return this;
  }

  @Override
  public ConsumerBuilder subscription(String subscription) {
    this.subscription = subscription;
    return this;
  }

  @Override
  public ConsumerBuilder rawRecordReceiver(RawRecordReceiver rawRecordReceiver) {
    this.rawRecordReceiver = rawRecordReceiver;
    return this;
  }

  @Override
  public ConsumerBuilder hRecordReceiver(HRecordReceiver hRecordReceiver) {
    this.hRecordReceiver = hRecordReceiver;
    return this;
  }

  @Override
  public Consumer build() {
    checkNotNull(subscription);
    checkState(rawRecordReceiver != null || hRecordReceiver != null);
    return new ConsumerImpl(
        serverUrls, channelProvider, name, subscription, rawRecordReceiver, hRecordReceiver);
  }
}
