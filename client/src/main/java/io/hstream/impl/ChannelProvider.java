package io.hstream.impl;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.Closeable;
import java.util.concurrent.ConcurrentHashMap;

public class ChannelProvider implements Closeable {

  private static final int DEFAULT_CHANNEL_PROVIDER_SIZE = 64;

  private final ConcurrentHashMap<String, ManagedChannel> provider;

  public ChannelProvider(int size) {
    provider = new ConcurrentHashMap<>(size);
  }

  public ChannelProvider() {
    this(DEFAULT_CHANNEL_PROVIDER_SIZE);
  }

  public ManagedChannel get(String serverUrl) {
    return provider.computeIfAbsent(
        serverUrl, url -> ManagedChannelBuilder.forTarget(url).usePlaintext().build());
  }

  @Override
  public void close() {
    provider.forEachValue(Long.MAX_VALUE, managedChannel -> managedChannel.shutdown());
    provider.clear();
  }
}
