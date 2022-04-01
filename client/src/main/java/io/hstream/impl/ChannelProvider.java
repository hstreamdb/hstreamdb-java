package io.hstream.impl;

import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.Closeable;
import java.util.concurrent.ConcurrentHashMap;

public class ChannelProvider implements Closeable {

  private static final int DEFAULT_CHANNEL_PROVIDER_SIZE = 64;
  private ChannelCredentials credentials;

  private final ConcurrentHashMap<String, ManagedChannel> provider;

  public ChannelProvider(int size) {
    provider = new ConcurrentHashMap<>(size);
  }

  public ChannelProvider(ChannelCredentials credentials) {
    this.credentials = credentials;
    provider = new ConcurrentHashMap<>(DEFAULT_CHANNEL_PROVIDER_SIZE);
  }

  public ChannelProvider() {
    this(DEFAULT_CHANNEL_PROVIDER_SIZE);
  }

  public ManagedChannel get(String serverUrl) {
    if (credentials == null) {
      return provider.computeIfAbsent(
          serverUrl, url -> ManagedChannelBuilder.forTarget(url).usePlaintext().build());
    }
    return provider.computeIfAbsent(
        serverUrl, url -> Grpc.newChannelBuilder(url, credentials).build());
  }

  @Override
  public void close() {
    provider.forEachValue(Long.MAX_VALUE, ManagedChannel::shutdown);
    provider.clear();
  }
}
