package io.hstream.impl;

import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ChannelProviderImpl implements ChannelProvider {

  private static final int DEFAULT_CHANNEL_PROVIDER_SIZE = 64;
  private ChannelCredentials credentials;

  private final ConcurrentHashMap<String, ManagedChannel> provider;
  static final String userAgent =
      "hstreamdb-java/" + ChannelProvider.class.getPackage().getImplementationVersion();

  Map<String, String> header = new HashMap<>();

  public ChannelProviderImpl(int size) {
    provider = new ConcurrentHashMap<>(size);
  }

  public ChannelProviderImpl(ChannelCredentials credentials) {
    this.credentials = credentials;
    provider = new ConcurrentHashMap<>(DEFAULT_CHANNEL_PROVIDER_SIZE);
  }

  public ChannelProviderImpl(ChannelCredentials credentials, Map<String, String> header) {
    this.credentials = credentials;
    provider = new ConcurrentHashMap<>(DEFAULT_CHANNEL_PROVIDER_SIZE);
    if (header != null) {
      this.header = header;
    }
  }

  public ChannelProviderImpl() {
    this(DEFAULT_CHANNEL_PROVIDER_SIZE);
  }

  @Override
  public ManagedChannel get(String serverUrl) {
    return provider.computeIfAbsent(serverUrl, this::getInternal);
  }

  ManagedChannel getInternal(String url) {
    ManagedChannelBuilder<?> builder;
    if (credentials == null) {
      builder = ManagedChannelBuilder.forTarget(url).usePlaintext();
    } else {
      builder = Grpc.newChannelBuilder(url, credentials);
    }
    if (!header.isEmpty()) {
      var metadata = new Metadata();
      header.forEach(
          (k, v) -> metadata.put(Metadata.Key.of(k, Metadata.ASCII_STRING_MARSHALLER), v));
      builder.intercept(MetadataUtils.newAttachHeadersInterceptor(metadata));
    }
    return builder
        .disableRetry()
        .userAgent(userAgent)
        .executor(MoreExecutors.directExecutor())
        .build();
  }

  @Override
  public void close() {
    provider.forEachValue(Long.MAX_VALUE, ManagedChannel::shutdown);
    provider.clear();
  }
}
