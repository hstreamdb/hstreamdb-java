package io.hstream.impl;

import static com.google.common.base.Preconditions.*;
import static io.hstream.util.UrlSchemaUtils.parseServerUrls;

import io.grpc.ChannelCredentials;
import io.grpc.TlsChannelCredentials;
import io.hstream.HStreamClient;
import io.hstream.HStreamClientBuilder;
import io.hstream.HStreamDBClientException;
import io.hstream.UrlSchema;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

public class HStreamClientBuilderImpl implements HStreamClientBuilder {

  String serviceUrl;
  boolean enableTls = false;
  String caPath;
  boolean enableTlsAuthentication;
  String keyPath;
  String certPath;
  Map<String, String> metadata = new HashMap<>();

  long requestTimeoutMs = DefaultSettings.GRPC_CALL_TIMEOUT_MS;

  private ChannelProvider channelProvider;

  public void channelProvider(ChannelProvider channelProvider) {
    this.channelProvider = channelProvider;
  }

  @Override
  public HStreamClientBuilder serviceUrl(String serviceUrl) {
    this.serviceUrl = serviceUrl;
    return this;
  }

  @Override
  public HStreamClientBuilder enableTls() {
    this.enableTls = true;
    return this;
  }

  @Override
  public HStreamClientBuilder tlsCaPath(String caPath) {
    this.caPath = caPath;
    return this;
  }

  @Override
  public HStreamClientBuilder enableTlsAuthentication() {
    this.enableTlsAuthentication = true;
    return this;
  }

  @Override
  public HStreamClientBuilder tlsKeyPath(String keyPath) {
    this.keyPath = keyPath;
    return this;
  }

  @Override
  public HStreamClientBuilder tlsCertPath(String certPath) {
    this.certPath = certPath;
    return this;
  }

  @Override
  public HStreamClientBuilder requestTimeoutMs(long timeoutMs) {
    this.requestTimeoutMs = timeoutMs;
    return this;
  }

  @Override
  public HStreamClientBuilder withMetadata(String key, String value) {
    metadata.put(key, value);
    return this;
  }

  @Override
  public HStreamClient build() {
    checkArgument(serviceUrl != null, "HStreamClientBuilder: `serviceUrl` should not be null");
    checkArgument(requestTimeoutMs > 0);
    Pair<UrlSchema, List<String>> schemaHosts = parseServerUrls(serviceUrl);
    // FIXME: remove enableTls option
    if (schemaHosts.getKey().equals(UrlSchema.HSTREAMS) && !enableTls) {
      throw new HStreamDBClientException("hstreams url schema should enable tls");
    }

    // tls
    ChannelCredentials credentials = null;
    if (enableTls) {
      checkArgument(caPath != null, "when TLS is enabled, `caPath` should not be null");
      try {
        TlsChannelCredentials.Builder credentialsBuilder =
            TlsChannelCredentials.newBuilder().trustManager(new File(caPath));
        if (enableTlsAuthentication) {
          checkArgument(
              certPath != null,
              "when TLS authentication is enabled, `certPath` should not be null");
          checkArgument(
              keyPath != null, "when TLS authentication is enabled, `keyPath` should not be null");
          credentialsBuilder = credentialsBuilder.keyManager(new File(certPath), new File(keyPath));
        }
        credentials = credentialsBuilder.build();
      } catch (IOException e) {
        throw new HStreamDBClientException(String.format("invalid tls options, %s", e));
      }
    }

    // channel provider
    ChannelProvider provider = channelProvider;
    if (provider == null) {
      provider = new ChannelProviderImpl(credentials, metadata);
    }
    return new HStreamClientKtImpl(schemaHosts.getRight(), requestTimeoutMs, provider);
  }
}
