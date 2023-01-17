package io.hstream.impl;

import static com.google.common.base.Preconditions.*;

import io.grpc.TlsChannelCredentials;
import io.hstream.HStreamClient;
import io.hstream.HStreamClientBuilder;
import io.hstream.HStreamDBClientException;
import io.hstream.UrlSchema;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

public class HStreamClientBuilderImpl implements HStreamClientBuilder {

  private String serviceUrl;
  private boolean enableTls = false;
  private String caPath;
  private boolean enableTlsAuthentication;
  private String keyPath;
  private String certPath;

  private long requestTimeoutMs = DefaultSettings.GRPC_CALL_TIMEOUT_MS;

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
  public HStreamClient build() {
    checkNotNull(serviceUrl);
    checkArgument(requestTimeoutMs > 0);
    Pair<UrlSchema, List<String>> schemaHosts = parseServerUrls(serviceUrl);
    // FIXME: remove enableTls option
    if (schemaHosts.getKey().equals(UrlSchema.HSTREAMS) && !enableTls) {
      throw new HStreamDBClientException("hstreams url schema should enable tls");
    }
    if (enableTls) {
      checkNotNull(caPath);
      try {
        TlsChannelCredentials.Builder credentialsBuilder =
            TlsChannelCredentials.newBuilder().trustManager(new File(caPath));
        if (enableTlsAuthentication) {
          checkNotNull(certPath);
          checkNotNull(keyPath);
          credentialsBuilder = credentialsBuilder.keyManager(new File(certPath), new File(keyPath));
        }
        return new HStreamClientKtImpl(
            schemaHosts.getRight(), requestTimeoutMs, credentialsBuilder.build());
      } catch (IOException e) {
        throw new HStreamDBClientException(String.format("invalid tls options, %s", e));
      }
    }
    return new HStreamClientKtImpl(schemaHosts.getRight(), requestTimeoutMs, null);
  }

  private Pair<UrlSchema, List<String>> parseServerUrls(String url) {
    String uriStr = url.strip();
    var schemaHosts = uriStr.split("://");
    if (schemaHosts.length != 2) {
      throw new HStreamDBClientException(
          "incorrect serviceUrl:" + uriStr + " (correct example: hstream://127.0.0.1:6570)");
    }
    var schemaStr = schemaHosts[0];
    UrlSchema urlSchema;
    try {
      urlSchema = UrlSchema.valueOf(schemaStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new HStreamDBClientException("Invalid url schema:" + schemaStr);
    }
    var hosts = schemaHosts[1];
    return Pair.of(urlSchema, parseHosts(hosts));
  }

  private List<String> parseHosts(String hosts) {
    return Arrays.stream(hosts.split(",")).map(this::normalizeHost).collect(Collectors.toList());
  }

  private String normalizeHost(String host) {
    var address_port = host.split(":");
    if (address_port.length == 1) {
      return host + ":6570";
    }
    return host;
  }
}
