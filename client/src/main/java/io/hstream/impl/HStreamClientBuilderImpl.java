package io.hstream.impl;

import static com.google.common.base.Preconditions.*;

import io.grpc.TlsChannelCredentials;
import io.hstream.HStreamClient;
import io.hstream.HStreamClientBuilder;
import io.hstream.HStreamDBClientException;
import java.io.File;
import java.io.IOException;
import java.util.List;

public class HStreamClientBuilderImpl implements HStreamClientBuilder {

  private String serviceUrl;
  private boolean enableTls = false;
  private String caPath;
  private boolean enableTlsAuthentication;
  private String keyPath;
  private String certPath;

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
  public HStreamClient build() {
    checkNotNull(serviceUrl);
    List<String> serverUrls = parseServerUrls(serviceUrl);
    if (enableTls) {
      try {
        TlsChannelCredentials.Builder credentialsBuilder =
            TlsChannelCredentials.newBuilder().trustManager(new File(caPath));
        if (enableTlsAuthentication) {
          credentialsBuilder = credentialsBuilder.keyManager(new File(certPath), new File(keyPath));
        }
        return new HStreamClientKtImpl(serverUrls, credentialsBuilder.build());
      } catch (IOException e) {
        throw new HStreamDBClientException(String.format("invalid tls options, %s", e));
      }
    }
    return new HStreamClientKtImpl(serverUrls, null);
  }

  private List<String> parseServerUrls(String url) {
    return List.of(url.strip().split(","));
  }
}
