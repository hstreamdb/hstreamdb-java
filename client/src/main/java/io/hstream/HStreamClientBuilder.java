package io.hstream;

/** A builder for {@link HStreamClient}s. */
public interface HStreamClientBuilder {

  HStreamClientBuilder serviceUrl(String serviceUrl);

  HStreamClientBuilder enableTls();

  HStreamClientBuilder tlsCaPath(String caPath);

  HStreamClientBuilder enableTlsAuthentication();

  HStreamClientBuilder tlsKeyPath(String keyPath);

  HStreamClientBuilder tlsCertPath(String certPath);

  HStreamClient build();
}
