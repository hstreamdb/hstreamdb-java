package io.hstream;

/** A builder for {@link HStreamClient}s. */
public interface HStreamClientBuilder {

  /**
   * Positional, serviceUrl is the bootstrap service url, when {@link HStreamClientBuilder} builds
   * an {@link HStreamClient}, it will lookup and set the real cluster service to {@link
   * HStreamClient} urls through serviceUrl.
   *
   * @param serviceUrl the bootstrap service url
   * @return the {@link HStreamClientBuilder} instance
   */
  HStreamClientBuilder serviceUrl(String serviceUrl);

  /**
   * Optional, Enable TLS, it requires the tlsCaPath option.
   *
   * @return the {@link HStreamClientBuilder} instance
   */
  HStreamClientBuilder enableTls();

  /**
   * Optional if enableTls is not set.
   *
   * @param caPath the CA certificate file path
   * @return the {@link HStreamClientBuilder} instance
   */
  HStreamClientBuilder tlsCaPath(String caPath);

  /**
   * Optional, enable authentication based on TLS, it requires tlsKeyPath and tlsCertPath options.
   *
   * @return the {@link HStreamClientBuilder} instance
   */
  HStreamClientBuilder enableTlsAuthentication();

  /**
   * Optional if enableTlsAuthentication() is not set
   *
   * @param keyPath the pk8 format key file path
   * @return the {@link HStreamClientBuilder} instance
   */
  HStreamClientBuilder tlsKeyPath(String keyPath);

  /**
   * Optional if enableTlsAuthentication() is not set
   *
   * @param certPath the certificate file path
   * @return the {@link HStreamClientBuilder} instance
   */
  HStreamClientBuilder tlsCertPath(String certPath);

  HStreamClientBuilder requestTimeoutMs(long timeoutMs);

  HStreamClientBuilder withMetadata(String key, String value);

  HStreamClient build();
}
