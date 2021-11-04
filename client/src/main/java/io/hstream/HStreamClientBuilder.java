package io.hstream;

/** A builder for {@link HStreamClient}s. */
public interface HStreamClientBuilder {

  HStreamClientBuilder serviceUrl(String serviceUrl);

  HStreamClient build();
}
