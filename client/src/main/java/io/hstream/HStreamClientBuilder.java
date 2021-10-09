package io.hstream;

public interface HStreamClientBuilder {

  HStreamClientBuilder serviceUrl(String serviceUrl);

  HStreamClient build();
}
