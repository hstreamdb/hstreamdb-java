package io.hstream.impl;

import io.hstream.HStreamClient;
import io.hstream.HStreamClientBuilder;

public class HStreamClientBuilderImpl implements HStreamClientBuilder {

  private String serviceUrl;

  @Override
  public HStreamClientBuilder serviceUrl(String serviceUrl) {
    this.serviceUrl = serviceUrl;
    return this;
  }

  @Override
  public HStreamClient build() {
    return new HStreamClientImpl(serviceUrl);
  }
}
