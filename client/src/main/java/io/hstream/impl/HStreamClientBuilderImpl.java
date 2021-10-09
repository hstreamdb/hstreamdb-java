package io.hstream.impl;

import static com.google.common.base.Preconditions.*;

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
    checkNotNull(serviceUrl);
    return new HStreamClientImpl(serviceUrl);
  }
}
