package io.hstream.impl;

import static com.google.common.base.Preconditions.*;

import io.hstream.HStreamClient;
import io.hstream.HStreamClientBuilder;
import java.util.List;

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
    List<String> serverUrls = parseServerUrls(serviceUrl);
    return new HStreamClientKtImpl(serverUrls);
  }

  private List<String> parseServerUrls(String url) {
    return List.of(url.strip().split(","));
  }
}
