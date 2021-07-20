package io.hstream;

import io.hstream.impl.ClientImpl;

public class ClientBuilder {

  private String serviceUrl;

  public ClientBuilder serviceUrl(String serviceUrl) {
    this.serviceUrl = serviceUrl;
    return this;
  }

  public HStreamClient build() {
    return new ClientImpl(serviceUrl);
  }
}
