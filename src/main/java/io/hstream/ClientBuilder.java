package io.hstream;

import io.hstream.impl.ClientImpl;

/** used to construct a hstream client, which you can use it to interact with hstream server */
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
