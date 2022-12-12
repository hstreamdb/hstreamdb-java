package io.hstream;

public class ConsumerInformation {
  String name;
  String uri;

  String userAgent;

  public String getName() {
    return name;
  }

  public String getUri() {
    return uri;
  }

  public String getUserAgent() {
    return userAgent;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private String name;
    private String uri;
    private String userAgent;

    private Builder() {}

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder uri(String uri) {
      this.uri = uri;
      return this;
    }

    public Builder userAgent(String userAgent) {
      this.userAgent = userAgent;
      return this;
    }

    public ConsumerInformation build() {
      ConsumerInformation consumerInformation = new ConsumerInformation();
      consumerInformation.uri = this.uri;
      consumerInformation.name = this.name;
      consumerInformation.userAgent = this.userAgent;
      return consumerInformation;
    }
  }
}
