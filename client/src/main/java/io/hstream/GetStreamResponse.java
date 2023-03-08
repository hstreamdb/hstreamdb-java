package io.hstream;

public class GetStreamResponse {
  Stream stream;

  public Stream getStream() {
    return stream;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    Stream stream;

    public Builder setStream(Stream stream) {
      this.stream = stream;
      return this;
    }

    public GetStreamResponse build() {
      var resp = new GetStreamResponse();
      resp.stream = this.stream;
      return resp;
    }
  }
}
