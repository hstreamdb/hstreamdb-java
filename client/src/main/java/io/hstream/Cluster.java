package io.hstream;

public class Cluster {
  /** unit: Seconds. */
  long uptime;

  public long getUptime() {
    return uptime;
  }

  public static class Builder {
    long uptime;

    public Builder uptime(long uptime) {
      this.uptime = uptime;
      return this;
    }

    public Cluster build() {
      return new Cluster(uptime);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  Cluster(long uptime) {
    this.uptime = uptime;
  }
}
