package io.hstream;

import java.util.Objects;

public class Stream {
  private String streamName;
  private int replicationFactor;

  public Stream(String streamName, int replicationFactor) {
    this.streamName = streamName;
    this.replicationFactor = replicationFactor;
  }

  public String getStreamName() {
    return streamName;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  public void setReplicationFactor(int replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Stream stream = (Stream) o;
    return replicationFactor == stream.replicationFactor
        && Objects.equals(streamName, stream.streamName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamName, replicationFactor);
  }
}
