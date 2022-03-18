package io.hstream;

import java.util.Objects;

public class Stream {
  private String streamName;
  private int replicationFactor;
  private int backlogDuration;

  public Stream(String streamName, int replicationFactor, int backlogDuration) {
    this.streamName = streamName;
    this.replicationFactor = replicationFactor;
    this.backlogDuration = backlogDuration;
  }

  public String getStreamName() {
    return streamName;
  }

  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public void setReplicationFactor(int replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  public int getBacklogDuration() {
    return backlogDuration;
  }

  public void setBacklogDuration(int backlogDuration) {
    this.backlogDuration = backlogDuration;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Stream stream = (Stream) o;
    return replicationFactor == stream.replicationFactor
        && backlogDuration == stream.backlogDuration
        && streamName.equals(stream.streamName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamName, replicationFactor, backlogDuration);
  }
}
