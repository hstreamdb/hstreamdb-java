package io.hstream;

import java.util.Objects;

public class Stream {
  private String streamName;
  private int replicationFactor;
  private int backlogDuration;
  private int shardCount;

  public Stream(String streamName, int replicationFactor, int backlogDuration, int shardCount) {
    this.streamName = streamName;
    this.replicationFactor = replicationFactor;
    this.backlogDuration = backlogDuration;
    this.shardCount = shardCount;
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

  public int getShardCount() {
    return shardCount;
  }

  public void setShardCount(int shardCount) {
    this.shardCount = shardCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Stream stream = (Stream) o;
    return replicationFactor == stream.replicationFactor
        && backlogDuration == stream.backlogDuration
        && streamName.equals(stream.streamName)
        && shardCount == stream.shardCount;
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamName, replicationFactor, backlogDuration, shardCount);
  }
}
