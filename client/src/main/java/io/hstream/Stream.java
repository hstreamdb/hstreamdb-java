package io.hstream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.time.Instant;
import java.util.Objects;

public class Stream {
  private String streamName;
  private int replicationFactor;
  private int backlogDuration;
  private int shardCount;
  private final Instant createdTime;

  Stream(
      String streamName,
      int replicationFactor,
      int backlogDuration,
      int shardCount,
      Instant createdTime) {
    this.streamName = streamName;
    this.replicationFactor = replicationFactor;
    this.backlogDuration = backlogDuration;
    this.shardCount = shardCount;
    this.createdTime = createdTime;
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

  public Instant getCreatedTime() {
    return createdTime;
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

  public static final class Builder {
    private String streamName;
    private int replicationFactor = 1;
    private int backlogDuration = 3600 * 24;
    private int shardCount = 1;
    private Instant createdTime;

    /**
     * @param streamName required, the name of the stream
     * @return Stream.Builder instance
     */
    public Builder streamName(String streamName) {
      this.streamName = streamName;
      return this;
    }

    /**
     * @param replicationFactor optional(default: 1), replication factor of the stream
     * @return Stream.Builder instance
     */
    public Builder replicationFactor(int replicationFactor) {
      this.replicationFactor = replicationFactor;
      return this;
    }

    /**
     * @param backlogDuration optional(default: 3600 * 24), backlog duration(in seconds) of the
     *     stream
     * @return Stream.Builder instance
     */
    public Builder backlogDuration(int backlogDuration) {
      this.backlogDuration = backlogDuration;
      return this;
    }

    /**
     * @param shardCount optional(default: 1), number of shards in the stream
     * @return Stream.Builder instance
     */
    public Builder shardCount(int shardCount) {
      this.shardCount = shardCount;
      return this;
    }

    public Builder createdTime(Instant createdTime) {
      this.createdTime = createdTime;
      return this;
    }

    public Stream build() {
      checkNotNull(streamName);
      checkArgument(replicationFactor >= 1 && replicationFactor <= 15);
      checkArgument(shardCount >= 1);
      return new Stream(streamName, replicationFactor, backlogDuration, shardCount, createdTime);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }
}
