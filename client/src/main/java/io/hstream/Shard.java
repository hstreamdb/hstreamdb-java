package io.hstream;

import java.util.Objects;

public class Shard {

  private String streamName;
  private long shardId;
  private String startingHashKey;
  private String endingHashKey;

  public Shard(String streamName, long shardId, String startingHashKey, String endingHashKey) {
    this.streamName = streamName;
    this.shardId = shardId;
    this.startingHashKey = startingHashKey;
    this.endingHashKey = endingHashKey;
  }

  public String getStreamName() {
    return streamName;
  }

  public long getShardId() {
    return shardId;
  }

  public String getStartingHashKey() {
    return startingHashKey;
  }

  public String getEndingHashKey() {
    return endingHashKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Shard shard = (Shard) o;
    return shardId == shard.shardId
        && streamName.equals(shard.streamName)
        && startingHashKey.equals(shard.startingHashKey)
        && endingHashKey.equals(shard.endingHashKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamName, shardId, startingHashKey, endingHashKey);
  }
}
