package io.hstream;

import java.util.Objects;

/** A class for storing information about streams */
public class Stream {
  private String streamName;
  private int replicationFactor;

  /**
   * a constructor for a real-time data stream
   *
   * @param streamName the name of the stream
   * @param replicationFactor the number of replicas to be stored in the database
   */
  public Stream(String streamName, int replicationFactor) {
    this.streamName = streamName;
    this.replicationFactor = replicationFactor;
  }

  /** get the name of the stream */
  public String getStreamName() {
    return streamName;
  }

  /** get the replication factor of the stream */
  public int getReplicationFactor() {
    return replicationFactor;
  }

  /** update the name of the stream */
  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  /** update the replication factor of the stream */
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
