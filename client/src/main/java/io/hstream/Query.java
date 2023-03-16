package io.hstream;

import java.util.List;

public class Query {
  String name;
  TaskStatus status;
  long createdTime;
  String queryText;
  List<String> sourceStreams;
  String resultStream;

  public String getName() {
    return name;
  }

  public TaskStatus getStatus() {
    return status;
  }

  public long getCreatedTime() {
    return createdTime;
  }

  public String getQueryText() {
    return queryText;
  }

  public List<String> getSourceStreams() {
    return sourceStreams;
  }

  public String getResultStream() {
    return resultStream;
  }

  public static final class Builder {
    private String name;
    private TaskStatus status;
    private long createdTime;
    private String queryText;
    private List<String> sourceStreams;
    private String resultStream;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder status(TaskStatus status) {
      this.status = status;
      return this;
    }

    public Builder createdTime(long createdTime) {
      this.createdTime = createdTime;
      return this;
    }

    public Builder queryText(String queryText) {
      this.queryText = queryText;
      return this;
    }

    public Builder sourceStreams(List<String> sourceStreams) {
      this.sourceStreams = sourceStreams;
      return this;
    }

    public Builder resultStream(String resultStream) {
      this.resultStream = resultStream;
      return this;
    }

    public Query build() {
      Query query = new Query();
      query.queryText = this.queryText;
      query.name = this.name;
      query.createdTime = this.createdTime;
      query.status = this.status;
      query.sourceStreams = sourceStreams;
      query.resultStream = resultStream;
      return query;
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }
}
