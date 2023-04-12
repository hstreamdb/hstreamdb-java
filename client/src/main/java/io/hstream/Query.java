package io.hstream;

import java.util.List;

public class Query {
  String name;
  QueryType type;
  TaskStatus status;
  long createdTime;
  String queryText;
  List<String> sourceStreams;
  String resultName;

  public enum QueryType {
    CreateStreamAs,
    CreateViewAs,
  }

  public String getName() {
    return name;
  }

  public QueryType getType() {
    return type;
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

  public String getResultName() {
    return resultName;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private String name;
    private QueryType type;
    private TaskStatus status;
    private long createdTime;
    private String queryText;
    private List<String> sourceStreams;
    private String resultName;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder type(QueryType type) {
      this.type = type;
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

    public Builder resultName(String resultName) {
      this.resultName = resultName;
      return this;
    }

    public Query build() {
      Query query = new Query();
      query.status = this.status;
      query.createdTime = this.createdTime;
      query.resultName = this.resultName;
      query.name = this.name;
      query.type = this.type;
      query.sourceStreams = this.sourceStreams;
      query.queryText = this.queryText;
      return query;
    }
  }
}
