package io.hstream;

import java.util.List;

public class View {
  String name;
  TaskStatus status;
  long createdTime;
  String sql;
  List<String> schema;

  public static Builder newBuilder() {
    return new Builder();
  }

  public String getName() {
    return name;
  }

  public TaskStatus getStatus() {
    return status;
  }

  public long getCreatedTime() {
    return createdTime;
  }

  public String getSql() {
    return sql;
  }

  public List<String> getSchema() {
    return schema;
  }

  public static final class Builder {
    private String name;
    private TaskStatus status;
    private long createdTime;
    private String sql;
    private List<String> schema;

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

    public Builder sql(String sql) {
      this.sql = sql;
      return this;
    }

    public Builder schema(List<String> schema) {
      this.schema = schema;
      return this;
    }

    public View build() {
      View view = new View();
      view.createdTime = this.createdTime;
      view.schema = this.schema;
      view.status = this.status;
      view.name = this.name;
      view.sql = this.sql;
      return view;
    }
  }
}
