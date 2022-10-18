package io.hstream;

public class Query {
    String id;
    TaskStatus status;
    long createdTime;
    String queryText;

    public String getId() {
        return id;
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

    public static final class Builder {
        private String id;
        private TaskStatus status;
        private long createdTime;
        private String queryText;

        public Builder id(String id) {
            this.id = id;
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

        public Query build() {
            Query query = new Query();
            query.queryText = this.queryText;
            query.id = this.id;
            query.createdTime = this.createdTime;
            query.status = this.status;
            return query;
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }
}
