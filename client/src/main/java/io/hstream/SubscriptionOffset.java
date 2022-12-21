package io.hstream;

public class SubscriptionOffset {
    long shardId;
    long batchId;

    public static Builder newBuilder() {
        return new Builder();
    }

    public long getShardId() {
        return shardId;
    }

    public long getBatchId() {
        return batchId;
    }

    public static final class Builder {
        private long shardId;
        private long batchId;

        public Builder withShardId(long shardId) {
            this.shardId = shardId;
            return this;
        }

        public Builder withBatchId(long batchId) {
            this.batchId = batchId;
            return this;
        }

        public SubscriptionOffset build() {
            SubscriptionOffset subscriptionOffset = new SubscriptionOffset();
            subscriptionOffset.shardId = this.shardId;
            subscriptionOffset.batchId = this.batchId;
            return subscriptionOffset;
        }
    }
}
