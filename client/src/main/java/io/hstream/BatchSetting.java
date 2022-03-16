package io.hstream;

public class BatchSetting {

    private final int recordCountLimit;
    private final int bytesLimit;
    private final long ageLimit;

    public int getRecordCountLimit() {
        return recordCountLimit;
    }

    public int getBytesLimit() {
        return bytesLimit;
    }

    public long getAgeLimit() {
        return ageLimit;
    }

    static public Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private int recordCountLimit;
        private int bytesLimit;
        private long ageLimit;

        public Builder() {}

        /** @param recordCountLimit optional, default: 100, it MUST be greater than 0 */
        public Builder recordCountLimit(int recordCountLimit) {
            this.recordCountLimit = recordCountLimit;
            return Builder.this;
        }

        /**
         * @param bytesLimit optional, default: 4096(Bytes), disabled if bytesLimit {@literal <=} 0
         */
        public Builder bytesLimit(int bytesLimit) {
            this.bytesLimit = bytesLimit;
            return Builder.this;
        }

        /**
         * @param ageLimit optional, default: 100(ms), disabled if ageLimit {@literal <=} 0
         */
        public Builder ageLimit(long ageLimit) {
            this.ageLimit = ageLimit;
            return Builder.this;
        }

        public BatchSetting build() {

            return new BatchSetting(this);
        }
    }

    private BatchSetting(Builder builder) {
        this.recordCountLimit = builder.recordCountLimit;
        this.bytesLimit = builder.bytesLimit;
        this.ageLimit = builder.ageLimit;
    }
}

