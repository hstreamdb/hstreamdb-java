package io.hstream;

public class BatchSetting {

    private int recordCountLimit = 100;
    private int bytesLimit = 4096;
    private long ageLimit = 100;

    public BatchSetting() {}

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
            if (recordCountLimit < 1 && bytesLimit < 1 && ageLimit < 1) {
                throw new HStreamDBClientException("disabled all options, at least one option should be enabled");
            }
            return new BatchSetting(this);
        }
    }

    public BatchSetting(Builder builder) {
        this.recordCountLimit = builder.recordCountLimit;
        this.bytesLimit = builder.bytesLimit;
        this.ageLimit = builder.ageLimit;
    }
}

