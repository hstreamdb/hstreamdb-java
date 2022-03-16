package io.hstream;

public class FlowControlSetting {

    private int bytesLimit;
    private boolean throwExceptionIfFull;

    public int getBytesLimit() {
        return bytesLimit;
    }

    public boolean throwExceptionIfFull() {
        return throwExceptionIfFull;
    }

    public static class Builder {

        private int bytesLimit;
        private boolean throwExceptionIfFull;

        public Builder() {}

        public Builder bytesLimit(int bytesLimit) {
            this.bytesLimit = bytesLimit;
            return Builder.this;
        }

        /**
         * @param throwExceptionIfFull optional, default: false, if throwExceptionIfFull is true, throw
         *     HStreamDBClientException when buffer is full, otherwise, block thread and wait the buffer
         *     to be flushed.
         */
        public Builder throwExceptionIfFull(boolean throwExceptionIfFull) {
            this.throwExceptionIfFull = throwExceptionIfFull;
            return this;
        }

        public FlowControlSetting build() {

            return new FlowControlSetting(this);
        }
    }

    private FlowControlSetting(Builder builder) {
        this.bytesLimit = builder.bytesLimit;
        this.throwExceptionIfFull = builder.throwExceptionIfFull;
    }
}
