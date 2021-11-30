package io.hstream;

import static com.google.common.base.Preconditions.*;
public class Record {

    private String key;
    private byte[] rawRecord;
    private HRecord hRecord;

    private boolean isRawRecord;

    public static Builder newBuilder() {
        return new Builder();
    }

    private Record(String key, byte[] rawRecord) {
        isRawRecord = true;
        this.key = key;
        this.rawRecord = rawRecord;

    }

    private Record(String key, HRecord hRecord) {
        isRawRecord = false;
        this.key = key;
        this.hRecord = hRecord;
    }

    public String getKey() {
        return key;
    }

    public byte[] getRawRecord() {
        checkArgument(isRawRecord);
        return rawRecord;
    }

    public HRecord getHRecord() {
        checkArgument(!isRawRecord);
        return hRecord;
    }

    public boolean isRawRecord() {
        return isRawRecord;
    }

    public static class Builder {
        private String key;
        private byte[] rawRecord;
        private HRecord hRecord;

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder rawRecord(byte[] rawRecord) {
            this.rawRecord = rawRecord;
            return this;
        }

        public Builder hRecord(HRecord hRecord) {
            this.hRecord = hRecord;
            return this;
        }

        public Record build() {
            checkArgument((rawRecord != null && hRecord == null) || (rawRecord == null && hRecord != null));
            if(rawRecord != null) {
                return new Record(key, rawRecord);
            } else {
                return new Record(key, hRecord);
            }
        }

    }
}
