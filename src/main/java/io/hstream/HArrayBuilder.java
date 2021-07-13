package io.hstream;

import com.google.protobuf.ListValue;
import com.google.protobuf.util.Values;

public class HArrayBuilder {

    private ListValue.Builder delegate;

    public HArrayBuilder(){
        this.delegate = ListValue.newBuilder();
    }

    public HArrayBuilder add(boolean value) {
        this.delegate = delegate.addValues(Values.of(value));
        return this;
    }

    public HArrayBuilder add(double value) {
        this.delegate = delegate.addValues(Values.of(value));
        return this;
    }

    public HArrayBuilder add(int value) {
        this.delegate = delegate.addValues(Values.of(value));
        return this;
    }

    public HArrayBuilder add(long value) {
        this.delegate = delegate.addValues(Values.of(value));
        return this;
    }

    public HArrayBuilder add(String value) {
        this.delegate = delegate.addValues(Values.of(value));
        return this;
    }

    public HArrayBuilder add(HRecord value) {
        this.delegate = delegate.addValues(Values.of(value.getDelegate()));
        return this;
    }

    public HArrayBuilder add(HArray value) {
        this.delegate = delegate.addValues(Values.of(value.getDelegate()));
        return this;
    }

    public HArray build() {
       return new HArray(delegate.build());
    }

}
