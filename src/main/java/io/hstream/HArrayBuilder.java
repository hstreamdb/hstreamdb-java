package io.hstream;

import com.google.protobuf.ListValue;
import com.google.protobuf.util.Values;

/** the {@link HArray} constructor */
public class HArrayBuilder {

  private ListValue.Builder delegate;

  public HArrayBuilder() {
    this.delegate = ListValue.newBuilder();
  }

  public HArrayBuilder add(boolean value) {
    delegate.addValues(Values.of(value));
    return this;
  }

  public HArrayBuilder add(double value) {
    delegate.addValues(Values.of(value));
    return this;
  }

  public HArrayBuilder add(int value) {
    delegate.addValues(Values.of(value));
    return this;
  }

  public HArrayBuilder add(long value) {
    delegate.addValues(Values.of(value));
    return this;
  }

  public HArrayBuilder add(String value) {
    delegate.addValues(Values.of(value));
    return this;
  }

  public HArrayBuilder add(HRecord value) {
    delegate.addValues(Values.of(value.getDelegate()));
    return this;
  }

  public HArrayBuilder add(HArray value) {
    delegate.addValues(Values.of(value.getDelegate()));
    return this;
  }

  public HArray build() {
    return new HArray(delegate.build());
  }
}
