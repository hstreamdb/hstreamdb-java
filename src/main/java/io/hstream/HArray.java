package io.hstream;

import com.google.protobuf.ListValue;

/** a data structure like array */
public class HArray {

  private ListValue delegate;

  public static HArrayBuilder newBuilder() {
    return new HArrayBuilder();
  }

  public HArray(ListValue delegate) {
    this.delegate = delegate;
  }

  public ListValue getDelegate() {
    return delegate;
  }

  public int size() {
    return delegate.getValuesCount();
  }

  public boolean getBoolean(int index) {
    return delegate.getValues(index).getBoolValue();
  }

  public int getInt(int index) {
    return (int) delegate.getValues(index).getNumberValue();
  }

  public double getDouble(int index) {
    return delegate.getValues(index).getNumberValue();
  }

  public long getLong(int index) {
    return (long) delegate.getValues(index).getNumberValue();
  }

  public String getString(int index) {
    return delegate.getValues(index).getStringValue();
  }

  public HArray getHArray(int index) {
    return new HArray(delegate.getValues(index).getListValue());
  }

  public HRecord getHRecord(int index) {
    return new HRecord(delegate.getValues(index).getStructValue());
  }
}
