package io.hstream;

import com.google.protobuf.Struct;
import com.google.protobuf.util.Values;

/** used to construct a {@link HRecord} */
public class HRecordBuilder {

  private Struct.Builder structBuilder;

  public HRecordBuilder() {
    this.structBuilder = Struct.newBuilder();
  }

  public HRecordBuilder put(String fieldName, String stringValue) {
    structBuilder.putFields(fieldName, Values.of(stringValue));
    return this;
  }

  public HRecordBuilder put(String fieldName, double numberValue) {
    structBuilder.putFields(fieldName, Values.of(numberValue));
    return this;
  }

  public HRecordBuilder put(String fieldName, boolean boolValue) {
    structBuilder.putFields(fieldName, Values.of(boolValue));
    return this;
  }

  public HRecordBuilder put(String fieldName, HRecord hRecord) {
    structBuilder.putFields(fieldName, Values.of(hRecord.getDelegate()));
    return this;
  }

  public HRecordBuilder put(String fieldName, HArray hArray) {
    structBuilder.putFields(fieldName, Values.of(hArray.getDelegate()));
    return this;
  }

  public HRecord build() {
    return new HRecord(structBuilder.build());
  }
}
