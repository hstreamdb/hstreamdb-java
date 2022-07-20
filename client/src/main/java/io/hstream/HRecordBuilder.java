package io.hstream;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Values;

/** A builder for {@link io.hstream.HRecord}s. */
public class HRecordBuilder {

  private Struct.Builder structBuilder;

  public HRecordBuilder() {
    this.structBuilder = Struct.newBuilder();
  }

  public HRecordBuilder put(String fieldName, String stringValue) {
    this.structBuilder = structBuilder.putFields(fieldName, Values.of(stringValue));
    return this;
  }

  public HRecordBuilder put(String fieldName, double numberValue) {
    this.structBuilder = structBuilder.putFields(fieldName, Values.of(numberValue));
    return this;
  }

  public HRecordBuilder put(String fieldName, boolean boolValue) {
    this.structBuilder = structBuilder.putFields(fieldName, Values.of(boolValue));
    return this;
  }

  public HRecordBuilder put(String fieldName, HRecord hRecord) {
    this.structBuilder = structBuilder.putFields(fieldName, Values.of(hRecord.getDelegate()));
    return this;
  }

  public HRecordBuilder put(String fieldName, HArray hArray) {
    this.structBuilder = structBuilder.putFields(fieldName, Values.of(hArray.getDelegate()));
    return this;
  }

  public HRecordBuilder putNull(String fieldName) {
    structBuilder.putFields(fieldName, Values.ofNull());
    return this;
  }

  public HRecordBuilder merge(String json) {
    try {
      JsonFormat.parser().merge(json, this.structBuilder);
    } catch (InvalidProtocolBufferException e) {
      throw new HStreamDBClientException(e);
    }
    return this;
  }

  public HRecordBuilder merge(byte[] hRecordBytes) {
    try {
      this.structBuilder.mergeFrom(hRecordBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new HStreamDBClientException(e);
    }
    return this;
  }

  public HRecord build() {
    return new HRecord(structBuilder.build());
  }
}
