package io.hstream.util;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.hstream.*;
import io.hstream.Record;
import io.hstream.impl.DefaultSettings;
import io.hstream.internal.HStreamRecord;
import io.hstream.internal.HStreamRecordHeader;
import io.hstream.internal.ReceivedRecord;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordUtils {

  private static Logger logger = LoggerFactory.getLogger(RecordUtils.class);

  public static HStreamRecord buildHStreamRecordFromRawRecord(byte[] rawRecord) {
    HStreamRecordHeader header =
        HStreamRecordHeader.newBuilder()
            .setFlag(HStreamRecordHeader.Flag.RAW)
            .setKey(DefaultSettings.DEFAULT_ORDERING_KEY)
            .build();
    return HStreamRecord.newBuilder()
        .setHeader(header)
        .setPayload(ByteString.copyFrom(rawRecord))
        .build();
  }

  public static HStreamRecord buildHStreamRecordFromHRecord(HRecord hRecord) {
    try {
      HStreamRecordHeader header =
          HStreamRecordHeader.newBuilder()
              .setFlag(HStreamRecordHeader.Flag.JSON)
              .setKey(DefaultSettings.DEFAULT_ORDERING_KEY)
              .build();
      String json = JsonFormat.printer().print(hRecord.getDelegate());
      logger.debug("hrecord to json: {}", json);
      return HStreamRecord.newBuilder()
          .setHeader(header)
          .setPayload(ByteString.copyFrom(json, StandardCharsets.UTF_8))
          .build();
    } catch (InvalidProtocolBufferException e) {
      throw new HStreamDBClientException.InvalidRecordException("hrecord to json error", e);
    }
  }

  public static HStreamRecord buildHStreamRecordFromRecord(Record record) {
    HStreamRecord hStreamRecord =
        record.isRawRecord()
            ? buildHStreamRecordFromRawRecord(record.getRawRecord())
            : buildHStreamRecordFromHRecord(record.getHRecord());
    if (record.getKey() == null) {
      return hStreamRecord;
    }
    HStreamRecordHeader newHeader =
        HStreamRecordHeader.newBuilder(hStreamRecord.getHeader()).setKey(record.getKey()).build();
    return HStreamRecord.newBuilder(hStreamRecord).setHeader(newHeader).build();
  }

  public static byte[] parseRawRecordFromHStreamRecord(HStreamRecord hStreamRecord) {
    HStreamRecordHeader.Flag flag = hStreamRecord.getHeader().getFlag();
    if (!flag.equals(HStreamRecordHeader.Flag.RAW)) {
      throw new HStreamDBClientException.InvalidRecordException("not raw record");
    }
    return hStreamRecord.getPayload().toByteArray();
  }

  public static HRecord parseHRecordFromHStreamRecord(HStreamRecord hStreamRecord) {
    HStreamRecordHeader.Flag flag = hStreamRecord.getHeader().getFlag();
    if (!flag.equals(HStreamRecordHeader.Flag.JSON)) {
      logger.error("expect json record error");
      throw new HStreamDBClientException.InvalidRecordException("not json record");
    }

    try {
      String json = hStreamRecord.getPayload().toStringUtf8();
      logger.debug("get json payload: {}", json);
      Struct.Builder structBuilder = Struct.newBuilder();
      JsonFormat.parser().merge(json, structBuilder);
      Struct struct = structBuilder.build();
      return new HRecord(struct);
    } catch (InvalidProtocolBufferException e) {
      throw new HStreamDBClientException.InvalidRecordException("construct hrecord error", e);
    }
  }

  public static boolean isRawRecord(ReceivedRecord receivedRecord) {
    try {
      HStreamRecord hStreamRecord = HStreamRecord.parseFrom(receivedRecord.getRecord());
      return isRawRecord(hStreamRecord);
    } catch (InvalidProtocolBufferException e) {
      throw new HStreamDBClientException.InvalidRecordException("parse HStreamRecord error", e);
    }
  }

  public static boolean isRawRecord(HStreamRecord hStreamRecord) {
    HStreamRecordHeader.Flag flag = hStreamRecord.getHeader().getFlag();
    return flag.equals(HStreamRecordHeader.Flag.RAW);
  }

  public static boolean isHRecord(ReceivedRecord receivedRecord) {
    try {
      HStreamRecord hStreamRecord = HStreamRecord.parseFrom(receivedRecord.getRecord());
      return isHRecord(hStreamRecord);
    } catch (InvalidProtocolBufferException e) {
      throw new HStreamDBClientException.InvalidRecordException("parse HStreamRecord error", e);
    }
  }

  public static boolean isHRecord(HStreamRecord hStreamRecord) {
    HStreamRecordHeader.Flag flag = hStreamRecord.getHeader().getFlag();
    return flag.equals(HStreamRecordHeader.Flag.JSON);
  }
}
