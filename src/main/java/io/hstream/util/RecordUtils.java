package io.hstream.util;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.hstream.HRecord;
import io.hstream.HStreamDBClientException;
import io.hstream.HStreamRecord;
import io.hstream.HStreamRecordHeader;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordUtils {

  private static Logger logger = LoggerFactory.getLogger(RecordUtils.class);

  private static final int JSON_RECORD_FLAG = 0x01 << 24;

  // - RAW:  0x02 << 24
  private static final int RAW_RECORD_FLAG = 0x02 << 24;

  public static HStreamRecord buildHStreamRecordFromRawRecord(byte[] rawRecord) {
    HStreamRecordHeader header = HStreamRecordHeader.newBuilder().setFlag(RAW_RECORD_FLAG).build();
    return HStreamRecord.newBuilder()
        .setHeader(header)
        .setPayload(ByteString.copyFrom(rawRecord))
        .build();
  }

  public static HStreamRecord buildHStreamRecordFromHRecord(HRecord hRecord) {
    try {
      HStreamRecordHeader header =
          HStreamRecordHeader.newBuilder().setFlag(JSON_RECORD_FLAG).build();
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

  public static byte[] parseRawRecordFromHStreamRecord(HStreamRecord hStreamRecord) {
    int flag = hStreamRecord.getHeader().getFlag();
    if (flag != RAW_RECORD_FLAG) {
      throw new HStreamDBClientException.InvalidRecordException("not raw record");
    }
    return hStreamRecord.getPayload().toByteArray();
  }

  public static HRecord parseHRecordFromHStreamRecord(HStreamRecord hStreamRecord) {
    int flag = hStreamRecord.getHeader().getFlag();
    if (flag != JSON_RECORD_FLAG) {
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
}
