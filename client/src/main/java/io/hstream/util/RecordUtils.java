package io.hstream.util;

import static com.google.common.base.Preconditions.*;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdException;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import io.hstream.*;
import io.hstream.Record;
import io.hstream.impl.DefaultSettings;
import io.hstream.impl.ReceivedHStreamRecord;
import io.hstream.internal.BatchHStreamRecords;
import io.hstream.internal.BatchedRecord;
import io.hstream.internal.HStreamRecord;
import io.hstream.internal.HStreamRecordHeader;
import io.hstream.internal.ReceivedRecord;
import io.hstream.internal.RecordId;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordUtils {

  private static final Logger logger = LoggerFactory.getLogger(RecordUtils.class);

  public static HStreamRecord buildHStreamRecordFromRawRecord(byte[] rawRecord) {
    HStreamRecordHeader header =
        HStreamRecordHeader.newBuilder()
            .setFlag(HStreamRecordHeader.Flag.RAW)
            .setKey(DefaultSettings.DEFAULT_PARTITION_KEY)
            .build();
    return HStreamRecord.newBuilder()
        .setHeader(header)
        .setPayload(ByteString.copyFrom(rawRecord))
        .build();
  }

  public static HStreamRecord buildHStreamRecordFromHRecord(HRecord hRecord) {
    HStreamRecordHeader header =
        HStreamRecordHeader.newBuilder()
            .setFlag(HStreamRecordHeader.Flag.JSON)
            .setKey(DefaultSettings.DEFAULT_PARTITION_KEY)
            .build();

    return HStreamRecord.newBuilder().setHeader(header).setPayload(hRecord.toByteString()).build();
  }

  public static HStreamRecord buildHStreamRecordFromRecord(Record record) {
    HStreamRecord hStreamRecord =
        record.isRawRecord()
            ? buildHStreamRecordFromRawRecord(record.getRawRecord())
            : buildHStreamRecordFromHRecord(record.getHRecord());
    if (record.getPartitionKey() == null) {
      return hStreamRecord;
    }
    HStreamRecordHeader newHeader =
        HStreamRecordHeader.newBuilder(hStreamRecord.getHeader())
            .setKey(record.getPartitionKey())
            .build();
    return HStreamRecord.newBuilder(hStreamRecord).setHeader(newHeader).build();
  }

  public static byte[] parseRawRecordFromHStreamRecord(HStreamRecord hStreamRecord) {
    HStreamRecordHeader.Flag flag = hStreamRecord.getHeader().getFlag();
    if (!flag.equals(HStreamRecordHeader.Flag.RAW)) {
      throw new HStreamDBClientException.InvalidRecordException("not raw record");
    }
    return hStreamRecord.getPayload().toByteArray();
  }

  public static RecordHeader parseRecordHeaderFromHStreamRecord(HStreamRecord hStreamRecord) {
    return RecordHeader.newBuild().partitionKey(hStreamRecord.getHeader().getKey()).build();
  }

  public static HRecord parseHRecordFromHStreamRecord(HStreamRecord hStreamRecord) {
    HStreamRecordHeader.Flag flag = hStreamRecord.getHeader().getFlag();
    if (!flag.equals(HStreamRecordHeader.Flag.JSON)) {
      logger.error("expect json record error");
      throw new HStreamDBClientException.InvalidRecordException("not json record");
    }

    try {
      Struct struct = Struct.parseFrom(hStreamRecord.getPayload());
      return new HRecord(struct);
    } catch (InvalidProtocolBufferException e) {
      throw new HStreamDBClientException.InvalidRecordException("construct hrecord error", e);
    }
  }

  public static boolean isRawRecord(HStreamRecord hStreamRecord) {
    HStreamRecordHeader.Flag flag = hStreamRecord.getHeader().getFlag();
    return flag.equals(HStreamRecordHeader.Flag.RAW);
  }

  public static boolean isHRecord(HStreamRecord hStreamRecord) {
    HStreamRecordHeader.Flag flag = hStreamRecord.getHeader().getFlag();
    return flag.equals(HStreamRecordHeader.Flag.JSON);
  }

  public static List<ReceivedHStreamRecord> decompress(ReceivedRecord receivedRecord) {
    BatchedRecord batchedRecord = receivedRecord.getRecord();
    switch (batchedRecord.getCompressionType()) {
      case None:
        return parseBatchHStreamRecords(batchedRecord.getPayload(), receivedRecord);
      case Gzip:
        try {
          ByteArrayInputStream byteArrayInputStream =
              new ByteArrayInputStream(batchedRecord.getPayload().toByteArray());
          GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream);
          return parseBatchHStreamRecords(
              ByteString.copyFrom(gzipInputStream.readAllBytes()), receivedRecord);
        } catch (IOException e) {
          throw new HStreamDBClientException.InvalidRecordException("decompress record error", e);
        }
      case Zstd:
        try {
          byte[] payloadBytes = batchedRecord.getPayload().toByteArray();
          byte[] srcPayloadBytes = Zstd.decompress(payloadBytes, payloadBytes.length);
          return parseBatchHStreamRecords(ByteString.copyFrom(srcPayloadBytes), receivedRecord);
        } catch (ZstdException e) {
          throw new HStreamDBClientException.InvalidRecordException("decompress record error", e);
        }
    }
    throw new HStreamDBClientException.InvalidRecordException("invalid record");
  }

  private static List<ReceivedHStreamRecord> parseBatchHStreamRecords(
      ByteString byteString, ReceivedRecord receivedRecord) {
    try {
      BatchHStreamRecords batchHStreamRecords = BatchHStreamRecords.parseFrom(byteString);
      List<HStreamRecord> hStreamRecords = batchHStreamRecords.getRecordsList();
      checkArgument(
          receivedRecord.getRecordIdsCount() == hStreamRecords.size(),
          "decode HStreamRecord error: invalid batched records from server, the `RecordIdsCount` should equals to `BatchHStreamRecords.size`");

      List<ReceivedHStreamRecord> receivedHStreamRecords = new ArrayList<>(hStreamRecords.size());
      for (int i = 0; i < hStreamRecords.size(); ++i) {
        RecordId recordId = receivedRecord.getRecordIds(i);
        receivedHStreamRecords.add(new ReceivedHStreamRecord(recordId, hStreamRecords.get(i)));
      }
      return receivedHStreamRecords;
    } catch (InvalidProtocolBufferException e) {
      throw new HStreamDBClientException.InvalidRecordException("parse record error", e);
    }
  }

  public static ByteString compress(
      List<HStreamRecord> records, io.hstream.CompressionType compressionType) {
    BatchHStreamRecords recordBatch =
        BatchHStreamRecords.newBuilder().addAllRecords(records).build();
    switch (compressionType) {
      case NONE:
        return recordBatch.toByteString();
      case GZIP:
        try {

          var byteArrayOutputStream = new ByteArrayOutputStream();
          var gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
          gzipOutputStream.write(recordBatch.toByteArray());
          gzipOutputStream.close();
          return ByteString.copyFrom(byteArrayOutputStream.toByteArray());
        } catch (IOException e) {
          throw new HStreamDBClientException("compress records error: ", e);
        }
      case ZSTD:
        try {
          return ByteString.copyFrom(Zstd.compress(recordBatch.toByteArray()));
        } catch (ZstdException e) {
          throw new HStreamDBClientException("compress records error: ", e);
        }
    }
    throw new HStreamDBClientException("compress records error");
  }
}
