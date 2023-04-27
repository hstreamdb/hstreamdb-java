package io.hstream;

import com.google.protobuf.ByteString;
import io.hstream.internal.BatchedRecord;
import io.hstream.internal.HStreamRecord;
import io.hstream.internal.HStreamRecordHeader;
import io.hstream.internal.ReceivedRecord;
import io.hstream.internal.RecordId;
import io.hstream.util.GrpcUtils;
import io.hstream.util.RecordUtils;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class RecordUtilsTest {

  @Test
  public void compressRecordsWithGzip() {
    testCompressRecords(CompressionType.GZIP);
  }

  @Test
  public void compressRecordsWithNone() {
    testCompressRecords(CompressionType.NONE);
  }

  @Test
  public void compressRecordsWithZstd() {
    testCompressRecords(CompressionType.ZSTD);
  }

  @Disabled("compressRatio")
  @Test
  public void compressRatioWithGzip() {
    compressRatio(4096, 100, CompressionType.GZIP);
    compressRatio(4096, 250, CompressionType.GZIP);
    compressRatio(4096, 500, CompressionType.GZIP);
    compressRatio(4096, 750, CompressionType.GZIP);
    compressRatio(4096, 1000, CompressionType.GZIP);
  }

  @Disabled("compressRatio")
  @Test
  public void compressRatioWithZstd() {
    compressRatio(4096, 100, CompressionType.ZSTD);
    compressRatio(4096, 250, CompressionType.ZSTD);
    compressRatio(4096, 500, CompressionType.ZSTD);
    compressRatio(4096, 750, CompressionType.ZSTD);
    compressRatio(4096, 1000, CompressionType.ZSTD);
  }

  private void compressRatio(int payloadSize, int recordsCount, CompressionType compressionType) {
    var random = new Random();
    var recordPayload = new byte[payloadSize];
    random.nextBytes(recordPayload);

    var hstreamRecord =
        HStreamRecord.newBuilder()
            .setHeader(
                HStreamRecordHeader.newBuilder()
                    .setKey("k1")
                    .setFlag(HStreamRecordHeader.Flag.RAW)
                    .build())
            .setPayload(ByteString.copyFrom(recordPayload))
            .build();
    var len = recordsCount;
    var records = new ArrayList<HStreamRecord>(len);
    for (int i = 0; i < len; ++i) {
      records.add(hstreamRecord);
    }
    var compressedPayload = RecordUtils.compress(records, compressionType);
    var srcSize = hstreamRecord.getSerializedSize() * len;
    var dstSize = compressedPayload.size();
    System.out.println("before compress size: " + srcSize);
    System.out.println("after compress size: " + dstSize);
    System.out.println("compress ratio: " + (double) srcSize / dstSize);
  }

  private void testCompressRecords(CompressionType compressionType) {
    var recordPayload = ByteString.copyFrom("hello", StandardCharsets.UTF_8);
    var hstreamRecord =
        HStreamRecord.newBuilder()
            .setHeader(
                HStreamRecordHeader.newBuilder()
                    .setKey("k1")
                    .setFlag(HStreamRecordHeader.Flag.RAW)
                    .build())
            .setPayload(recordPayload)
            .build();
    var compressedPayload = RecordUtils.compress(List.of(hstreamRecord), compressionType);

    var receivedRecord =
        ReceivedRecord.newBuilder()
            .setRecord(
                BatchedRecord.newBuilder()
                    .setCompressionType(GrpcUtils.compressionTypeToInternal(compressionType))
                    .setBatchSize(1)
                    .setPayload(compressedPayload)
                    .build())
            .addRecordIds(
                RecordId.newBuilder().setShardId(1).setBatchId(1).setBatchIndex(0).build())
            .build();
    var receivedHStreamRecords = RecordUtils.decompress(receivedRecord);
    for (var receivedHStreamRecord : receivedHStreamRecords) {
      Assertions.assertEquals(receivedHStreamRecord.getRecord().getPayload(), recordPayload);
    }
  }
}
