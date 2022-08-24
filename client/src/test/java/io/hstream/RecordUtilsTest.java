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
import java.util.List;
import org.junit.jupiter.api.Assertions;
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
      Assertions.assertTrue(receivedHStreamRecord.getRecord().getPayload().equals(recordPayload));
    }
  }
}
