package io.hstream;

import com.google.protobuf.ByteString;
import io.hstream.internal.HStreamRecord;
import io.hstream.internal.HStreamRecordHeader;
import io.hstream.util.RecordUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;

public class CompressionBenchmark {

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void compressWithGZip() {
    var records = mkRecords(4096, 1000);
    RecordUtils.compress(records, CompressionType.GZIP);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void compressWithZstd() {
    var records = mkRecords(4096, 1000);
    RecordUtils.compress(records, CompressionType.ZSTD);
  }

  private static List<HStreamRecord> mkRecords(int payloadSize, int recordsCount) {
    var recordPayload = new byte[payloadSize];
    ThreadLocalRandom.current().nextBytes(recordPayload);

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

    return records;
  }
}
