package io.hstream.impl

import io.hstream.BatchSetting
import io.hstream.CompressionType
import io.hstream.Record
import io.hstream.buildBlackBoxSinkClient_
import io.hstream.util.GrpcUtils.compressionTypeFromGrpc
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture

class BufferedProducerKtImplTest {
    @Test
    fun testBufferedProducerCanWork() {
        val streamName = "some-stream"
        val xs = buildBlackBoxSinkClient_()
        val client = xs.first
        val controller = xs.second.first
        val chan = controller.getStreamNameFlushChannel(streamName)
        val producer = client.newBufferedProducer()
            .stream(streamName)
            .build()
        val writeResult = producer.write(
            Record.newBuilder()
                .rawRecord("some-byte-data".toByteArray())
                .build(),
        )
        producer.flush()
        writeResult.get()
        assert(chan.tryReceive().isSuccess)
    }

    @Test
    fun testCompressionTypes() {
        val compressionTypes = listOf(CompressionType.NONE, CompressionType.GZIP, CompressionType.ZSTD)
        compressionTypes.forEach {
            val streamName = "some-stream"
            val xs = buildBlackBoxSinkClient_()
            val client = xs.first
            val controller = xs.second.first
            val chan = controller.getStreamNameFlushChannel(streamName)
            val producer = client.newBufferedProducer()
                .stream(streamName)
                .compressionType(it)
                .build()
            val writeResult = producer.write(
                Record.newBuilder()
                    .rawRecord("some-byte-data".toByteArray())
                    .build(),
            )
            producer.flush()
            writeResult.get()
            val receivedRecord = chan.tryReceive()
            val castCompressionType = compressionTypeFromGrpc(receivedRecord.getOrThrow().second.compressionType)
            assert(castCompressionType.equals(it))
        }
    }

    @Test
    fun testBatchSettingRecordCountLimit() {
        val streamName = "some-stream"
        val xs = buildBlackBoxSinkClient_()
        val client = xs.first
        val controller = xs.second.first
        val chan = controller.getStreamNameFlushChannel(streamName)
        val recordCountLimit = 50
        val producer = client.newBufferedProducer()
            .stream(streamName)
            .batchSetting(
                BatchSetting.newBuilder()
                    .recordCountLimit(recordCountLimit)
                    .build(),
            )
            .build()

        val mutWriteResults = mutableListOf<CompletableFuture<String>>()
        repeat(recordCountLimit - 1) {
            val writeResult = producer.write(
                Record.newBuilder()
                    .rawRecord("some-byte-data".toByteArray())
                    .build(),
            )
            mutWriteResults.add(writeResult)
        }
        for (x in mutWriteResults) {
            assert(!x.isDone)
            assert(!x.isCompletedExceptionally)
            assert(!x.isCancelled)
        }
        val writeResult = producer.write(
            Record.newBuilder()
                .rawRecord("some-byte-data".toByteArray())
                .build(),
        )
        mutWriteResults.add(writeResult)
        Thread.sleep(150)
        for (x in mutWriteResults) {
            assert(x.isDone)
            assert(!x.isCompletedExceptionally)
            assert(!x.isCancelled)
        }
    }
}
