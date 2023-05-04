package io.hstream.impl

import io.hstream.HRecord
import io.hstream.Record
import io.hstream.buildBlackBoxSinkClient
import org.junit.jupiter.api.Test

class ProducerKtImplTest {
    @Test
    fun testProducerKtImplCanWork() {
        val client = buildBlackBoxSinkClient()
        val producer = client.newProducer()
            .stream("some-stream")
            .build()
        var future = producer.write(
            Record.newBuilder()
                .rawRecord("ok-payload".toByteArray())
                .build()
        )
        future.join()
        future = producer.write(
            Record.newBuilder()
                .hRecord(
                    HRecord.newBuilder()
                        .put("some-how", "any-how")
                        .build()
                ).build()
        )
        future.join()
    }

// TODO: add tests for request timeout, which will create a new stub
}
