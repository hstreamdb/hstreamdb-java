package io.hstream.impl
import io.hstream.HStreamClient
import io.hstream.buildBlackBoxSourceClient
import io.hstream.buildBlackBoxSourceClient_
import io.hstream.internal.RecordId
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.mockito.junit.MockitoJUnitRunner
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean

@RunWith(MockitoJUnitRunner::class)
class ResponderImplTest {

    private lateinit var blackBoxSourceClient: HStreamClient

    @BeforeEach
    fun setup() {
        blackBoxSourceClient = buildBlackBoxSourceClient()
    }

    @AfterEach
    fun shutdown() {
        blackBoxSourceClient.close()
    }

    @Test
    fun `when ackAgeLimit == default, all ack should wait for buffer limit`() {
        val xs = buildBlackBoxSourceClient_()
        val consumerName = "some-consumer"
        val client = xs.first
        val countDownLatch = CountDownLatch(500)
        val producer = client
            .newConsumer()
            .name(consumerName)
            .ackAgeLimit(0)
            .subscription("anything")
            .ackBufferSize(10)
            .rawRecordReceiver { _, _ ->
                countDownLatch.countDown()
            }.build()

        producer.startAsync().awaitRunning()
        countDownLatch.await()
        producer.stopAsync().awaitTerminated()

        val ackReceiver = xs.second.getAckChannel(consumerName)
        val initAckSize = ackReceiver.tryReceive().getOrThrow().size

        var ret: List<RecordId>?
        val isLast = AtomicBoolean(false)
        while (
            run {
                ret = ackReceiver.tryReceive().getOrNull()
                ret != null
            }
        ) {
            if (ret!!.size != initAckSize) {
                assert(!isLast.get())
                isLast.set(true)
            }
        }
    }
}
