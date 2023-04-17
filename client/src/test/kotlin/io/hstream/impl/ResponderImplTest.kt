
import io.hstream.HStreamClient
import io.hstream.buildBlackBoxSourceClient
import io.hstream.buildBlackBoxSourceClient_
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.mockito.junit.MockitoJUnitRunner
import java.util.concurrent.CountDownLatch

@RunWith(MockitoJUnitRunner::class)
class ResponderImplTest {

    private lateinit var blackBoxSourceClient: HStreamClient

    @BeforeEach
    fun setup() {
        blackBoxSourceClient = buildBlackBoxSourceClient()
    }

    @AfterEach()
    fun shutdown() {
        blackBoxSourceClient.close()
    }

    @Test
    @Disabled("DEBUG")
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
        producer.stopAsync().awaitRunning()

        val ackReceiver = xs.second[consumerName]
        val initAckRecvSize = ackReceiver!!.tryReceive().getOrThrow().size
        repeat(50) {
            assert(ackReceiver.tryReceive().getOrThrow().size == initAckRecvSize)
        }
    }
}
