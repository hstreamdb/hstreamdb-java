package io.hstream.impl
import io.hstream.HStreamClient
import io.hstream.buildBlackBoxSourceClient
import io.hstream.buildBlackBoxSourceClient_
import io.hstream.internal.RecordId
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.mockito.junit.MockitoJUnitRunner
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.abs

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
    fun `when ackAgeLimit == 0, all ack should wait for buffer limit`() {
        val xs = buildBlackBoxSourceClient_()
        val consumerName = "some-consumer"
        val client = xs.first
        val countDownLatch = CountDownLatch(500)
        val consumer = client
            .newConsumer()
            .name(consumerName)
            .ackAgeLimit(0)
            .subscription("anything")
            .ackBufferSize(10)
            .rawRecordReceiver { _, responder ->
                countDownLatch.countDown()
                responder.ack()
            }.build()

        consumer.startAsync().awaitRunning()
        countDownLatch.await()
        consumer.stopAsync().awaitTerminated()

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

    @Test
    fun `when ackAgeLimit is set, and ackBufferSize is not set, ACKs should send by age limit`() {
        val xs = buildBlackBoxSourceClient_()
        val consumerName = "some-consumer"
        val client = xs.first
        val controller = xs.second
        val intervalMs: Long = 500
        val consumer = client
            .newConsumer()
            .name(consumerName)
            .ackAgeLimit(intervalMs)
            .subscription("anything")
            .rawRecordReceiver { _, responder ->
                responder.ack()
            }.build()

        val retList = CopyOnWriteArrayList<Long>()
        controller.setAckChannel(consumerName)
        val ackReceiver = controller.getAckChannel(consumerName)
        consumer.startAsync().awaitRunning()
        Thread {
            runBlocking {
                while (true) {
                    ackReceiver.receive()
                    retList.add(
                        System.currentTimeMillis()
                    )
                }
            }
        }.start()
        Thread.sleep(1000 * 10)
        consumer.stopAsync().awaitTerminated()

        fun inRange(time: Long): Boolean {
            return (
                abs(intervalMs - time) <= 50
                )
        }

        val retListSize = retList.size - 1
        for (i in 0 until retListSize) {
            val time0 = retList[i + 1]
            val time1 = retList[i]
            val time = time0 - time1
            assert(inRange(time)) { "i = $i, time0 = $time0, time1 = $time1, time = $time" }
        }
    }
}
