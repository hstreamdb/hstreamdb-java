package io.hstream.impl
import io.hstream.HStreamClient
import io.hstream.ReceivedHRecord
import io.hstream.ReceivedRawRecord
import io.hstream.buildBlackBoxSourceClient
import io.hstream.buildBlackBoxSourceClient_
import io.hstream.internal.RecordId
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
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
        val countDownLatch = CountDownLatch(1200)
        val consumer = client
            .newConsumer()
            .name(consumerName)
            .ackAgeLimit(0)
            .subscription("anything")
            .ackBufferSize(10)
            .rawRecordReceiver { _, responder ->
                countDownLatch.countDown()
                responder.ack()
            }.hRecordReceiver { _, responder ->
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
    @Disabled("should not dependent on async timers")
    fun `when ackAgeLimit is set, and ackBufferSize is set to very large, ACKs should send by age limit`() {
        val xs = buildBlackBoxSourceClient_()
        val consumerName = "some-consumer"
        val client = xs.first
        val controller = xs.second
        controller.setSendBatchLen(1)
        val sendInterval: Long = 400
        controller.setSendInterval(sendInterval)
        val intervalMs: Long = 800
        val consumer = client
            .newConsumer()
            .name(consumerName)
            .ackAgeLimit(intervalMs)
            .subscription("anything")
            .ackBufferSize(10000000)
            .rawRecordReceiver { _, responder ->
                responder.ack()
            }.hRecordReceiver { _, responder ->
                responder.ack()
            }
            .build()

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
                abs(intervalMs - time) <= sendInterval + 50
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

    @Test
    fun testBothRecordReceiverShouldWork() {
        val xs = buildBlackBoxSourceClient_()
        val consumerName = "some-consumer"
        val client = xs.first
        val controller = xs.second
        val intervalMs: Long = 500
        val consumerBuilder = client
            .newConsumer()
            .name(consumerName)
            .ackAgeLimit(intervalMs)
            .subscription("anything")

        val receivedHRecords = CopyOnWriteArrayList<ReceivedHRecord>()
        val receivedRawRecords = CopyOnWriteArrayList<ReceivedRawRecord>()

        val countDownLatch = CountDownLatch(3000)

        consumerBuilder.hRecordReceiver { receivedHRecord, responder ->
            responder.ack()
            receivedHRecords.add(receivedHRecord)
            countDownLatch.countDown()
        }
        consumerBuilder.rawRecordReceiver { receivedRawRecord, responder ->
            responder.ack()
            receivedRawRecords.add(receivedRawRecord)
            countDownLatch.countDown()
        }

        val consumer = consumerBuilder.build()

        consumer.startAsync().awaitRunning()
        countDownLatch.await()
        controller.closeAllSubscriptions()
        Thread.sleep(50)
        consumer.stopAsync().awaitTerminated()

        assertEquals(receivedHRecords.size, receivedRawRecords.size)
        assert(receivedHRecords.size != 0)
    }
}
