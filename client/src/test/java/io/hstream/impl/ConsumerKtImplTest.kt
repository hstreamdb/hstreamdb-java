package io.hstream.impl

import com.google.common.util.concurrent.Service
import io.hstream.HStreamClient
import io.hstream.buildMockedClient
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.mockito.junit.MockitoJUnitRunner
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ScheduledThreadPoolExecutor

@RunWith(MockitoJUnitRunner::class)
class ConsumerKtImplTest {
    @Test
    fun testBasic() {
        val throwableArray: CopyOnWriteArrayList<Throwable> = CopyOnWriteArrayList()
        val client: HStreamClient = buildMockedClient()
        val consumer = client.newConsumer()
            .subscription("some_sub")
            .hRecordReceiver { record, ackSender ->
                assert(record != null)
                ackSender.ack()
            }
            .build()
        val threadPool = ScheduledThreadPoolExecutor(1)
        consumer.addListener(
            object : Service.Listener() {
                override fun failed(from: Service.State, failure: Throwable) {
                    println("failed: $from")
                    throwableArray.add(failure)
                }
            },
            threadPool
        )
        consumer.startAsync().awaitRunning()
        Thread.sleep(1000)
        consumer.stopAsync().awaitTerminated()

        if (throwableArray.isNotEmpty()) {
            for (e in throwableArray) {
                println("failed: $e")
            }
            throw throwableArray[0]
        }
    }
}
