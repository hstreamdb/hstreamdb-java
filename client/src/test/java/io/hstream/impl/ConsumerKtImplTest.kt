package io.hstream.impl

import com.google.common.util.concurrent.Service
import io.hstream.HStreamClient
import io.hstream.Subscription
import io.hstream.buildMockedClient
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.runner.RunWith
import org.mockito.junit.MockitoJUnitRunner
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ScheduledThreadPoolExecutor

@RunWith(MockitoJUnitRunner::class)
class ConsumerKtImplTest {

    @Disabled("FIXME")
    @Test
    fun testCreateConsumerOnNonExistedSubscriptionIdShouldFailed() {
        val client: HStreamClient = buildMockedClient()
        val future = CompletableFuture<Unit>()
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

                override fun starting() {
                    println("Listener is starting")
                }

                override fun running() {
                    println("Listener is running")
                }

                override fun failed(from: Service.State, failure: Throwable) {
                    println("failed: $from")
                    future.completeExceptionally(failure)
                }

                override fun terminated(from: Service.State) {
                    println("Listener is terminated")
                }
            },
            threadPool
        )

        consumer.startAsync().awaitRunning()
        Thread.sleep(1000)
        consumer.stopAsync().awaitTerminated()

        assertDoesNotThrow {
            consumer.failureCause()
        }

        if (!future.isCompletedExceptionally) {
            future.complete(Unit)
        }

        if (future.isCompletedExceptionally) {
            future.get()
        }
    }

    @Test
    fun testConsumerKtImplTestBasic() {
        val future = CompletableFuture<Unit>()
        val client: HStreamClient = buildMockedClient()
        val streamName = "some_stream"
        client.createStream(streamName)
        val subId = "some_sub"
        client.createSubscription(Subscription.newBuilder().offset(Subscription.SubscriptionOffset.EARLIEST).subscription(subId).stream(streamName).build())
        val consumer = client.newConsumer()
            .subscription(subId)
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
                    future.completeExceptionally(failure)
                }
            },
            threadPool
        )
        consumer.startAsync().awaitRunning()
        Thread.sleep(1000)
        consumer.stopAsync().awaitTerminated()

        if (!future.isCompletedExceptionally) {
            future.complete(Unit)
        }

        if (future.isCompletedExceptionally) {
            future.get()
        }
    }
}
