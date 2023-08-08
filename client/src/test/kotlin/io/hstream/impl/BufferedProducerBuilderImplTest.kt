package io.hstream.impl

import io.hstream.HStreamDBClientException
import io.hstream.buildMockedClient
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class BufferedProducerBuilderImplTest {

    @Test
    fun testBuildWithValidInput() {
        val client = buildMockedClient()
        val builder = BufferedProducerBuilderImpl(client as HStreamClientKtImpl)
        val producer = builder.stream("test-stream").requestTimeoutMs(1000).build()
        assertNotNull(producer)
    }

    @Test
    fun testBuildWithMissingStreamName() {
        val client = buildMockedClient()
        val builder = BufferedProducerBuilderImpl(client as HStreamClientKtImpl)
        assertThrows(HStreamDBClientException::class.java) { builder.requestTimeoutMs(1000).build() }
    }

    @Test
    fun testBuildWithInvalidRequestTimeoutMs() {
        val client = buildMockedClient()
        val builder = BufferedProducerBuilderImpl(client as HStreamClientKtImpl)
        assertThrows(
            IllegalArgumentException::class.java,
        ) { builder.stream("test-stream").requestTimeoutMs(-1).build() }
    }

    @Test
    fun testBuildWithNullClient() {
        val builder = BufferedProducerBuilderImpl(null)
        assertThrows(
            NullPointerException::class.java,
        ) { builder.stream("test-stream").requestTimeoutMs(1000).build() }
    }

    @Test
    fun testBuildWithDefaultRequestTimeoutMs() {
        val client = buildMockedClient()
        val builder = BufferedProducerBuilderImpl(client as HStreamClientKtImpl)
        val producer = builder.stream("test-stream").build()
        assertNotNull(producer)
    }

    @Test
    fun testBuildWithMaxRequestTimeoutMs() {
        val client = buildMockedClient()
        val builder = BufferedProducerBuilderImpl(client as HStreamClientKtImpl)
        val producer = builder.stream("test-stream").requestTimeoutMs(Long.MAX_VALUE).build()
        assertNotNull(producer)
    }
}
