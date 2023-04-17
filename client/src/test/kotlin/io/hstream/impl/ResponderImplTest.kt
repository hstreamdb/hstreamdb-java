package io.hstream.impl

import io.hstream.HStreamClient
import io.hstream.buildBlackBoxSourceClient
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.runner.RunWith
import org.mockito.junit.MockitoJUnitRunner

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
}
