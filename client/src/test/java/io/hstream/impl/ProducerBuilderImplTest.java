package io.hstream.impl;

import static io.hstream.HServerMockKt.buildMockedClient;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.hstream.HStreamClient;
import io.hstream.Producer;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProducerBuilderImplTest {

  @Test
  public void testBuildWithValidInput() {
    HStreamClient client = buildMockedClient();
    ProducerBuilderImpl builder = new ProducerBuilderImpl((HStreamClientKtImpl) client);
    Producer producer = builder.stream("test-stream").requestTimeoutMs(1000).build();
    assertNotNull(producer);
  }

  @Test
  public void testBuildWithMissingStreamName() {
    HStreamClient client = buildMockedClient();
    ProducerBuilderImpl builder = new ProducerBuilderImpl((HStreamClientKtImpl) client);
    assertThrows(IllegalArgumentException.class, () -> builder.requestTimeoutMs(1000).build());
  }

  @Test
  public void testBuildWithInvalidRequestTimeoutMs() {
    HStreamClient client = buildMockedClient();
    ProducerBuilderImpl builder = new ProducerBuilderImpl((HStreamClientKtImpl) client);
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          builder.stream("test-stream").requestTimeoutMs(-1).build();
        });
  }

  @Test
  public void testBuildWithNullClient() {
    ProducerBuilderImpl builder = new ProducerBuilderImpl(null);
    assertThrows(
        NullPointerException.class,
        () -> builder.stream("test-stream").requestTimeoutMs(1000).build());
  }

  @Test
  public void testBuildWithDefaultRequestTimeoutMs() {
    HStreamClient client = buildMockedClient();
    ProducerBuilderImpl builder = new ProducerBuilderImpl((HStreamClientKtImpl) client);
    Producer producer = builder.stream("test-stream").build();
    assertNotNull(producer);
  }

  @Test
  public void testBuildWithMaxRequestTimeoutMs() {
    HStreamClient client = buildMockedClient();
    ProducerBuilderImpl builder = new ProducerBuilderImpl((HStreamClientKtImpl) client);
    Producer producer = builder.stream("test-stream").requestTimeoutMs(Long.MAX_VALUE).build();
    assertNotNull(producer);
  }
}
