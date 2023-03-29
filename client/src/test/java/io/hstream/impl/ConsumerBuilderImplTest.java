package io.hstream.impl;

import static io.hstream.HServerMockKt.buildMockedClient;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.hstream.ConsumerBuilder;
import io.hstream.HStreamClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerBuilderImplTest {

  @Test
  public void testBuildWithoutSubscriptionShouldThrowException() {
    ConsumerBuilderImpl consumerBuilder = new ConsumerBuilderImpl(null);
    consumerBuilder.hRecordReceiver((_x, _y) -> {});
    assertThrows(IllegalArgumentException.class, consumerBuilder::build);
  }

  @Test
  public void testBuildOk() {
    try (HStreamClient client = buildMockedClient()) {
      ConsumerBuilder consumerBuilder = client.newConsumer();
      consumerBuilder
          .subscription("some_sub")
          .hRecordReceiver(
              (record, recv) -> {
                recv.ack();
              });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testReceiverShouldNotBeBothNull() {
    ConsumerBuilderImpl consumerBuilder = new ConsumerBuilderImpl(null);
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          consumerBuilder.subscription("some_sub").build();
        });
  }

  @Test
  public void testBuildWithoutRecordReceiverShouldThrowException() {
    ConsumerBuilderImpl consumerBuilder = new ConsumerBuilderImpl(null);
    consumerBuilder.subscription("test-subscription");
    assertThrows(IllegalArgumentException.class, consumerBuilder::build);
  }

  @Test
  public void testBuildWithNullNameShouldAssignRandomUUID() throws Exception {
    try (HStreamClient client = buildMockedClient()) {
      ConsumerBuilder consumerBuilder = client.newConsumer();
      consumerBuilder.subscription("test-subscription");
      consumerBuilder.hRecordReceiver((_x, _y) -> {});
      consumerBuilder.name(null);
      ConsumerKtImpl consumer = (ConsumerKtImpl) consumerBuilder.build();
      Assertions.assertNotNull(consumer.getConsumerName());
    }
  }

  @Test
  public void testBuildWithInvalidAckBufferSizeShouldSetToMinimum() throws Exception {
    try (HStreamClient client = buildMockedClient()) {
      ConsumerBuilder consumerBuilder = client.newConsumer();
      consumerBuilder.subscription("test-subscription");
      consumerBuilder.hRecordReceiver((_x, _y) -> {});
      consumerBuilder.ackBufferSize(-1);
      ConsumerKtImpl consumer = (ConsumerKtImpl) consumerBuilder.build();
      Assertions.assertEquals(1, consumer.getAckBufferSize());
    }
  }
}
