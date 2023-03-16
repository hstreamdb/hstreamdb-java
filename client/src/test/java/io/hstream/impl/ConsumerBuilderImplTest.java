package io.hstream.impl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerBuilderImplTest {

  @Test
  public void testBuildWithoutSubscriptionShouldThrowException() {
    ConsumerBuilderImpl consumerBuilder = new ConsumerBuilderImpl(null);
    consumerBuilder.hRecordReceiver((_x, _y) -> {});
    Assertions.assertThrows(IllegalArgumentException.class, consumerBuilder::build);
  }

  @Test
  public void testBuildWithoutRecordReceiverShouldThrowException() {
    ConsumerBuilderImpl consumerBuilder = new ConsumerBuilderImpl(null);
    consumerBuilder.subscription("test-subscription");
    Assertions.assertThrows(IllegalArgumentException.class, consumerBuilder::build);
  }

  @Test
  @Disabled
  // TODO, mock init
  public void testBuildWithNullNameShouldAssignRandomUUID() {
    ConsumerBuilderImpl consumerBuilder = new ConsumerBuilderImpl(null);
    consumerBuilder.subscription("test-subscription");
    consumerBuilder.hRecordReceiver((_x, _y) -> {});
    consumerBuilder.name(null);
    ConsumerKtImpl consumer = (ConsumerKtImpl) consumerBuilder.build();
    Assertions.assertNotNull(consumer.getConsumerName());
  }

  @Test
  @Disabled
  // TODO, mock init
  public void testBuildWithInvalidAckBufferSizeShouldSetToMinimum() {
    ConsumerBuilderImpl consumerBuilder = new ConsumerBuilderImpl(null);
    consumerBuilder.subscription("test-subscription");
    consumerBuilder.hRecordReceiver((_x, _y) -> {});
    consumerBuilder.ackBufferSize(-1);
    ConsumerKtImpl consumer = (ConsumerKtImpl) consumerBuilder.build();
    Assertions.assertEquals(1, consumer.getAckBufferSize());
  }
}
