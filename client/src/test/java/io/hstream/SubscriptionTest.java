package io.hstream;

import static io.hstream.Subscription.SubscriptionOffset.EARLIEST;
import static io.hstream.Subscription.SubscriptionOffset.LATEST;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
class SubscriptionTest {
  @Test
  public void testSubscriptionBuilder() {
    Subscription.Builder builder = Subscription.newBuilder();

    // with all required parameters
    Subscription subscription = builder.subscription("sub-1").stream("stream-1").build();
    assertEquals(subscription.getSubscriptionId(), "sub-1");
    assertEquals(subscription.getStreamName(), "stream-1");
    assertEquals(subscription.getAckTimeoutSeconds(), 600);
    assertEquals(subscription.getMaxUnackedRecords(), 10000);
    assertEquals(subscription.getOffset(), LATEST);

    // with optional parameters
    Instant createdTime = Instant.now();
    subscription =
        builder.subscription("sub-2").stream("stream-2")
            .ackTimeoutSeconds(1200)
            .maxUnackedRecords(20000)
            .offset(EARLIEST)
            .createdTime(createdTime)
            .build();
    assertEquals(subscription.getSubscriptionId(), "sub-2");
    assertEquals(subscription.getStreamName(), "stream-2");
    assertEquals(subscription.getAckTimeoutSeconds(), 1200);
    assertEquals(subscription.getMaxUnackedRecords(), 20000);
    assertEquals(subscription.getOffset(), EARLIEST);
    assertEquals(subscription.getCreatedTime(), createdTime);
  }

  @Test
  public void testBuilder() {
    Subscription subscription =
        Subscription.newBuilder().subscription("subscription-id").stream("stream-name")
            .ackTimeoutSeconds(120)
            .maxUnackedRecords(5000)
            .offset(EARLIEST)
            .createdTime(Instant.now())
            .build();

    assertEquals("subscription-id", subscription.getSubscriptionId());
    assertEquals("stream-name", subscription.getStreamName());
    assertEquals(120, subscription.getAckTimeoutSeconds());
    assertEquals(5000, subscription.getMaxUnackedRecords());
    assertEquals(EARLIEST, subscription.getOffset());
  }

  @Test
  public void testBuilderMissingSubscriptionIdThrowsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> Subscription.newBuilder().stream("stream-name").build());
  }

  @Test
  public void testBuilderMissingStreamNameThrowsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> Subscription.newBuilder().subscription("subscription-id").build());
  }

  @Test
  public void testBuilderInvalidAckTimeoutSecondsThrowsIllegalStateException() {
    assertThrows(
        IllegalStateException.class,
        () ->
            Subscription.newBuilder().subscription("subscription-id").stream("stream-name")
                .ackTimeoutSeconds(0)
                .build());
    assertThrows(
        IllegalStateException.class,
        () ->
            Subscription.newBuilder().subscription("subscription-id").stream("stream-name")
                .ackTimeoutSeconds(36000)
                .build());
  }

  @Test
  public void testBuilderInvalidMaxUnAckedRecordsThrowsIllegalStateException() {
    assertThrows(
        IllegalStateException.class,
        () ->
            Subscription.newBuilder().subscription("subscription-id").stream("stream-name")
                .maxUnackedRecords(0)
                .build());
  }

  @Test
  public void testEqualsAndHashCode() {
    Subscription subscription1 =
        Subscription.newBuilder().subscription("subscription-id").stream("stream-name")
            .ackTimeoutSeconds(120)
            .maxUnackedRecords(5000)
            .offset(EARLIEST)
            .createdTime(Instant.now())
            .build();
    Subscription subscription2 =
        Subscription.newBuilder().subscription("subscription-id").stream("stream-name")
            .ackTimeoutSeconds(120)
            .maxUnackedRecords(5000)
            .offset(EARLIEST)
            .createdTime(Instant.now())
            .build();

    Subscription subscription3 =
        Subscription.newBuilder().subscription("subscription-id-2").stream("stream-name-2")
            .ackTimeoutSeconds(300)
            .maxUnackedRecords(10000)
            .offset(LATEST)
            .createdTime(Instant.now())
            .build();

    assertEquals(subscription1, subscription2);
    assertEquals(subscription1.hashCode(), subscription2.hashCode());

    assertNotEquals(subscription1, subscription3);
    assertNotEquals(subscription1.hashCode(), subscription3.hashCode());
  }
}
