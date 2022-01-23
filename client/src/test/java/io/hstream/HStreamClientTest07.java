package io.hstream;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HStreamClientTest07 {

  private static final Logger logger = LoggerFactory.getLogger(HStreamClientTest07.class);
  private static final String serviceUrl = "127.0.0.1:6570";
  private static String STREAM_NAME_PREFIX = "test_stream_";
  private static String SUBSCRIPTION_PREFIX = "test_sub_";

  private static AtomicInteger counter = new AtomicInteger();

  static String newStreamName() {
    return STREAM_NAME_PREFIX + counter.incrementAndGet();
  }

  static String newSubscriptionId() {
    return SUBSCRIPTION_PREFIX + counter.incrementAndGet();
  }

  @Test
  void createStreamTest() throws Exception {
    HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();
    client.createStream(newStreamName());
    client.close();
  }

  @Test
  void writeTest() throws Exception {
    HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();
    String streamName = newStreamName();
    client.createStream(streamName);

    Random random = new Random();
    byte[] payload = new byte[100];
    random.nextBytes(payload);
    Producer producer = client.newProducer().stream(streamName).build();
    CompletableFuture<RecordId> future =
        producer.write(Record.newBuilder().key("k1").rawRecord(payload).build());
    RecordId recordId = future.join();
    logger.info("write successfully, got recordId: " + recordId.toString());
    client.close();
  }

  @Test
  void createSubscriptionTest() throws Exception {
    HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();
    var streamName = newStreamName();
    var subscriptionId = newSubscriptionId();
    client.createStream(streamName);
    client.createSubscription(
        Subscription.newBuilder().subscription(subscriptionId).stream(streamName).build());
    client.close();
  }

  @Test
  void readTest() throws Exception {
    var streamName = newStreamName();
    var subscriptionId = newSubscriptionId();
    HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();
    client.createStream(streamName);
    client.createSubscription(
        Subscription.newBuilder().subscription(subscriptionId).stream(streamName).build());

    Producer producer = client.newProducer().stream(streamName).build();
    int recordCount = 10;
    for (int i = 0; i < recordCount; ++i) {
      RecordId recordId =
          producer
              .write(
                  Record.newBuilder()
                      .key("k1")
                      .rawRecord(("record-" + i).getBytes(StandardCharsets.UTF_8))
                      .build())
              .join();
      logger.info("wrote record {} , get recordId: {}", i, recordId);
    }

    CountDownLatch latch = new CountDownLatch(recordCount);
    Consumer consumer =
        client
            .newConsumer()
            .subscription(subscriptionId)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  var recordId = receivedRawRecord.getRecordId();
                  logger.info("read record {}", recordId);
                  responder.ack();
                  latch.countDown();
                })
            .build();
    consumer.startAsync().awaitRunning();

    latch.await();
    consumer.stopAsync().awaitTerminated();

    client.close();
  }

  @Test
  @RepeatedTest(3)
  void readMultipleShardsTest() throws Exception {
    var streamName = newStreamName();
    var subscriptionId = newSubscriptionId();
    HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();
    client.createStream(streamName);
    client.createSubscription(
        Subscription.newBuilder().subscription(subscriptionId).stream(streamName).build());

    Producer producer = client.newProducer().stream(streamName).build();
    int shardCount = 3;
    int recordCount = 10;
    for (int j = 0; j < shardCount; ++j) {
      String orderingKey = "key-" + j;
      for (int i = 0; i < recordCount; ++i) {
        RecordId recordId =
            producer
                .write(
                    Record.newBuilder()
                        .key(orderingKey)
                        .rawRecord(("record-" + i).getBytes(StandardCharsets.UTF_8))
                        .build())
                .join();
        logger.info("wrote record {} with key {} , get recordId: {}", i, orderingKey, recordId);
      }
    }

    CountDownLatch latch = new CountDownLatch(recordCount * shardCount);
    Consumer consumer =
        client
            .newConsumer()
            .subscription(subscriptionId)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  var recordId = receivedRawRecord.getRecordId();
                  logger.info("read record {}", recordId);
                  responder.ack();
                  latch.countDown();
                })
            .build();
    consumer.startAsync().awaitRunning();

    latch.await();
    consumer.stopAsync().awaitTerminated();

    client.close();
  }

  @Test
  void consumerRebalenceTest() throws Exception {
    var streamName = newStreamName();
    var subscriptionId = newSubscriptionId();
    HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();
    client.createStream(streamName);
    client.createSubscription(
        Subscription.newBuilder().subscription(subscriptionId).stream(streamName).build());

    Producer producer = client.newProducer().stream(streamName).build();
    int shardCount = 3;
    int recordCount = 10;
    for (int j = 0; j < shardCount; ++j) {
      String orderingKey = "key-" + j;
      for (int i = 0; i < recordCount; ++i) {
        RecordId recordId =
            producer
                .write(
                    Record.newBuilder()
                        .key(orderingKey)
                        .rawRecord(("record-" + i).getBytes(StandardCharsets.UTF_8))
                        .build())
                .join();
        logger.info("wrote record {} with key {} , get recordId: {}", i, orderingKey, recordId);
      }
    }

    CountDownLatch latch = new CountDownLatch(2 * recordCount * shardCount);
    new Thread(
            () -> {
              Consumer consumer =
                  client
                      .newConsumer()
                      .subscription(subscriptionId)
                      .rawRecordReceiver(
                          (receivedRawRecord, responder) -> {
                            var recordId = receivedRawRecord.getRecordId();
                            logger.info("c1 read record {}", recordId);
                            responder.ack();
                            latch.countDown();
                          })
                      .build();
              consumer.startAsync().awaitRunning();

              try {
                latch.await();
              } catch (InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
              }
              consumer.stopAsync().awaitTerminated();
            })
        .start();

    Thread.sleep(5000);
    new Thread(
            () -> {
              Consumer consumer =
                  client
                      .newConsumer()
                      .subscription(subscriptionId)
                      .rawRecordReceiver(
                          (receivedRawRecord, responder) -> {
                            var recordId = receivedRawRecord.getRecordId();
                            logger.info("c2 read record {}", recordId);
                            responder.ack();
                            latch.countDown();
                          })
                      .build();
              consumer.startAsync().awaitRunning();

              try {
                latch.await();
              } catch (InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
              }
              consumer.stopAsync().awaitTerminated();
            })
        .start();

    for (int j = 0; j < shardCount; ++j) {
      String orderingKey = "key-" + j;
      for (int i = 0; i < recordCount; ++i) {
        RecordId recordId =
            producer
                .write(
                    Record.newBuilder()
                        .key(orderingKey)
                        .rawRecord(("record-" + i).getBytes(StandardCharsets.UTF_8))
                        .build())
                .join();
        logger.info("wrote record {} with key {} , get recordId: {}", i, orderingKey, recordId);
      }
    }

    latch.await();
    client.close();
  }
}
