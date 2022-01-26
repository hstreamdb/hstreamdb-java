package io.hstream;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class HStreamClientTest {

  private static final Logger logger = LoggerFactory.getLogger(HStreamClientTest.class);
  private static final String serviceUrl = "localhost:6570";
  private static final String TEST_STREAM_PREFIX = "TEST_STREAM_";
  private static final String TEST_SUBSCRIPTION_PREFIX = "TEST_SUB_";
  private HStreamClient client;
  private String testStreamName;
  private String testSubscriptionId;

  @BeforeEach
  public void setUp() {
    client = HStreamClient.builder().serviceUrl(serviceUrl).build();
    String suffix = RandomStringUtils.randomAlphanumeric(10);
    testStreamName = TEST_STREAM_PREFIX + suffix;
    testSubscriptionId = TEST_SUBSCRIPTION_PREFIX + suffix;
    client.createStream(testStreamName);
    Subscription subscription =
        Subscription.newBuilder().subscription(testSubscriptionId).stream(testStreamName)
            .offset(new SubscriptionOffset(SubscriptionOffset.SpecialOffset.LATEST))
            .ackTimeoutSeconds(10)
            .build();
    client.createSubscription(subscription);
  }

  @AfterEach
  public void cleanUp() {
    TestUtils.deleteAllSubscriptions(client);
    client.deleteStream(testStreamName);
  }

  public static ArrayList<RecordId> doProduceAndGatherRid(
      Producer producer, int payloadSize, int recordsNums) {
    var rids = new ArrayList<RecordId>();
    Random rand = new Random();
    byte[] rRec = new byte[payloadSize];
    var writes = new ArrayList<CompletableFuture<RecordId>>();
    for (int i = 0; i < recordsNums; i++) {
      rand.nextBytes(rRec);
      writes.add(producer.write(rRec));
    }
    writes.forEach(w -> rids.add(w.join()));
    return rids;
  }

  // @Test
  // public void testReceiverException() throws Exception {
  //   Consumer consumer =
  //       client
  //           .newConsumer()
  //           .subscription(TEST_SUBSCRIPTION)
  //           .rawRecordReceiver(
  //               (receivedRawRecord, responder) -> {
  //                 throw new RuntimeException("receiver exception");
  //               })
  //           .build();
  //   Service.Listener listener =
  //       new Service.Listener() {
  //         @Override
  //         public void starting() {
  //           super.starting();
  //         }

  //         @Override
  //         public void running() {
  //           super.running();
  //         }

  //         @Override
  //         public void stopping(Service.State from) {
  //           super.stopping(from);
  //         }

  //         @Override
  //         public void terminated(Service.State from) {
  //           super.terminated(from);
  //         }

  //         @Override
  //         public void failed(Service.State from, Throwable failure) {
  //           logger.error("consumer failed from state {} ", from, failure);
  //           super.failed(from, failure);
  //           // consumer.stopAsync();
  //         }
  //       };

  //   consumer.addListener(listener, Executors.newSingleThreadExecutor());
  //   consumer.startAsync().awaitRunning();

  //   Producer producer = client.newProducer().stream(TEST_STREAM).build();
  //   Random random = new Random();
  //   byte[] rawRecord = new byte[100];
  //   for (int i = 0; i < 5; ++i) {
  //     Thread.sleep(5000);
  //     random.nextBytes(rawRecord);
  //     producer.write(rawRecord);
  //   }

  //   consumer.awaitTerminated(1, TimeUnit.SECONDS);
  //   consumer.stopAsync().awaitTerminated();
  // }

  @Test
  @Order(1)
  public void testWriteRawRecord() throws Exception {
    Producer producer = client.newProducer().stream(testStreamName).build();
    Random random = new Random();
    byte[] rawRecord = new byte[100];
    random.nextBytes(rawRecord);
    RecordId recordId = producer.write(rawRecord).join();
    logger.info("write record: {}", recordId);

    CountDownLatch latch = new CountDownLatch(1);
    Consumer consumer =
        client
            .newConsumer()
            .subscription(testSubscriptionId)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  logger.info("recv {}", receivedRawRecord.getRecordId());
                  Assertions.assertEquals(recordId, receivedRawRecord.getRecordId());
                  Assertions.assertArrayEquals(rawRecord, receivedRawRecord.getRawRecord());
                  responder.ack();
                  latch.countDown();
                })
            .build();
    consumer.startAsync().awaitRunning();

    latch.await();
    consumer.stopAsync().awaitTerminated();
  }

  @Test
  @Order(2)
  public void testWriteHRecord() throws Exception {

    Producer producer = client.newProducer().stream(testStreamName).build();
    HRecord hRecord =
        HRecord.newBuilder().put("key1", 10).put("key2", "hello").put("key3", true).build();
    RecordId recordId = producer.write(hRecord).join();

    CountDownLatch countDownLatch = new CountDownLatch(1);
    Consumer consumer =
        client
            .newConsumer()
            .subscription(testSubscriptionId)
            .hRecordReceiver(
                (receivedHRecord, responder) -> {
                  Assertions.assertEquals(recordId, receivedHRecord.getRecordId());
                  responder.ack();
                  countDownLatch.countDown();
                })
            .build();
    consumer.startAsync().awaitRunning();

    countDownLatch.await();
    consumer.stopAsync().awaitTerminated();
  }

  @Test
  @Order(3)
  public void testWriteBatchRawRecord() throws Exception {
    BufferedProducer producer =
        client.newBufferedProducer().stream(testStreamName).recordCountLimit(10).build();
    final int count = 100;
    var ids = doProduceAndGatherRid(producer, 100, count);
    producer.close();

    logger.info("producer finish, ids:{}", ids.size());

    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger index = new AtomicInteger();
    Consumer consumer =
        client
            .newConsumer()
            .subscription(testSubscriptionId)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  Assertions.assertEquals(
                      ids.get(index.getAndIncrement()), receivedRawRecord.getRecordId());
                  responder.ack();
                  if (index.get() == count) {
                    latch.countDown();
                  }
                })
            .build();
    consumer.startAsync().awaitRunning();

    latch.await();
    consumer.stopAsync().awaitTerminated();
  }

  @Test
  @Order(4)
  public void testWriteBatchRawRecordMultiThread() throws Exception {
    BufferedProducer producer =
        client.newBufferedProducer().stream(testStreamName)
            .recordCountLimit(10)
            .flushIntervalMs(10)
            .build();
    Random random = new Random();
    final int count = 100;
    CompletableFuture<RecordId>[] recordIdFutures = new CompletableFuture[count];

    Thread thread1 =
        new Thread(
            () -> {
              for (int i = 0; i < count / 2; ++i) {
                byte[] rawRecord = new byte[100];
                random.nextBytes(rawRecord);
                CompletableFuture<RecordId> future = producer.write(rawRecord);
                recordIdFutures[i] = future;
              }
            });

    Thread thread2 =
        new Thread(
            () -> {
              for (int i = count / 2; i < count; ++i) {
                byte[] rawRecord = new byte[100];
                random.nextBytes(rawRecord);
                CompletableFuture<RecordId> future = producer.write(rawRecord);
                recordIdFutures[i] = future;
              }
            });

    thread1.start();
    thread2.start();

    thread1.join();
    thread2.join();

    CompletableFuture.allOf(recordIdFutures).join();
    producer.close();

    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger readCount = new AtomicInteger();
    Consumer consumer =
        client
            .newConsumer()
            .subscription(testSubscriptionId)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  readCount.incrementAndGet();
                  responder.ack();
                  if (readCount.get() == count) {
                    latch.countDown();
                  }
                })
            .build();
    consumer.startAsync().awaitRunning();

    latch.await();
    consumer.stopAsync().awaitTerminated();
  }

  @Test
  @Order(5)
  public void testConsumerGroup() throws Exception {
    Producer producer = client.newProducer().stream(testStreamName).build();
    Random random = new Random();
    byte[] rawRecord = new byte[100];
    final int count = 10;
    for (int i = 0; i < count; ++i) {
      random.nextBytes(rawRecord);
      producer.write(rawRecord).join();
    }

    logger.info("write done");

    AtomicInteger readCount = new AtomicInteger();
    Consumer consumer1 =
        client
            .newConsumer()
            .subscription(testSubscriptionId)
            .name("consumer-1")
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  logger.info("consumer-1 recv {}", receivedRawRecord.getRecordId().getBatchId());
                  readCount.incrementAndGet();
                  responder.ack();
                })
            .build();

    Consumer consumer2 =
        client
            .newConsumer()
            .subscription(testSubscriptionId)
            .name("consumer-2")
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  logger.info("consumer-2 recv {}", receivedRawRecord.getRecordId().getBatchId());
                  readCount.incrementAndGet();
                  responder.ack();
                })
            .build();

    Consumer consumer3 =
        client
            .newConsumer()
            .subscription(testSubscriptionId)
            .name("consumer-3")
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  logger.info("consumer-3 recv {}", receivedRawRecord.getRecordId().getBatchId());
                  readCount.incrementAndGet();
                  responder.ack();
                })
            .build();

    consumer1.startAsync().awaitRunning();
    consumer2.startAsync().awaitRunning();
    consumer3.startAsync().awaitRunning();

    logger.info("consumers ready");

    Thread.sleep(5000);

    consumer1.stopAsync().awaitTerminated();
    consumer2.stopAsync().awaitTerminated();
    consumer3.stopAsync().awaitTerminated();

    Assertions.assertEquals(count, readCount.get());
  }

  @Disabled("wait for fix HS-456")
  @Test
  @Order(6)
  public void testStreamQuery() throws Exception {
    AtomicInteger receivedCount = new AtomicInteger(0);
    Observer<HRecord> observer =
        new Observer<HRecord>() {
          @Override
          public void onNext(HRecord value) {
            logger.info("get hrecord: {}", value);
            receivedCount.incrementAndGet();
          }

          @Override
          public void onError(Throwable t) {
            logger.error("error: ", t);
          }

          @Override
          public void onCompleted() {}
        };

    Queryer queryer =
        client
            .newQueryer()
            .sql("select * from " + testStreamName + " where temperature > 30 emit changes;")
            .resultObserver(observer)
            .build();

    queryer.startAsync().awaitRunning();

    logger.info("begin to write");

    Producer producer = client.newProducer().stream(testStreamName).build();
    HRecord hRecord1 = HRecord.newBuilder().put("temperature", 29).put("humidity", 20).build();
    HRecord hRecord2 = HRecord.newBuilder().put("temperature", 34).put("humidity", 21).build();
    HRecord hRecord3 = HRecord.newBuilder().put("temperature", 35).put("humidity", 22).build();
    producer.write(hRecord1);
    producer.write(hRecord2);
    producer.write(hRecord3);

    try {
      Thread.sleep(5000);
      Assertions.assertEquals(2, receivedCount.get());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    queryer.stopAsync().awaitTerminated();
  }

  @Test
  @Order(7)
  public void testConsumerInTurn() throws Exception {
    final int recordCount = 10;
    Producer producer = client.newProducer().stream(testStreamName).build();
    Random random = new Random();
    for (int i = 0; i < recordCount; ++i) {
      byte[] rawRecord = new byte[100];
      random.nextBytes(rawRecord);
      producer.write(rawRecord).join();
    }

    final int maxReceivedCountC1 = 3;
    CountDownLatch latch1 = new CountDownLatch(1);
    AtomicInteger c1ReceivedRecordCount = new AtomicInteger(0);
    Consumer consumer1 =
        client
            .newConsumer()
            .subscription(testSubscriptionId)
            .name("consumer1")
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  if (c1ReceivedRecordCount.get() < maxReceivedCountC1) {
                    responder.ack();
                    if (c1ReceivedRecordCount.incrementAndGet() == maxReceivedCountC1) {
                      latch1.countDown();
                    }
                  }
                })
            .build();
    consumer1.startAsync().awaitRunning();
    latch1.await();
    consumer1.stopAsync().awaitTerminated();

    Thread.sleep(3000);

    final int maxReceivedCountC2 = recordCount - maxReceivedCountC1;
    CountDownLatch latch2 = new CountDownLatch(1);
    AtomicInteger c2ReceivedRecordCount = new AtomicInteger(0);
    Consumer consumer2 =
        client
            .newConsumer()
            .subscription(testSubscriptionId)
            .name("consumer2")
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  if (c2ReceivedRecordCount.get() < maxReceivedCountC2) {
                    responder.ack();
                    if (c2ReceivedRecordCount.incrementAndGet() == maxReceivedCountC2) {
                      latch2.countDown();
                    }
                  }
                })
            .build();
    consumer2.startAsync().awaitRunning();
    latch2.await();
    consumer2.stopAsync().awaitTerminated();

    Assertions.assertEquals(recordCount, c1ReceivedRecordCount.get() + c2ReceivedRecordCount.get());
  }

  @Test
  @Order(8)
  public void testWriteBatchRawRecordBasedTimer() throws Exception {
    BufferedProducer producer =
        client.newBufferedProducer().stream(testStreamName)
            .recordCountLimit(100)
            .flushIntervalMs(100)
            .build();
    final int count = 10;
    var ids = doProduceAndGatherRid(producer, 100, count);

    logger.info("producer finish");
    producer.close();

    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger index = new AtomicInteger();
    Consumer consumer =
        client
            .newConsumer()
            .subscription(testSubscriptionId)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  Assertions.assertEquals(
                      ids.get(index.getAndIncrement()), receivedRawRecord.getRecordId());
                  responder.ack();
                  if (index.get() == count) {
                    latch.countDown();
                  }
                })
            .build();
    consumer.startAsync().awaitRunning();

    latch.await();
    consumer.stopAsync().awaitTerminated();
  }

  @Test
  @Order(9)
  public void testWriteBatchRawRecordBasedBytesSize() throws Exception {
    BufferedProducer producer =
        client.newBufferedProducer().stream(testStreamName)
            .recordCountLimit(100)
            .flushIntervalMs(-1)
            .maxBytesSize(4096)
            .build();
    Random random = new Random();
    final int count = 42;
    CompletableFuture<RecordId>[] recordIdFutures = new CompletableFuture[count];
    for (int i = 0; i < count; ++i) {
      byte[] rawRecord = new byte[100];
      random.nextBytes(rawRecord);
      CompletableFuture<RecordId> future = producer.write(rawRecord);
      recordIdFutures[i] = future;
    }
    for (int i = 0; i < count - 1; ++i) {
      recordIdFutures[i].join();
    }

    try {
      recordIdFutures[41].get(3, TimeUnit.SECONDS);
      assert false;
    } catch (TimeoutException ignored) {
    }
    producer.flush();
    recordIdFutures[41].join();
    producer.close();

    logger.info("producer finish");

    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger index = new AtomicInteger();
    Consumer consumer =
        client
            .newConsumer()
            .subscription(testSubscriptionId)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  Assertions.assertEquals(
                      recordIdFutures[index.getAndIncrement()].join(),
                      receivedRawRecord.getRecordId());
                  responder.ack();
                  if (index.get() == count) {
                    latch.countDown();
                  }
                })
            .build();
    consumer.startAsync().awaitRunning();

    latch.await();
    consumer.stopAsync().awaitTerminated();
  }
}
