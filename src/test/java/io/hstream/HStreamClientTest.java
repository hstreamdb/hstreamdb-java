package io.hstream;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HStreamClientTest {

  private static final Logger logger = LoggerFactory.getLogger(HStreamClientTest.class);
  private static final String serviceUrl = "localhost:6570";
  private static final String TEST_STREAM = "test_stream";
  private static final String TEST_SUBSCRIPTION = "test_subscription";
  private HStreamClient client;

  @BeforeEach
  public void setUp() {
    client = HStreamClient.builder().serviceUrl(serviceUrl).build();
    client.createStream(TEST_STREAM);
    Subscription subscription =
        new Subscription(
            TEST_SUBSCRIPTION,
            TEST_STREAM,
            new SubscriptionOffset(SubscriptionOffset.SpecialOffset.LATEST));
    client.createSubscription(subscription);
  }

  @AfterEach
  public void cleanUp() {
    TestUtils.deleteAllSubscriptions(client);
    client.deleteStream(TEST_STREAM);
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
  public void testWriteRawRecord() throws Exception {
    CompletableFuture<RecordId> recordIdFuture = new CompletableFuture<>();
    Consumer consumer =
        client
            .newConsumer()
            .subscription(TEST_SUBSCRIPTION)
            .rawRecordReceiver(
                (receivedRawRecord, responder) ->
                    recordIdFuture.thenAccept(
                        recordId ->
                            Assertions.assertEquals(recordId, receivedRawRecord.getRecordId())))
            .build();
    consumer.startAsync().awaitRunning();

    Producer producer = client.newProducer().stream(TEST_STREAM).build();
    Random random = new Random();
    byte[] rawRecord = new byte[100];
    random.nextBytes(rawRecord);
    RecordId recordId = producer.write(rawRecord);
    recordIdFuture.complete(recordId);

    Thread.sleep(1000);
    consumer.stopAsync().awaitTerminated();
  }

  @Test
  public void testWriteHRecord() throws Exception {

    Producer producer = client.newProducer().stream(TEST_STREAM).build();
    HRecord hRecord =
        HRecord.newBuilder().put("key1", 10).put("key2", "hello").put("key3", true).build();
    RecordId recordId = producer.write(hRecord);

    CountDownLatch countDownLatch = new CountDownLatch(1);
    Consumer consumer =
        client
            .newConsumer()
            .subscription(TEST_SUBSCRIPTION)
            .hRecordReceiver(
                (receivedHRecord, responder) -> {
                  logger.info("receivedHRecord: {}", receivedHRecord.getHRecord());
                  Assertions.assertEquals(recordId, receivedHRecord.getRecordId());
                  countDownLatch.countDown();
                })
            .build();
    consumer.startAsync().awaitRunning();

    countDownLatch.await();
    consumer.stopAsync().awaitTerminated();
  }

  @Test
  public void testDuplicateSubscribe() throws Exception {
    Consumer consumer1 =
        client
            .newConsumer()
            .subscription(TEST_SUBSCRIPTION)
            .rawRecordReceiver((receivedRawRecord, responder) -> {})
            .build();
    consumer1.startAsync().awaitRunning();

    Assertions.assertThrows(
        HStreamDBClientException.SubscribeException.class,
        () -> {
          Consumer consumer2 =
              client
                  .newConsumer()
                  .subscription(TEST_SUBSCRIPTION)
                  .rawRecordReceiver((receivedRawRecord, responder) -> {})
                  .build();
          try {
            consumer2.startAsync().awaitRunning();
          } catch (IllegalStateException e) {
            throw e.getCause();
          }
        });

    consumer1.stopAsync().awaitTerminated();
  }

  @Test
  public void testConsumerSession() throws Exception {
    Consumer consumer1 =
        client
            .newConsumer()
            .subscription(TEST_SUBSCRIPTION)
            .rawRecordReceiver((receivedRawRecord, responder) -> {})
            .build();
    consumer1.startAsync().awaitRunning();
    consumer1.stopAsync().awaitTerminated();

    Thread.sleep(3000);

    Consumer consumer2 =
        client
            .newConsumer()
            .subscription(TEST_SUBSCRIPTION)
            .rawRecordReceiver((receivedRawRecord, responder) -> {})
            .build();
    consumer2.startAsync().awaitRunning();
    consumer2.stopAsync().awaitTerminated();
  }

  //  @Test
  //  public void testResponder() throws Exception {
  //    CountDownLatch flag1 = new CountDownLatch(1);
  //    AtomicInteger consumedCount = new AtomicInteger();
  //    Consumer consumer1 =
  //        client
  //            .newConsumer()
  //            .subscription(TEST_SUBSCRIPTION)
  //            .rawRecordReceiver(
  //                (receivedRawRecord, responder) -> {
  //                  logger.info("enter process, count is {}", consumedCount.incrementAndGet());
  //                  if (consumedCount.get() == 3) {
  //                    logger.info("enter if");
  //                    responder.ack();
  //                    logger.info("finished ack");
  //                    flag1.countDown();
  //                  }
  //                })
  //            .build();
  //    consumer1.startAsync().awaitRunning();
  //
  //    Producer producer = client.newProducer().stream(TEST_STREAM).build();
  //    Random random = new Random();
  //    ArrayList<RecordId> recordIds = new ArrayList<>(5);
  //    for (int i = 0; i < 4; ++i) {
  //      byte[] rawRecord = new byte[100];
  //      random.nextBytes(rawRecord);
  //      RecordId recordId = producer.write(rawRecord);
  //      System.out.println("i: " + i + " record id: " + recordId.getBatchId());
  //      recordIds.add(recordId);
  //    }
  //
  //    flag1.await();
  //    consumer1.stopAsync().awaitTerminated();
  //
  //    Thread.sleep(3000);
  //    CountDownLatch flag2 = new CountDownLatch(1);
  //    Consumer consumer2 =
  //        client
  //            .newConsumer()
  //            .subscription(TEST_SUBSCRIPTION)
  //            .rawRecordReceiver(
  //                (receivedRawRecord, responder) -> {
  //                  System.out.println("expected: " + recordIds.get(3).getBatchId());
  //                  System.out.println("got: " + receivedRawRecord.getRecordId().getBatchId());
  //                  Assertions.assertEquals(recordIds.get(3), receivedRawRecord.getRecordId());
  //                  flag2.countDown();
  //                })
  //            .build();
  //    consumer2.startAsync().awaitRunning();
  //
  //    flag2.await();
  //    consumer2.stopAsync().awaitTerminated();
  //  }

  @Test
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
            .sql("select * from " + TEST_STREAM + " where temperature > 30 emit changes;")
            .resultObserver(observer)
            .build();

    queryer.startAsync().awaitRunning();

    logger.info("begin to write");

    Producer producer = client.newProducer().stream(TEST_STREAM).build();
    HRecord hRecord1 = HRecord.newBuilder().put("temperature", 29).put("humidity", 20).build();
    HRecord hRecord2 = HRecord.newBuilder().put("temperature", 34).put("humidity", 21).build();
    HRecord hRecord3 = HRecord.newBuilder().put("temperature", 35).put("humidity", 22).build();
    producer.write(hRecord1);
    producer.write(hRecord2);
    producer.write(hRecord3);

    try {
      Thread.sleep(3000);
      Assertions.assertEquals(2, receivedCount.get());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    queryer.stopAsync().awaitTerminated();
  }

  @Test
  public void testWriteBatchRawRecord() throws Exception {

    Producer producer =
        client.newProducer().stream(TEST_STREAM).enableBatch().recordCountLimit(10).build();
    Random random = new Random();
    final int count = 100;
    CompletableFuture<RecordId>[] recordIdFutures = new CompletableFuture[count];
    for (int i = 0; i < count; ++i) {
      byte[] rawRecord = new byte[100];
      random.nextBytes(rawRecord);
      CompletableFuture<RecordId> future = producer.writeAsync(rawRecord);
      recordIdFutures[i] = future;
    }
    CompletableFuture.allOf(recordIdFutures).join();

    logger.info("producer finish");

    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger index = new AtomicInteger();
    Consumer consumer =
        client
            .newConsumer()
            .subscription(TEST_SUBSCRIPTION)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  Assertions.assertEquals(
                      recordIdFutures[index.getAndIncrement()].join(),
                      receivedRawRecord.getRecordId());
                  if (index.get() == count - 1) {
                    latch.countDown();
                  }
                })
            .build();
    consumer.startAsync().awaitRunning();

    latch.await();
    consumer.stopAsync().awaitTerminated();
  }

  @Test
  public void testWriteBatchRawRecordMultiThread() throws Exception {
    Producer producer =
        client.newProducer().stream(TEST_STREAM).enableBatch().recordCountLimit(10).build();
    Random random = new Random();
    final int count = 100;
    CompletableFuture<RecordId>[] recordIdFutures = new CompletableFuture[count];

    Thread thread1 =
        new Thread(
            () -> {
              for (int i = 0; i < count / 2; ++i) {
                byte[] rawRecord = new byte[100];
                random.nextBytes(rawRecord);
                CompletableFuture<RecordId> future = producer.writeAsync(rawRecord);
                recordIdFutures[i] = future;
              }
            });

    Thread thread2 =
        new Thread(
            () -> {
              for (int i = count / 2; i < count; ++i) {
                byte[] rawRecord = new byte[100];
                random.nextBytes(rawRecord);
                CompletableFuture<RecordId> future = producer.writeAsync(rawRecord);
                recordIdFutures[i] = future;
              }
            });

    thread1.start();
    thread2.start();

    AtomicInteger readCount = new AtomicInteger();
    Consumer consumer =
        client
            .newConsumer()
            .subscription(TEST_SUBSCRIPTION)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  readCount.incrementAndGet();
                })
            .build();
    consumer.startAsync().awaitRunning();

    Thread.sleep(3000);
    Assertions.assertEquals(count, readCount.get());
    consumer.stopAsync().awaitTerminated();
  }

  @Test
  public void testFlush() throws Exception {
    Producer producer =
        client.newProducer().stream(TEST_STREAM).enableBatch().recordCountLimit(100).build();
    Random random = new Random();
    final int count = 10;
    CompletableFuture<RecordId>[] recordIdFutures = new CompletableFuture[count];
    for (int i = 0; i < count; ++i) {
      byte[] rawRecord = new byte[100];
      random.nextBytes(rawRecord);
      CompletableFuture<RecordId> future = producer.writeAsync(rawRecord);
      recordIdFutures[i] = future;
    }
    producer.flush();

    // CompletableFuture.allOf(recordIdFutures).join();

    logger.info("producer finish");

    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger index = new AtomicInteger();
    Consumer consumer =
        client
            .newConsumer()
            .subscription(TEST_SUBSCRIPTION)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  Assertions.assertEquals(
                      recordIdFutures[index.getAndIncrement()].join(),
                      receivedRawRecord.getRecordId());
                  if (index.get() == count - 1) {
                    latch.countDown();
                  }
                })
            .build();
    consumer.startAsync().awaitRunning();

    latch.await();
    consumer.stopAsync().awaitTerminated();
  }

  @Test
  public void testFlushMultiThread() throws Exception {
    AtomicInteger readCount = new AtomicInteger();
    Consumer consumer =
        client
            .newConsumer()
            .subscription(TEST_SUBSCRIPTION)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  readCount.incrementAndGet();
                })
            .build();
    consumer.startAsync().awaitRunning();

    Producer producer =
        client.newProducer().stream(TEST_STREAM).enableBatch().recordCountLimit(100).build();
    Random random = new Random();
    final int count = 10;

    Thread thread1 =
        new Thread(
            () -> {
              for (int i = 0; i < count; ++i) {
                byte[] rawRecord = new byte[100];
                random.nextBytes(rawRecord);
                producer.writeAsync(rawRecord);
              }
              producer.flush();
            });

    Thread thread2 =
        new Thread(
            () -> {
              for (int i = 0; i < count; ++i) {
                byte[] rawRecord = new byte[100];
                random.nextBytes(rawRecord);
                producer.writeAsync(rawRecord);
              }
              producer.flush();
            });

    thread1.start();
    thread2.start();

    Thread.sleep(5000);
    Assertions.assertEquals(count * 2, readCount.get());
    consumer.stopAsync().awaitTerminated();
  }
}
