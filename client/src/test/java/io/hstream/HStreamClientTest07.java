package io.hstream;

import static io.hstream.TestUtils.*;

import com.google.common.collect.Iterables;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Disabled("hs-928, hs-932")
public class HStreamClientTest07 {

  private static final Logger logger = LoggerFactory.getLogger(HStreamClientTest07.class);
  private static final String serviceUrl = "127.0.0.1:6570";

  @Test
  void createStreamTest() throws Exception {
    HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();
    randStream(client);
    client.close();
  }

  @Test
  void writeTest() throws Exception {
    HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();
    String streamName = randStream(client);

    Random random = new Random();
    byte[] payload = new byte[100];
    random.nextBytes(payload);
    Producer producer = client.newProducer().stream(streamName).build();
    CompletableFuture<RecordId> future =
        producer.write(Record.newBuilder().orderingKey("k1").rawRecord(payload).build());
    RecordId recordId = future.join();
    logger.info("write successfully, got recordId: " + recordId.toString());
    client.close();
  }

  @Test
  void createSubscriptionTest() throws Exception {
    HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();
    var streamName = randStream(client);
    randSubscription(client, streamName);
    client.close();
  }

  @Test
  void readTest() throws Exception {
    HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();
    var streamName = randStream(client);
    var subscriptionId = randSubscription(client, streamName);

    Producer producer = client.newProducer().stream(streamName).build();
    int recordCount = 10;
    for (int i = 0; i < recordCount; ++i) {
      RecordId recordId =
          producer
              .write(
                  Record.newBuilder()
                      .orderingKey("k1")
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
    HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();
    var streamName = randStream(client);
    var subscriptionId = randSubscription(client, streamName);

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
                        .orderingKey(orderingKey)
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
    HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();
    var streamName = randStream(client);
    var subscriptionId = randSubscription(client, streamName);

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
                        .orderingKey(orderingKey)
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
                        .orderingKey(orderingKey)
                        .rawRecord(("record-" + i).getBytes(StandardCharsets.UTF_8))
                        .build())
                .join();
        logger.info("wrote record {} with key {} , get recordId: {}", i, orderingKey, recordId);
      }
    }

    latch.await();
    client.close();
  }

  // -------------------------------------------------------------------------

  @Test
  void dynamicallyAddPartitionTest() throws Exception {
    // Prepare env
    HStreamClient hStreamClient = HStreamClient.builder().serviceUrl(serviceUrl).build();
    var stream = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, stream);
    int shardCount = 10;
    int recordCount = 100;

    // Read
    List<Integer> readRes = new ArrayList<>();
    CountDownLatch notify = new CountDownLatch(recordCount);
    var lock = new ReentrantLock();
    Consumer consumer =
        createConsumerCollectIntegerPayload(
            logger, hStreamClient, subscription, readRes, notify, lock);
    consumer.startAsync().awaitRunning();

    // Write
    Producer producer = hStreamClient.newProducer().stream(stream).build();
    Random rand = new Random();
    HashMap<String, List<Integer>> writeRes = new HashMap<>();
    for (int i = 0; i < recordCount; ++i) {
      var key = "key-" + rand.nextInt(shardCount);
      var rid = produceIntegerAndGatherRid(producer, i, key);
      logger.info("=== Write to {}, value={}, id={}.", key, i, rid);
      if (writeRes.containsKey(key)) {
        writeRes.get(key).add(i);
      } else {
        writeRes.put(key, new ArrayList<>(Arrays.asList(i)));
      }
    }
    notify.await(20, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();

    // Analisis
    logger.info("===== Write Stats =====");
    int totalWrite = 0;
    for (String key : writeRes.keySet()) {
      var thisValue = writeRes.get(key);
      logger.info("{}: {}. Len={}", key, thisValue, thisValue.size());
      totalWrite += thisValue.size();
    }
    logger.info("Total Write = {}", totalWrite);

    logger.info("===== Read Stats ======");
    logger.info("{}", readRes);
    logger.info("Total Read = {}", readRes.size());

    // Every item written to the database is read out
    HashSet<Integer> writeResAsSet = new HashSet<>();
    for (var thisValue : writeRes.values()) {
      writeResAsSet.addAll(thisValue);
    }
    HashSet<Integer> diffSet = new HashSet<>(writeResAsSet);
    diffSet.removeAll(readRes);
    logger.info("Difference between write and read: {}", diffSet);
    Assertions.assertEquals(new HashSet<>(readRes), writeResAsSet);
  }

  @Test
  void orderPreservedByPartition() throws Exception {
    // Prepare env
    HStreamClient hStreamClient = HStreamClient.builder().serviceUrl(serviceUrl).build();
    var stream = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, stream);
    int shardCount = 10;
    int recordCount = 100;

    // Read
    List<String> readRes = new ArrayList<>();
    CountDownLatch notify = new CountDownLatch(recordCount);
    var lock = new ReentrantLock();
    Consumer consumer =
        createConsumerCollectStringPayload(
            logger, hStreamClient, subscription, readRes, notify, lock);
    consumer.startAsync().awaitRunning();

    // Write
    Producer producer = hStreamClient.newProducer().stream(stream).build();
    Random rand = new Random();
    HashMap<String, List<String>> writeRes = new HashMap<>();
    for (int i = 0; i < recordCount; ++i) {
      var key = "key-" + rand.nextInt(shardCount);
      var res = doProduce(producer, 32, 1, key);
      logger.info("=== Write to {}, num={}.", key, res.size());
      if (writeRes.containsKey(key)) {
        writeRes.get(key).addAll(res);
      } else {
        writeRes.put(key, new ArrayList<>(res));
      }
    }
    notify.await(20, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();

    // Analisis
    logger.info("===== Write Stats =====");
    int totalWrite = 0;
    for (String key : writeRes.keySet()) {
      var thisValue = writeRes.get(key);
      logger.info("{}: Len={}", key, thisValue.size());
      totalWrite += thisValue.size();
    }
    logger.info("Total Write = {}", totalWrite);

    logger.info("===== Read Stats ======");
    logger.info("Total Read = {}", readRes.size());

    // 1. Every item written to the database is read out
    HashSet<String> writeResAsSet = new HashSet<>();
    for (var thisValue : writeRes.values()) {
      writeResAsSet.addAll(thisValue);
    }
    Assertions.assertEquals(new HashSet<>(readRes), writeResAsSet);

    // 2. Order property: messages read from a certain key preserve the order
    //    when they were written to it
    Assertions.assertEquals(shardCount, writeRes.size());
    for (String key : writeRes.keySet()) {
      Assertions.assertTrue(isSkippedSublist(writeRes.get(key), readRes));
    }
  }

  @Test
  void shardBalance() throws Exception {
    // Prepare env
    HStreamClient hStreamClient = HStreamClient.builder().serviceUrl(serviceUrl).build();
    var stream = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, stream);
    int shardCount = 10;
    int recordCount = 100;
    int consumerCount = 7;

    // Read
    List<List<String>> readRes = new ArrayList<List<String>>();
    for (int i = 0; i < consumerCount; ++i) {
      readRes.add(new ArrayList<>());
    }
    CountDownLatch notify = new CountDownLatch(recordCount);
    var lock = new ReentrantLock();
    ArrayList<Consumer> consumers = new ArrayList<>();
    for (int i = 0; i < consumerCount; ++i) {
      Consumer consumer =
          createConsumerCollectStringPayload(
              logger, hStreamClient, subscription, readRes.get(i), notify, lock);
      consumers.add(consumer);
      consumer.startAsync().awaitRunning();
    }

    // Write
    Producer producer = hStreamClient.newProducer().stream(stream).build();
    Random rand = new Random();
    HashMap<String, List<String>> writeRes = new HashMap<>();
    for (int i = 0; i < recordCount; ++i) {
      var key = "key-" + rand.nextInt(shardCount);
      var res = doProduce(producer, 32, 1, key);
      logger.info("=== Write to {}, num={}.", key, res.size());
      if (writeRes.containsKey(key)) {
        writeRes.get(key).addAll(res);
      } else {
        writeRes.put(key, new ArrayList<>(res));
      }
    }

    notify.await(20, TimeUnit.SECONDS);
    consumers.forEach(c -> c.stopAsync().awaitTerminated());

    // Analisis
    // Write part
    logger.info("===== Write Stats =====");
    int totalWrite = 0;
    for (String key : writeRes.keySet()) {
      var thisValue = writeRes.get(key);
      logger.info("{}: Len={}", key, thisValue.size());
      totalWrite += thisValue.size();
    }
    logger.info("Total Write = {}", totalWrite);

    // Read part
    logger.info("===== Read Stats ======");
    int totalRead = 0;
    for (int i = 0; i < consumerCount; ++i) {
      var thisValue = readRes.get(i);
      logger.info("Consumer {}: Len={}", i, thisValue.size());
      totalRead += thisValue.size();
    }
    logger.info("Total Read = {}", totalRead);

    // Keys balancing part
    logger.info("===== Keys Stats =====");

    List<HashSet<String>> ownedKeysByConsumers = new ArrayList<>();
    HashSet<String> unionOfKeys = new HashSet<>();
    for (int i = 0; i < consumerCount; ++i) {
      HashSet<String> ownedKeys = conjectureKeysOfConsumer(writeRes, readRes.get(i));
      ownedKeysByConsumers.add(ownedKeys);
      logger.info("Consumer {}: {}", i, ownedKeys);
      // 1. When consumer number <= key number, every consumer owns at least 1 key
      Assertions.assertFalse(ownedKeys.isEmpty());
      Iterables.addAll(unionOfKeys, ownedKeys);
    }
    logger.info("All allocated keys: {}", unionOfKeys);

    // 2. Every item written to the database is read out
    HashSet<String> writeResAsSet = new HashSet<>();
    HashSet<String> readResAsSet = new HashSet<>();
    for (var thisValue : writeRes.values()) {
      writeResAsSet.addAll(thisValue);
    }
    for (var thisValue : readRes) {
      readResAsSet.addAll(thisValue);
    }
    Assertions.assertEquals(readResAsSet, writeResAsSet);

    // 3. Assert the union of keys all consumers own is equal to all keys
    HashSet<String> expectedKeys = new HashSet<>();
    for (int i = 0; i < shardCount; ++i) {
      expectedKeys.add("key-" + i);
    }

    Assertions.assertEquals(unionOfKeys, expectedKeys);
    // 4. Assert the keys any two consumers own is disjoint
    for (int i = 0; i < consumerCount; ++i) {
      for (int j = 0; j < i; j++) {
        HashSet<String> intersection = new HashSet<String>(ownedKeysByConsumers.get(i));
        intersection.retainAll(ownedKeysByConsumers.get(j));
        Assertions.assertEquals(intersection, new HashSet<>());
      }
    }
  }

  @Test
  public void testOrderingKeyBatch() throws Exception {
    HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();
    var streamName = randStream(client);
    var testSubscriptionId = randSubscription(client, streamName);
    BufferedProducer producer =
        client.newBufferedProducer().stream(streamName)
            .recordCountLimit(100)
            .flushIntervalMs(100)
            .build();
    final int count = 10;
    doProduce(producer, 100, count / 2, "K1");
    doProduce(producer, 100, count / 2, "K2");

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
                  responder.ack();
                  index.incrementAndGet();
                  logger.info("ack for {}, idx:{}", receivedRawRecord.getRecordId(), index.get());
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
  public void testWriteOrderWithDiffKeys() throws Exception {
    HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();
    var streamName = randStream(client);
    var testSubscriptionId = randSubscription(client, streamName);
    BufferedProducer producer =
        client.newBufferedProducer().stream(streamName)
            .recordCountLimit(100)
            .flushIntervalMs(-1)
            .build();
    final int count = 100;
    List<CompletableFuture<RecordId>> fs = new LinkedList<>();
    List<byte[]> records = new LinkedList<>();
    for (int i = 0; i < count; i++) {
      var r = randBytes();
      records.add(r);
      String key = i % 2 == 0 ? "k1" : "k2";
      fs.add(producer.write(Record.newBuilder().rawRecord(r).orderingKey(key).build()));
    }
    producer.close();
    Map<String, Map<RecordId, byte[]>> ids = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      logger.debug(
          "write record:{}, id:{}", UUID.nameUUIDFromBytes(records.get(i)), fs.get(i).join());
      String key = i % 2 == 0 ? "k1" : "k2";
      if (i < 2) {
        ids.put(key, new HashMap<>());
      }
      ids.get(key).put(fs.get(i).join(), records.get(i));
    }

    logger.info("producer finish");

    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger index = new AtomicInteger();
    List<ReceivedRawRecord> rawRecords = new ArrayList<>();
    Consumer consumer =
        client
            .newConsumer()
            .subscription(testSubscriptionId)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  rawRecords.add(receivedRawRecord);
                  responder.ack();
                  index.incrementAndGet();
                  if (index.get() == count) {
                    latch.countDown();
                  }
                })
            .build();
    consumer.startAsync().awaitRunning();

    latch.await();
    consumer.stopAsync().awaitTerminated();

    for (ReceivedRawRecord r : rawRecords) {
      logger.debug(
          "l:{}, r:{}",
          UUID.nameUUIDFromBytes(r.getRawRecord()),
          UUID.nameUUIDFromBytes(ids.get(r.getHeader().getOrderingKey()).get(r.getRecordId())));
      Assertions.assertEquals(
          UUID.nameUUIDFromBytes(r.getRawRecord()),
          UUID.nameUUIDFromBytes(ids.get(r.getHeader().getOrderingKey()).get(r.getRecordId())));
    }
  }
}
