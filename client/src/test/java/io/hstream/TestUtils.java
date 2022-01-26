package io.hstream;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;

public class TestUtils {

  public static void deleteAllSubscriptions(HStreamClient client) {
    // client.listSubscriptions().stream().map(Subscription::getSubscriptionId).forEach(client::deleteSubscription);
    List<Subscription> subscriptions = client.listSubscriptions();
    for (Subscription subscription : subscriptions) {
      System.out.println(subscription);
      System.out.println(subscription.getSubscriptionId());
      client.deleteSubscription(subscription.getSubscriptionId());
    }
  }

  public static String randText() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  public static byte[] randBytes() {
    return UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
  }

  public static String randStream(HStreamClient c) {
    String streamName = "test_stream_" + randText();
    c.createStream(streamName, (short) 3);
    return streamName;
  }

  public static String randSubscription(HStreamClient c, String streamName) {
    String subscriptionName = "test_subscription_" + randText();
    Subscription subscription =
        Subscription.newBuilder().subscription(subscriptionName).stream(streamName)
            .ackTimeoutSeconds(60)
            .build();
    c.createSubscription(subscription);
    return subscriptionName;
  }

  public static ArrayList<String> doProduce(
      Producer producer, int payloadSize, int recordsNums, String key) {
    Random rand = new Random();
    byte[] rRec = new byte[payloadSize];
    var records = new ArrayList<String>();
    var xs = new CompletableFuture[recordsNums];
    for (int i = 0; i < recordsNums; i++) {
      rand.nextBytes(rRec);
      Record recordToWrite = Record.newBuilder().key(key).rawRecord(rRec).build();
      records.add(Arrays.toString(rRec));
      xs[i] = producer.write(recordToWrite);
    }
    CompletableFuture.allOf(xs).join();
    return records;
  }

  public static RecordId produceIntegerAndGatherRid(Producer producer, int data, String key) {
    Record recordToWrite =
        Record.newBuilder()
            .key(key)
            .rawRecord(Integer.toString(data).getBytes(StandardCharsets.UTF_8))
            .build();
    return producer.write(recordToWrite).join();
  }

  public static Consumer createConsumerCollectIntegerPayload(
      Logger logger,
      HStreamClient client,
      String subscription,
      List<Integer> records,
      CountDownLatch latch,
      ReentrantLock lock) {
    return client
        .newConsumer()
        .subscription(subscription)
        .rawRecordReceiver(
            (receivedRawRecord, responder) -> {
              logger.info(
                  "### Read 1 record, id={}, value={}.",
                  receivedRawRecord.getRecordId(),
                  Integer.parseInt(
                      new String(receivedRawRecord.getRawRecord(), StandardCharsets.UTF_8)));
              lock.lock();
              records.add(
                  Integer.parseInt(
                      new String(receivedRawRecord.getRawRecord(), StandardCharsets.UTF_8)));
              lock.unlock();
              responder.ack();
              latch.countDown();
            })
        .build();
  }

  public static Consumer createConsumerCollectStringPayload(
      Logger logger,
      HStreamClient client,
      String subscription,
      List<String> records,
      CountDownLatch latch,
      ReentrantLock lock) {
    return client
        .newConsumer()
        .subscription(subscription)
        .rawRecordReceiver(
            (receivedRawRecord, responder) -> {
              logger.info("### Read 1 record, id={}.", receivedRawRecord.getRecordId());
              lock.lock();
              records.add(Arrays.toString(receivedRawRecord.getRawRecord()));
              lock.unlock();
              responder.ack();
              latch.countDown();
            })
        .build();
  }

  public static <T> boolean isSkippedSublist(List<T> sub, List<T> whole) {
    List<T> wholeVar = new ArrayList<>(whole);
    for (int i = 0; i < sub.size(); ++i) {
      int index = wholeVar.indexOf(sub.get(i));
      if (index < 0) {
        return false;
      } else {
        wholeVar = wholeVar.subList(index, wholeVar.size());
      }
    }
    return true;
  }

  public static <T> HashSet<String> conjectureKeysOfConsumer(
      HashMap<String, List<T>> writeRec, List<T> readRec) {
    HashSet<String> result = new HashSet<>();
    for (String key : writeRec.keySet()) {
      var valuesOfThisKey = writeRec.get(key);
      for (T item : readRec) {
        if (valuesOfThisKey.contains(item)) {
          result.add(key);
        }
      }
    }
    return result;
  }
}
