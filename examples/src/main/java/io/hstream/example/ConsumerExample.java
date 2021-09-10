package io.hstream.example;

import io.hstream.*;
import java.util.Random;

public class ConsumerExample {
  private static final String SERVICE_URL = "localhost:6570";
  private static final String DEMO_STREAM = "demo_stream";
  private static final String DEMO_SUBSCRIPTION = "demo_stream";

  public static void main(String[] args) {
    HStreamClient client = HStreamClient.builder().serviceUrl(SERVICE_URL).build();

    client.createStream(DEMO_STREAM);

    Subscription subscription =
        new Subscription(
            DEMO_SUBSCRIPTION,
            DEMO_STREAM,
            new SubscriptionOffset(SubscriptionOffset.SpecialOffset.LATEST));
    client.createSubscription(subscription);

    Producer producer =
        client.newProducer().stream(DEMO_STREAM).enableBatch().recordCountLimit(1000).build();

    Random random = new Random();
    byte[] rawRecord = new byte[100];
    for (int i = 0; i < 1000; ++i) {
      random.nextBytes(rawRecord);
      producer.writeAsync(rawRecord);
    }
    producer.flush();

    Consumer consumer =
        client
            .newConsumer()
            .subscription(DEMO_SUBSCRIPTION)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  System.out.println("get record: " + receivedRawRecord.getRecordId());
                })
            .build();
    consumer.startAsync().awaitRunning();
  }
}
