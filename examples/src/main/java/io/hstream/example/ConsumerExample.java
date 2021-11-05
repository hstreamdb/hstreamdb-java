package io.hstream.example;

import io.hstream.*;

/** This example shows how to consume data from specified subscription */
public class ConsumerExample {
  private static final String SERVICE_URL = "localhost:6570";
  private static final String DEMO_SUBSCRIPTION = "demo_stream";

  public static void main(String[] args) {
    HStreamClient client = HStreamClient.builder().serviceUrl(SERVICE_URL).build();

    Consumer consumer =
        client
            .newConsumer()
            .subscription(DEMO_SUBSCRIPTION)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  System.out.println("get record: " + receivedRawRecord.getRecordId());
                  responder.ack();
                })
            .build();
    consumer.startAsync().awaitRunning();
  }
}
