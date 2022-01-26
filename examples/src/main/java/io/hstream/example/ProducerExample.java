package io.hstream.example;

import io.hstream.*;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/** This example shows how to write data to an existed stream */
public class ProducerExample {
  private static final String SERVICE_URL = "localhost:6570";
  private static final String DEMO_STREAM = "demo_stream";

  public static void main(String[] args) {
    HStreamClient client = HStreamClient.builder().serviceUrl(SERVICE_URL).build();

    BufferedProducer producer =
        client.newBufferedProducer().stream(DEMO_STREAM).recordCountLimit(1000).build();

    Random random = new Random();
    byte[] rawRecord = new byte[100];
    for (int i = 0; i < 1000; ++i) {
      random.nextBytes(rawRecord);
      CompletableFuture<RecordId> future = producer.write(rawRecord);
    }
    producer.close();
  }
}
