package io.hstream.example;

import io.hstream.HStreamClient;
import io.hstream.Producer;
import io.hstream.RecordId;

import java.util.Random;
import java.util.concurrent.CompletableFuture;

/** This example shows how to create a stream */
public class CreateStreamExample {
  private static final String SERVICE_URL = "localhost:6570";
  private static final String DEMO_STREAM = "demo_stream";

  public static void main(String[] args) {
    HStreamClient client = HStreamClient.builder().serviceUrl(SERVICE_URL).build();

    client.createStream(DEMO_STREAM);
  }
}
