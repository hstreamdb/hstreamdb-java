package io.hstream;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class HStreamClientTest07 {

    private static final Logger logger = LoggerFactory.getLogger(HStreamClientTest07.class);
    private static final String serviceUrl = "127.0.0.1:50051";
    private static String STREAM_NAME_PREFIX = "test_stream_";

    private AtomicInteger counter = new AtomicInteger();

    @Test
    void createStreamTest() throws Exception {
       HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();
       client.createStream(STREAM_NAME_PREFIX + counter.incrementAndGet());
       client.close();
    }

    @Test
    void writeTest() throws Exception {
        HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();
        String streamName = STREAM_NAME_PREFIX + counter.incrementAndGet();
        client.createStream(streamName);

        Random random = new Random();
        byte[] payload = new byte[100];
        random.nextBytes(payload);
        Producer producer = client.newProducer().stream(streamName).build();
        CompletableFuture<RecordId> future = producer.write(Record.newBuilder().key("k1").rawRecord(payload).build());
        RecordId recordId = future.join();
        logger.info("write successfully, got recordId: " + recordId.toString());
        client.close();
    }

}
