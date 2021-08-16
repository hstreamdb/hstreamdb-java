# hstreamdb-java

![Build Status](https://github.com/hstreamdb/hstreamdb-java/actions/workflows/main.yml/badge.svg)
[![Maven Central](https://img.shields.io/maven-central/v/io.hstream/hstreamdb-java)](https://search.maven.org/artifact/io.hstream/hstreamdb-java)
[![javadoc](https://javadoc.io/badge2/io.hstream/hstreamdb-java/javadoc.svg)](https://javadoc.io/doc/io.hstream/hstreamdb-java)

This is the offical Java client library for [HStreamDB](https://hstream.io/).

**Please use the latest released version.** 

**The latest release is v0.3.0, which requires hstream server v0.5.1.x.**

## Content 
- [Installation](#installation)
    - [Maven](#maven)
    - [Gradle](#gradle)
- [Example Usage](#example-usage)
    - [Connect to HStreamDB](#connect-to-hstreamdb)
    - [Work with Streams](#work-with-streams)
    - [Write Data to a Stream](#write-data-to-a-stream)
    - [Consume Data from a Stream](#consume-data-from-a-stream)
    - [Process Data in Stream with SQL](#process-data-in-stream-with-sql)

## Installation

The library artifact is published in Maven central, 
available at [hstreamdb-java](https://search.maven.org/artifact/io.hstream/hstreamdb-java).

### Maven

For Maven Users, the library can be included easily like this:

```xml

<dependencies>
  <dependency>
    <groupId>io.hstream</groupId>
    <artifactId>hstreamdb-java</artifactId>
    <version>${hstreamdbClient.version}</version>
  </dependency>
</dependencies>

```

### Gradle 

For Gradle Users, the library can be included easily like this:

```groovy

implementation 'io.hstream:hstreamdb-java:${hstreamdbClientVersion}'

```

## Example Usage 

### Connect to HStreamDB

```java

import io.hstream.*;

public class ConnectExample {
    public static void main(String[] args) throws Exception {
        final String serviceUrl = "localhost:6570";
        HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();
        System.out.println("Connected");
        client.close();
    }
}

```

### Work with Streams 

```java

// get a list of streams 
for(Stream stream: client.listStreams()) {
  System.out.println(stream.getStreamName());
}


// create a new stream
client.createStream("test_stream");


// delete a stream
client.deleteStream("test_stream");

```

### Write Data to a Stream 

```java

Producer producer = client.newProducer().stream("test_stream").build();

// write raw records synchronously
Random random = new Random();
byte[] rawRecord = new byte[100];
random.nextBytes(rawRecord);
RecordId recordId = producer.write(rawRecord);

// write raw records asynchronously
CompletableFuture<RecordId> future = producer.writeAsync(rawRecord);

// write hRecords synchronously
HRecord hRecord = HRecord.newBuilder()
        .put("key1", 10)
        .put("key2", "hello")
        .put("key3", true)
        .build();
RecordId recordId = producer.write(hRecord);

// write hRecords asynchronously
CompletableFuture<RecordId> future = producer.write(hRecord);

// buffered writes
Producer batchedProducer = client.newProducer()
        .stream("test_stream")
        .enableBatch()
        .recordCountLimit(100)
        .build();
for(int i = 0; i < 1000; ++i) {
    random.nextBytes(rawRecord);
    batchedProducer.writeAsync(rawRecord);
}
batchedProducer.flush()


```

**Please do not write both binary data and hrecord in one stream.**


### Consume Data from a Stream 

```java

// first, create a subscription for the stream
Subscription subscription =
        Subscription.newBuilder()
            .setSubscriptionId("my_subscription")
            .setStreamName("test_stream")
            .setOffset(
                SubscriptionOffset.newBuilder()
                    .setSpecialOffset(SubscriptionOffset.SpecialOffset.LATEST)
                    .build())
            .build();
client.createSubscription(subscription);

// second, create a consumer attacth to the subscription 
Consumer consumer =
      client
          .newConsumer()
          .subscription("my_subscription")
          .rawRecordReceiver(
              (receivedRawRecord, responder) -> {
                  System.out.println(receivedRawRecord.getRecordId());
                  responder.ack();})
          .build();

// third, start the consumer 
consumer.startAsync().awaitRunning();

```

### Process Data in Stream with SQL   

```java

// first, create an observer for sql results
Observer<HRecord> observer =
      new Observer<HRecord>() {
        @Override
        public void onNext(HRecord value) {
          System.out.println(value);
        }

        @Override
        public void onError(Throwable t) {
          System.out.println("error happend!");
        }

        @Override
        public void onCompleted() {}
      };

// second, create a queryer to execute a sql 
Queryer queryer =
      client
          .newQueryer()
          .sql("select * from test_stream emit changes;")
          .resultObserver(observer)
          .build();

// third, start the queryer 
queryer.startAsync().awaitRunning();

```
