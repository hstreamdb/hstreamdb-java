# hstreamdb-java

![Build Status](https://github.com/hstreamdb/hstreamdb-java/actions/workflows/main.yml/badge.svg)
[![Maven Central](https://img.shields.io/maven-central/v/io.hstream/hstreamdb-java)](https://search.maven.org/artifact/io.hstream/hstreamdb-java)
[![javadoc](https://javadoc.io/badge2/io.hstream/hstreamdb-java/0.8.0/javadoc.svg)](https://javadoc.io/doc/io.hstream/hstreamdb-java/0.8.0)
[![Snapshot Artifacts](https://img.shields.io/nexus/s/https/s01.oss.sonatype.org/io.hstream/hstreamdb-java.svg)](https://s01.oss.sonatype.org/content/repositories/snapshots/io/hstream/hstreamdb-java/0.8.0-SNAPSHOT/)
[![javadoc](https://javadoc.io/badge2/io.hstream/hstreamdb-java/0.8.0-SNAPSHOT/javadoc.svg)](https://hstreamdb.github.io/hstreamdb-java/javadoc/)

This is the official Java client library for [HStreamDB](https://hstream.io/).

**Please use the latest released version.**

**The latest release is v0.8.0, which requires hstream server v0.8.0 .**

## Content
- [Installation](#installation)
    - [Maven](#maven)
    - [Gradle](#gradle)
- [Example Usage](#example-usage)
    - [Connect to HStreamDB](#connect-to-hstreamdb)
    - [Work with Streams](#work-with-streams)
    - [Write Data to a Stream](#write-data-to-a-stream)
    - [Consume Data from a Stream](#consume-data-from-a-subscription)
    - [Read Data from a Stream Shard](#read-data-from-a-stream-shard)


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
    <version>0.8.0</version>
  </dependency>
</dependencies>

```

### Gradle

For Gradle Users, the library can be included easily like this:

```groovy

implementation 'io.hstream:hstreamdb-java:0.8.0'

```

## Example Usage

Here we will show you some simple examples to use hstreamdb-client,
you can find the complete examples in [hstreamdb-java-examples](https://github.com/hstreamdb/hstreamdb-java-examples),
for more detail on introduction and usage, please check the [guides](https://hstream.io/docs/en/latest/guides/write.html).

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

// create a new stream with 5 replicas
client.createStream("test_stream", 5);


// delete a stream
client.deleteStream("test_stream");

```

### Write Data to a Stream

```java

Producer producer = client.newProducer().stream("test_stream").build();

// write raw records
Random random = new Random();
byte[] rawRecord = new byte[100];
random.nextBytes(rawRecord);
Record recordR = Record.newBuilder().rawRecord(rawRecord).build();
CompletableFuture<String> future = producer.write(recordR);

// write hRecords
HRecord hRecord = HRecord.newBuilder()
        .put("key1", 10)
        .put("key2", "hello")
        .put("key3", true)
        .build();
Record recordH = Record.newBuilder().hRecord(hRecord).build();
CompletableFuture<String> future = producer.write(recordH);

// buffered writes
BatchSetting batchSetting =
    BatchSetting.newBuilder()
        .recordCountLimit(100)
        .bytesLimit(4096)
        .ageLimit(100)
        .build();

FlowControlSetting flowControlSetting =
    FlowControlSetting.newBuilder()
        .bytesLimit(40960)
        .build();

BufferedProducer batchedProducer =
    client.newBufferedProducer().stream("test_stream")
            .batchSetting(batchSetting)
            .flowControlSetting(flowControlSetting)
            .build();

for(int i = 0; i < 1000; ++i) {
    random.nextBytes(rawRecord);
    Record recordB = Record.newBuilder().rawRecord(rawRecord).build();
    batchedProducer.write(recordB);
}
// flush and close batchedProducer
batchedProducer.close();

```

**Please do not write both binary data and hrecord in one stream.**


### Consume Data from a Subscription

```java
// first, create a subscription for the stream
Subscription subscription = 
    Subscription
        .newBuilder()
        .subscription("my_subscription")
        .stream("my_stream")
        .ackTimeoutSeconds(600)
        .build();
client.createSubscription(subscription);

// second, create a consumer attach to the subscription
Consumer consumer =
    client
        .newConsumer()
        .subscription("my_subscription")
        .rawRecordReceiver(
            ((receivedRawRecord, responder) -> {
                System.out.println(receivedRawRecord.getRecordId());
                responder.ack();
            }))
        .build();

// third, start the consumer
consumer.startAsync().awaitRunning();
System.out.println("the consumer is started");

```

### Read Data from a Stream Shard

```java

Reader reader =
    client
        .newReader()
        .readerId("my_readerId")
        .streamName("my_stream")
        .shardId("my_shardId")
        .build();


List<Record> records = reader.read(10).join();

reader.close();

```