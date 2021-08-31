# hstreamdb-java-example

## Prerequisites 

- hstreamdb server [v0.5.2.0](https://hub.docker.com/layers/hstreamdb/hstream/v0.5.2.0/images/sha256-d26234758cd47916d6a5d137a3690cdeeddaf7af041a8eea5e7c1d7ada43f3b8)
- hstreamdb-java [v0.3.0](https://search.maven.org/artifact/io.hstream/hstreamdb-java/0.3.0/jar) 

## Examples

- [ConsumerExample.java](app/src/main/java/io/hstream/example/ConsumerExample.java) - How to consume messages from a subscription
- [StreamQueryExample.java](app/src/main/java/io/hstream/example/StreamQueryExample.java) - How to use SQL commands to query data from streams

## Run

### Step 1: Start HStreamDB Server

```bash

./start-hstreamdb.sh

```

### Step 2: Run this Example 

```bash

gradle run

```
