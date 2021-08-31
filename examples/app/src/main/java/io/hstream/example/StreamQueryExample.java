package io.hstream.example;

import io.hstream.*;

public class StreamQueryExample {
  private static final String SERVICE_URL = "localhost:6570";
  private static final String DEMO_STREAM = "demo_stream";
  private static final String DEMO_SUBSCRIPTION = "demo_stream";

  public static void main(String[] args) throws Exception {

    HStreamClient client = HStreamClient.builder().serviceUrl(SERVICE_URL).build();
    client.createStream(DEMO_STREAM);

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
            .sql("select * from " + DEMO_STREAM + " where temperature > 30 emit changes;")
            .resultObserver(observer)
            .build();

    // third, start the queryer
    queryer.startAsync().awaitRunning();

    Producer producer = client.newProducer().stream(DEMO_STREAM).build();
    HRecord hRecord1 = HRecord.newBuilder().put("temperature", 29).put("humidity", 20).build();
    HRecord hRecord2 = HRecord.newBuilder().put("temperature", 34).put("humidity", 21).build();
    HRecord hRecord3 = HRecord.newBuilder().put("temperature", 35).put("humidity", 22).build();
    producer.write(hRecord1);
    producer.write(hRecord2);
    producer.write(hRecord3);

    Thread.sleep(5000);

    queryer.stopAsync().awaitTerminated();
    client.close();
  }
}
