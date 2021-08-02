package io.hstream;

import java.util.List;

public interface HStreamClient extends AutoCloseable {

  static ClientBuilder builder() {
    return new ClientBuilder();
  }

  ProducerBuilder newProducer();

  ConsumerBuilder newConsumer();

  Publisher<HRecord> streamQuery(String sql);

  void createStream(String stream);

  void deleteStream(String stream);

  List<Stream> listStreams();

  void createSubscription(Subscription subscription);

  List<Subscription> listSubscriptions();

  void deleteSubscription(String subscriptionId);
}
