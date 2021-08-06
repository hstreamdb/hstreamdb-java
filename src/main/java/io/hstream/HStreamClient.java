package io.hstream;

import java.util.List;

/** HstreamClient implement the hstreamdb client */
public interface HStreamClient extends AutoCloseable {

  /**
   * a static method use to create a new client.
   *
   * @return {@link ClientBuilder}.
   */
  static ClientBuilder builder() {
    return new ClientBuilder();
  }

  /**
   * create a producer.
   *
   * @return the {@link ProducerBuilder}.
   */
  ProducerBuilder newProducer();

  /**
   * create a consumer.
   *
   * @return the {@link ConsumerBuilder}.
   */
  ConsumerBuilder newConsumer();

  /**
   * Execute a single sql query.
   *
   * @param sql sql query wait to execute.
   * @return sql execute result in {@link HRecord} format, which can be consumed from {@link
   *     Publisher}.
   */
  Publisher<HRecord> streamQuery(String sql);

  /**
   * Create a stream.
   *
   * @param stream the name of stream.
   */
  void createStream(String stream);

  /**
   * Delete specified stream with streamName.
   *
   * @param stream the name of stream.
   */
  void deleteStream(String stream);

  /**
   * Return all created {@link Stream}.
   *
   * @return the list of created streams.
   */
  List<Stream> listStreams();

  /**
   * Create a new Subscription.
   *
   * @param subscription {@link Subscription}.
   */
  void createSubscription(Subscription subscription);

  /**
   * Return all created {@link Subscription}.
   *
   * @return the list of created Subscriptions.
   */
  List<Subscription> listSubscriptions();

  /**
   * Delete specified subscription with subscriptionId.
   *
   * @param subscriptionId the id of the subscription to be deleted.
   */
  void deleteSubscription(String subscriptionId);
}
