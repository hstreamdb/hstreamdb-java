package io.hstream;

import io.hstream.impl.HStreamClientBuilderImpl;
import java.util.List;

/** HStreamDB Client. */
public interface HStreamClient extends AutoCloseable {

  /** @return {@link HStreamClientBuilder}. */
  static HStreamClientBuilder builder() {
    return new HStreamClientBuilderImpl();
  }

  /** @return the {@link ProducerBuilder}. */
  ProducerBuilder newProducer();

  /** @return the {@link ConsumerBuilder}. */
  ConsumerBuilder newConsumer();

  /** @return the {@link QueryerBuilder}. */
  QueryerBuilder newQueryer();

  /** @param stream the name of stream. */
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
