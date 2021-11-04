package io.hstream;

import io.hstream.impl.HStreamClientBuilderImpl;
import java.util.List;

/** A client for the HStreamDB. */
public interface HStreamClient extends AutoCloseable {

  /** @return a {@link HStreamClientBuilder} */
  static HStreamClientBuilder builder() {
    return new HStreamClientBuilderImpl();
  }

  /** @return a {@link ProducerBuilder} */
  ProducerBuilder newProducer();

  /** @return a {@link ConsumerBuilder} */
  ConsumerBuilder newConsumer();

  /** @return a {@link QueryerBuilder} */
  QueryerBuilder newQueryer();

  /**
   * Create a new stream with 3 replicas.
   *
   * @param stream the name of stream
   */
  void createStream(String stream);

  /**
   * Create a new stream.
   *
   * @param stream the name of stream
   */
  void createStream(String stream, short replicationFactor);

  /**
   * Delete the specified stream with streamName.
   *
   * @param stream the name of stream
   */
  void deleteStream(String stream);

  /**
   * List all streams.
   *
   * @return a list of {@link Stream}s
   */
  List<Stream> listStreams();

  /**
   * Create a new Subscription.
   *
   * @param subscription {@link Subscription}
   */
  void createSubscription(Subscription subscription);

  /**
   * List all subscriptions.
   *
   * @return a list of {@link Subscription}s.
   */
  List<Subscription> listSubscriptions();

  /**
   * Delete the specified subscription with subscriptionId.
   *
   * @param subscriptionId the id of the subscription to be deleted
   */
  void deleteSubscription(String subscriptionId);
}
