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

  BufferedProducerBuilder newBufferedProducer();

  /** @return a {@link ConsumerBuilder} */
  ConsumerBuilder newConsumer();

  /** @return a {@link QueryerBuilder} */
  QueryerBuilder newQueryer();

  /** @return a {@link ReaderBuilder} */
  ReaderBuilder newReader();

  /**
   * Create a new stream with 1 replicas.
   *
   * @param stream the name of stream
   */
  void createStream(String stream);

  /**
   * Create a new stream.
   *
   * @param stream the name of the stream
   * @param replicationFactor replication factor of the stream
   */
  void createStream(String stream, short replicationFactor);

  /**
   * Create a new stream.
   *
   * @param stream the name of the stream
   * @param replicationFactor replication factor of the stream
   * @param shardCount number of shards in the stream
   */
  void createStream(String stream, short replicationFactor, int shardCount);

  /**
   * Create a new stream.
   *
   * @param stream the name of the stream
   * @param replicationFactor replication factor of the stream
   * @param shardCount number of shards in the stream
   * @param backlogDuration backlog duration(in seconds) of the stream
   */
  void createStream(String stream, short replicationFactor, int shardCount, int backlogDuration);

  /**
   * List shards in a stream.
   *
   * @param streamName the name of the stream
   * @return a list of {@link Shard}s.
   */
  List<Shard> listShards(String streamName);

  /**
   * Delete the specified stream with streamName.
   *
   * @param stream the name of stream
   */
  void deleteStream(String stream);

  /**
   * Delete the specified stream with streamName.
   *
   * @param stream the name of stream
   * @param force the flag whether to enable force deletion
   */
  void deleteStream(String stream, boolean force);

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

  /**
   * Delete the specified subscription with subscriptionId.
   *
   * @param subscriptionId the id of the subscription to be deleted
   * @param force the flag whether to enable force deletion
   */
  void deleteSubscription(String subscriptionId, boolean force);

  Cluster describeCluster();

  void createQuery(String sql);
  List<Query> listQueries();
  Query getQuery(String id);
  void deleteQuery(String id);

  void createView(String sql);
  List<View> listViews();
  View getView(String name);
  void deleteView(String name);
}
