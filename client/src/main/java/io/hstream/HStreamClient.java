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
   * Create a new stream.
   *
   * @param stream Stream Object, you should use {@link Stream.Builder} to build it.
   */
  void createStream(Stream stream);

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

  GetStreamResponse getStream(String streamName);

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

  GetSubscriptionResponse getSubscription(String subscriptionId);

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

  Query createQuery(String name, String sql);

  List<Query> listQueries();

  Query getQuery(String name);

  void deleteQuery(String name);

  void pauseQuery(String name);

  void createView(String sql);

  List<View> listViews();

  View getView(String name);

  void deleteView(String name);

  /**
   * only support to execute view query, e.g. `select * from view1 where id = 1;`
   *
   * @param sql select view sql
   * @return all HRecord result set
   */
  List<HRecord> executeViewQuery(String sql);

  List<ConsumerInformation> listConsumers(String subscriptionId);

  // == Connectors
  Connector createConnector(CreateConnectorRequest request);

  List<Connector> listConnectors();

  Connector getConnector(String name);

  String getConnectorSpec(String type, String target);

  void pauseConnector(String name);

  void resumeConnector(String name);

  void deleteConnector(String name);
}
