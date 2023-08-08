package io.hstream.impl;

import static com.google.common.base.Preconditions.checkArgument;

import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.HStreamDBClientException;
import io.hstream.Observer;
import io.hstream.Queryer;
import io.hstream.QueryerBuilder;
import java.util.List;

public class QueryerBuilderImpl implements QueryerBuilder {

  private final HStreamClient client;
  private final List<String> serverUrls;
  private final ChannelProvider channelProvider;

  private String sql;
  private Observer<HRecord> resultObserver;

  public QueryerBuilderImpl(
      HStreamClient client, List<String> serverUrls, ChannelProvider channelProvider) {
    this.client = client;
    this.serverUrls = serverUrls;
    this.channelProvider = channelProvider;
  }

  @Override
  public QueryerBuilder sql(String sql) {
    this.sql = sql;
    return this;
  }

  @Override
  public QueryerBuilder resultObserver(Observer<HRecord> resultObserver) {
    this.resultObserver = resultObserver;
    return this;
  }

  @Override
  public Queryer build() {
    checkArgument(sql != null, "QueryerBuilder: `sql` should not be null");
    checkArgument(resultObserver != null, "QueryerBuilder: `resultObserver` should not be null");
    throw new HStreamDBClientException("unsupported");
  }
}
