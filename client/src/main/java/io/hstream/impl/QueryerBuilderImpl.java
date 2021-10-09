package io.hstream.impl;

import io.hstream.*;
import io.hstream.internal.HStreamApiGrpc;

public class QueryerBuilderImpl implements QueryerBuilder {

  private HStreamClient client;
  private HStreamApiGrpc.HStreamApiStub grpcStub;
  private String sql;
  private Observer<HRecord> resultObserver;

  public QueryerBuilderImpl(HStreamClient client, HStreamApiGrpc.HStreamApiStub grpcStub) {
    this.client = client;
    this.grpcStub = grpcStub;
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
    return new QueryerImpl(client, grpcStub, sql, resultObserver);
  }
}
