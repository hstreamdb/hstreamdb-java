package io.hstream;

import io.hstream.impl.QueryerImpl;

public class QueryerBuilder {

  private HStreamClient client;
  private HStreamApiGrpc.HStreamApiStub grpcStub;
  private String sql;
  private Observer<HRecord> resultObserver;

  public QueryerBuilder(HStreamClient client, HStreamApiGrpc.HStreamApiStub grpcStub) {
    this.client = client;
    this.grpcStub = grpcStub;
  }

  QueryerBuilder sql(String sql) {
    this.sql = sql;
    return this;
  }

  QueryerBuilder resultObserver(Observer<HRecord> resultObserver) {
    this.resultObserver = resultObserver;
    return this;
  }

  Queryer build() {
    return new QueryerImpl(client, grpcStub, sql, resultObserver);
  }
}
