package io.hstream;

import io.hstream.impl.QueryerImpl;
import io.hstream.internal.HStreamApiGrpc;

/** Builder used to configure and construct a {@link Queryer} instance. */
public class QueryerBuilder {

  private HStreamClient client;
  private HStreamApiGrpc.HStreamApiStub grpcStub;
  private String sql;
  private Observer<HRecord> resultObserver;

  public QueryerBuilder(HStreamClient client, HStreamApiGrpc.HStreamApiStub grpcStub) {
    this.client = client;
    this.grpcStub = grpcStub;
  }

  /**
   * Set SQL statement the queryer will execute.
   *
   * @param sql SQL statement, only allow statements like "select ... emit changes"
   * @return the {@link QueryerBuilder} instance
   */
  public QueryerBuilder sql(String sql) {
    this.sql = sql;
    return this;
  }

  /**
   * Set {@link Observer} for sql results.
   *
   * @param resultObserver the {@link Observer} instance
   * @return the {@link QueryerBuilder} instance
   */
  public QueryerBuilder resultObserver(Observer<HRecord> resultObserver) {
    this.resultObserver = resultObserver;
    return this;
  }

  /**
   * Construct the final {@link Queryer} instance.
   *
   * @return the {@link Queryer} instance
   */
  public Queryer build() {
    return new QueryerImpl(client, grpcStub, sql, resultObserver);
  }
}
