package io.hstream;

public interface QueryerBuilder {

  /**
   * Set SQL statement the queryer will execute.
   *
   * @param sql SQL statement, only allow statements like "select ... emit changes"
   * @return the {@link QueryerBuilder} instance
   */
  QueryerBuilder sql(String sql);

  /**
   * Set {@link Observer} for sql results.
   *
   * @param resultObserver the {@link Observer} instance
   * @return the {@link QueryerBuilder} instance
   */
  QueryerBuilder resultObserver(Observer<HRecord> resultObserver);

  /**
   * Construct the final {@link Queryer} instance.
   *
   * @return the {@link Queryer} instance
   */
  Queryer build();
}
