package io.hstream;

/** a Publisher is a provider of a potentially unbounded number of sequenced elements */
public interface Publisher<V> {

  /**
   * Request {@link Publisher} to start streaming data.
   *
   * @param o the {@link Observer} that will consume data from this {@link Publisher}
   */
  void subscribe(Observer<? super V> o);
}
