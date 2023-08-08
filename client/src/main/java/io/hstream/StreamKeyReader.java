package io.hstream;

public interface StreamKeyReader extends AutoCloseable {

  boolean hasNext();

  ReceivedRecord next();
}
