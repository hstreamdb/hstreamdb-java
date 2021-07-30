package io.hstream;

public class HStreamDBClientException extends RuntimeException {

  public HStreamDBClientException(final String message) {
    super(message);
  }

  public HStreamDBClientException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public HStreamDBClientException(final Throwable cause) {
    super(cause);
  }

  public static final class InvalidRecordException extends HStreamDBClientException {
    public InvalidRecordException(final String message) {
      super(message);
    }

    public InvalidRecordException(final String message, final Throwable cause) {
      super(message, cause);
    }
  }

  public static final class ConsumerException extends HStreamDBClientException {
    public ConsumerException(final String message) {
      super(message);
    }

    public ConsumerException(final String message, final Throwable cause) {
      super(message, cause);
    }
  }

  public static final class SubscribeException extends HStreamDBClientException {
    public SubscribeException(final String message) {
      super(message);
    }

    public SubscribeException(final String message, final Throwable cause) {
      super(message, cause);
    }
  }
}
