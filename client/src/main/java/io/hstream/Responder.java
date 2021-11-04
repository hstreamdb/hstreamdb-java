package io.hstream;

/** An object used to send acks to the HStream servers. */
public interface Responder {

  /** Acknowledges that the message has been successfully processed. */
  void ack();
}
