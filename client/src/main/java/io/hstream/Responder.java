package io.hstream;

/**
 * after receiving messages from the server, the interface defines methods related to the response
 */
public interface Responder {

  /** used to respond server when receive messages. */
  void ack();
}
