package io.hstream;

public interface Responder {

    /** used to respond server when receive messages. */
  void ack();
}
