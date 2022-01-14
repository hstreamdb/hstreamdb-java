package io.hstream;

/** The interface for the HStream BuffetedProducer. */
public interface BufferedProducer extends Producer {

  /** explicitly flush buffered records. */
  public void flush();

  /** close buffered producer. */
  public void close();
}
