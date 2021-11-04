package io.hstream;

/**
 * This interface can be implemented by users of {@link io.hstream.Consumer} to receive raw records.
 */
public interface RawRecordReceiver {

  /**
   * Used to consume raw format record.
   *
   * @param receivedRawRecord record received from producer
   * @param responder {@link Responder} used to ack producer when received message.
   */
  void processRawRecord(ReceivedRawRecord receivedRawRecord, Responder responder);
}
