package io.hstream;

/**
 * This interface can be implemented by users of {@link io.hstream.Consumer} to receive {@link
 * HRecord}s.
 */
public interface HRecordReceiver {

  /**
   * Used to receive {@link HRecord} format message.
   *
   * @param receivedHRecord {@link ReceivedHRecord} received from producer
   * @param responder {@link Responder} used to ack producer when received message
   */
  void processHRecord(ReceivedHRecord receivedHRecord, Responder responder);
}
