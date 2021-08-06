package io.hstream;

/** the interface that user use to process {@link HRecord} received from server */
public interface HRecordReceiver {

  /**
   * used to consume {@link HRecord} format message.
   *
   * @param receivedHRecord {@link ReceivedHRecord} received from producer
   * @param responder {@link Responder} used to ack producer when received message.
   */
  void processHRecord(ReceivedHRecord receivedHRecord, Responder responder);
}
