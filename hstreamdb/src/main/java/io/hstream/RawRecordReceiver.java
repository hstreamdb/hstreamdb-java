package io.hstream;

/** the interface that user use to process raw record received from server */
public interface RawRecordReceiver {

  /**
   * used to consume raw format record.
   *
   * @param receivedRawRecord record received from producer
   * @param responder {@link Responder} used to ack producer when received message.
   */
  void processRawRecord(ReceivedRawRecord receivedRawRecord, Responder responder);
}
