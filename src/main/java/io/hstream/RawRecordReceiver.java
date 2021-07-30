package io.hstream;

public interface RawRecordReceiver {

  void processRawRecord(ReceivedRawRecord receivedRawRecord, Responder responder);
}
