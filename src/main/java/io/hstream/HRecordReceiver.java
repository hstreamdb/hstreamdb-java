package io.hstream;

public interface HRecordReceiver {

  void processHRecord(ReceivedHRecord receivedHRecord, Responder responder);
}
