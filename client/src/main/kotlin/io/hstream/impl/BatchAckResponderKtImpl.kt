package io.hstream.impl

import io.hstream.BatchAckResponder
import io.hstream.internal.RecordId

class BatchAckResponderKtImpl(private val ackSender: AckSender, private val recordIds: List<RecordId>) : BatchAckResponder {
    override fun ackAll() {
        for (id in recordIds) {
            ackSender.ack(id)
        }
    }
}