package com.dobest.kafka.executor.async;

import com.dobest.kafka.AbsRecordContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author shaohongliang
 * @since 2019/9/5 10:35
 */
@Getter
@Slf4j
public class AsyncRecordContext<T> extends AbsRecordContext<T> {
    private ConsumerRecord<String, T> consumerRecord;

    private MarkCompactQueue markCompactQueue;

    private boolean isAcked;

    public AsyncRecordContext(ConsumerRecord<String, T> consumerRecord, MarkCompactQueue markCompactQueue) {
        this.consumerRecord = consumerRecord;
        this.markCompactQueue = markCompactQueue;
    }


    @Override
    public void ack() {
        this.isAcked = true;
        markCompactQueue.tigger();
        log.debug("RecordContext ack: {}", this);
    }

    @Override
    public String toString() {
        return "Record: " + this.consumerRecord.topic()
                + "-" + this.consumerRecord.partition()
                + "-" + consumerRecord.offset()
                + " Acked: " + this.isAcked;
    }
}
