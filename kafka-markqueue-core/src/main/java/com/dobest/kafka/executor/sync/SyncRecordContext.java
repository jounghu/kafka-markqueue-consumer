package com.dobest.kafka.executor.sync;

import com.dobest.kafka.AbsRecordContext;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author shaohongliang
 * @since 2019/9/5 10:35
 */
@Getter
public class SyncRecordContext<T> extends AbsRecordContext<T> {
    private ConsumerRecord<String, T> consumerRecord;

    private boolean isAcked;


    public SyncRecordContext(ConsumerRecord<String, T> consumerRecord) {
        this.consumerRecord = consumerRecord;
    }

    @Override
    public void ack() {
        this.isAcked = true;
    }

}
