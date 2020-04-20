package com.dobest.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author shaohongliang
 * @since 2019/9/5 9:58
 */
public interface RecordHandlerExecutor<T> {
    void submit(ConsumerRecord<String, T> record);

    void commitOffset(KafkaConsumer<String, T> kafkaConsumer);

    /**
     * @return
     */
    String topic();

}
