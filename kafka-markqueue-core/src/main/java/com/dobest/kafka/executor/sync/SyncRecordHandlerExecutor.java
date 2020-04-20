package com.dobest.kafka.executor.sync;

import com.dobest.kafka.AbstractRecordHandlerExecutor;
import com.dobest.kafka.RecordProcessorChain;
import com.dobest.kafka.properties.KafkaProperties;
import com.dobest.kafka.reject.RejectHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author shaohongliang
 * @since 2019/9/5 10:01
 */
public class SyncRecordHandlerExecutor<T> extends AbstractRecordHandlerExecutor<T> {


    public SyncRecordHandlerExecutor(
            KafkaProperties kafkaProperties,
            RecordProcessorChain recordProcessorChain
    ) {
        super(kafkaProperties, recordProcessorChain);
    }

    public SyncRecordHandlerExecutor(
            KafkaProperties kafkaProperties,
            RecordProcessorChain recordProcessorChain,
            RejectHandler rejectHandler
    ) {
        // used to config reject Handler
        super(kafkaProperties, recordProcessorChain, rejectHandler);
    }


    @Override
    public void submit(ConsumerRecord<String, T> record) {
        newRecordHandler(new SyncRecordContext(record)).run();
    }

    @Override
    public void commitOffset(KafkaConsumer<String, T> kafkaConsumer) {
        kafkaConsumer.commitSync();
    }

}
