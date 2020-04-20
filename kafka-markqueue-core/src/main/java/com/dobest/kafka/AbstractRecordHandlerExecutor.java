package com.dobest.kafka;


import com.dobest.kafka.properties.KafkaProperties;
import com.dobest.kafka.reject.NoopRejectHandler;
import com.dobest.kafka.reject.RejectHandler;

/**
 * 2020/4/16 11:22
 *
 * @author hujiansong@dobest.com
 * @since 1.8
 */
public abstract class AbstractRecordHandlerExecutor<T> implements RecordHandlerExecutor<T> {

    protected KafkaProperties kafkaProperties;
    protected RecordProcessorChain recordProcessorChain;
    private RejectHandler rejectHandler;


    public AbstractRecordHandlerExecutor(
            KafkaProperties kafkaProperties,
            RecordProcessorChain recordProcessorChain,
            RejectHandler rejectHandler
    ) {
        this.kafkaProperties = kafkaProperties;
        this.recordProcessorChain = recordProcessorChain;
        this.rejectHandler = rejectHandler;
    }

    public AbstractRecordHandlerExecutor(
            KafkaProperties kafkaProperties,
            RecordProcessorChain recordProcessorChain
    ) {
        this(kafkaProperties,recordProcessorChain,new NoopRejectHandler());
    }


    @Override
    public String topic() {
        return kafkaProperties.getTopic();
    }


    protected RecordHandler newRecordHandler(RecordContext ctx) {
        return new RecordHandler(
                recordProcessorChain,
                ctx,
                kafkaProperties.getMaxRetryNums(),
                rejectHandler
        );
    }

}
