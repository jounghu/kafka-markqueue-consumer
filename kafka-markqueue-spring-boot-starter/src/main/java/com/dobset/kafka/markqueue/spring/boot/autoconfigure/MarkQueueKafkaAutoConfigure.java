package com.dobset.kafka.markqueue.spring.boot.autoconfigure;

import com.dobest.kafka.KafkaConsumerThread;
import com.dobest.kafka.RecordHandlerExecutor;
import com.dobest.kafka.RecordProcessorChain;
import com.dobest.kafka.executor.async.AsyncRecordHandlerExecutor;
import com.dobest.kafka.executor.sync.SyncRecordHandlerExecutor;
import com.dobest.kafka.properties.KafkaProperties;
import com.dobest.kafka.reject.NoopRejectHandler;
import com.dobest.kafka.reject.RejectHandler;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * 2020/4/15 12:02
 *
 * @author hujiansong@dobest.com
 * @since 1.8
 */
@Configuration
@ConditionalOnBean(RecordProcessorChain.class)
public class MarkQueueKafkaAutoConfigure {

    @Bean
    @ConditionalOnMissingBean
    @ConfigurationProperties("markqueue.kafka")
    KafkaProperties kafkaProperties() {
        return new KafkaProperties();
    }

    @Bean
    @ConditionalOnMissingBean
    public RejectHandler rejectHandler() {
        return new NoopRejectHandler();
    }

    @Bean
    @ConditionalOnMissingBean
    public RecordHandlerExecutor executor(
            KafkaProperties kafkaProperties,
            RecordProcessorChain recordProcessorChain

    ) {
        if (kafkaProperties.getAsyncProcess()) {
            return new AsyncRecordHandlerExecutor(kafkaProperties, recordProcessorChain);
        } else {
            return new SyncRecordHandlerExecutor(kafkaProperties, recordProcessorChain);
        }
    }

    @Bean
    @ConditionalOnMissingBean
    public List<KafkaConsumerThread> consumerThreadList(
            KafkaProperties kafkaProperties,
            RecordHandlerExecutor executor,
            BuildProperties buildProperties
    ) {
        int threadNum = kafkaProperties.getFetcherThreadNum();
        List<KafkaConsumerThread> fetcherList = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            fetcherList.add(new KafkaConsumerThread<>(executor, kafkaProperties, buildProperties.getName()));
        }
        return fetcherList;
    }


}
