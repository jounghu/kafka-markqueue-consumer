package com.dobest.kafka;

import com.dobest.kafka.exception.QueueOffsetInvalidException;
import com.dobest.kafka.listener.SeekRebalanceListener;
import com.dobest.kafka.properties.KafkaProperties;
import com.dobest.kafka.serialization.JsonDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 消费者线程，根据KafkaProperties.getFetcherThreadNum()启动多个实例
 *
 * @author shaohongliang
 * @since 2019/8/8 14:01
 */
@Slf4j
public class KafkaConsumerThread<T> extends Thread {
    private final KafkaConsumer<String, T> kafkaConsumer;

    private final RecordHandlerExecutor executor;

    private long pollTimeoutMs;

    private static AtomicLong counter = new AtomicLong(0);


    /**
     * @param executor
     * @param kafkaProperties
     * @param projectId       projectId use for mintor
     */
    public KafkaConsumerThread(RecordHandlerExecutor executor, KafkaProperties kafkaProperties, String projectId) {
        this.pollTimeoutMs = kafkaProperties.getPollTimeoutMs();
        this.kafkaConsumer = createConsumer(kafkaProperties, projectId);
        this.executor = executor;
    }

    private KafkaConsumer<String, T> createConsumer(KafkaProperties kafkaProperties, String projectId) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBrokerList());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getGroupId());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaProperties.getAutoOffsetReset());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(false));
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(kafkaProperties.getMaxPollRecord()));
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.setProperty(JsonDeserializer.VALUE_DESERIALIZER_JSON_CLASS_CONFIG, kafkaProperties.getDeserializerClazz());

        // set client.id and set thread name
        String clientId = buildClientId(projectId, kafkaProperties.getTopic());
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        setName(clientId);

        // assign topic and set rebalance listener
        KafkaConsumer<String, T> consumer = null;
        try {
             consumer = new KafkaConsumer<>(props);
        } catch (Exception e) {
            e.printStackTrace();
        }
        consumer.subscribe(Collections.singletonList(kafkaProperties.getTopic()), new SeekRebalanceListener(consumer));
        return consumer;
    }

    /**
     * used for monitor
     *
     * @param projectName projectName used consul to find address
     * @param topic       topic used mark which thread are consuming
     * @return
     */
    private String buildClientId(String projectName, String topic) {
        return String.join("#", projectName, topic, String.valueOf(counter.getAndIncrement()));
    }

    @Override
    public void run() {

        while (true) {
            try {
                // 超过pollTimeoutMs后poll方法会返回
                ConsumerRecords<String, T> records = kafkaConsumer.poll(Duration.ofMillis(this.pollTimeoutMs));
                log.debug("拉取消息数量,poll received {} records", records.count());
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, T> record : records) {
                        // 将每条消息记录分发到线程池执行
                        executor.submit(record);
                    }
                }
                // 检查并提交偏移量
                executor.commitOffset(kafkaConsumer);
            } catch (QueueOffsetInvalidException e) {
                log.error("markQueue offset error! TopicPartitons" + e.partitions(), e);
                doSeekOffset(kafkaConsumer, e.partitions());
            } catch (Exception e) {
                log.error("kafka take failed", e);
            }
        }
    }

    private void doSeekOffset(KafkaConsumer<String, T> kafkaConsumer, Set<TopicPartition> e) {
        new SeekRebalanceListener(kafkaConsumer).onPartitionsAssigned(e);
    }
}