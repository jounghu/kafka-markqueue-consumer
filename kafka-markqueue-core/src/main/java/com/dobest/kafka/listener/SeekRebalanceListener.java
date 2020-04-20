package com.dobest.kafka.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;

/**
 * Copyright © 2020杭州游卡桌游. All rights reserved.
 *
 * @author hujiansong
 * @version V1.0
 * @Package cn.sunrisecolors.datalake.kafka.rebalance
 * @date 2020/2/19 20:13
 * <p>
 * <p>
 * <p>
 * <p>
 * <p>
 * auto.offset.reset=none 的时候
 * 第一次消费，如果不指定offset，那么会抛出一个NoOffsetForPartition异常
 */
@Slf4j
public class SeekRebalanceListener implements ConsumerRebalanceListener {

    private KafkaConsumer kafkaConsumer;

    public SeekRebalanceListener(KafkaConsumer kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // 获取LOG-END-OFFSET
        Map<TopicPartition, Long> logEndOffsetMap = kafkaConsumer.endOffsets(partitions);
        Map<TopicPartition, Long> startOffsetMap = kafkaConsumer.beginningOffsets(partitions);
        for (TopicPartition topicPartition : partitions) {
            // 获取该分区已经消费的偏移量
            OffsetAndMetadata committed = kafkaConsumer.committed(topicPartition);
            if (committed != null) {
                long committedOffset = committed.offset();
                long logEndOffset = logEndOffsetMap.getOrDefault(topicPartition, 0L);
                long startOffset = startOffsetMap.getOrDefault(topicPartition, 0L);
                // 校验
                if (committedOffset > logEndOffset) {
                    // 已经提交的offset比最大的offset还大，数组越界
                    // 重置committedOffset为LOG-END-OFFSET
                    log.info("Kafka committed offset({}) > logEndOffset({}),will seek to logEndOffset, topic partition:{}, Offset:{}",
                            committedOffset,
                            logEndOffset,
                            topicPartition, logEndOffset);
                    // 重置偏移量到上一次提交的偏移量的下一个位置处开始消费
                    kafkaConsumer.seek(topicPartition, logEndOffset);
                } else {
                    log.info("Kafka normal offset, will seek to Max(startOffset:{},committedOffset:{}), topic partition {}, offset {}",
                            startOffset,
                            committedOffset,
                            topicPartition,
                            Math.max(committedOffset, startOffset));
                    kafkaConsumer.seek(topicPartition, Math.max(committedOffset, startOffset));
                }
            } else {
                long startOffset = startOffsetMap.getOrDefault(topicPartition, 0L);
                // no consumer offset will reset to 0
                log.info("Kafka has no committed offset, will seek to startOffset, topic partition {}, offset {}", topicPartition, startOffset);
                kafkaConsumer.seek(topicPartition, startOffset);
            }

        }
    }
}
