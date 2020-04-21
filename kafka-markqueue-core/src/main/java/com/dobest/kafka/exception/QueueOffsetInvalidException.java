package com.dobest.kafka.exception;

import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Set;

/**
 * Copyright © 2020杭州游卡桌游. All rights reserved.
 *
 * @author hujiansong
 * @version V1.0
 * @Package cn.sunrisecolors.datalake.kafka.exception
 * @date 2020/3/25 15:37
 */
public class QueueOffsetInvalidException extends InvalidOffsetException {

    private TopicPartition tp;

    /**
     * will seek to offset
     */
    private long offset;


    public QueueOffsetInvalidException(String message, TopicPartition tp, long offset) {
        super(message);
        this.tp = tp;
        this.offset = offset;
    }

    public long getOffset() {
        return offset;
    }

    public TopicPartition getTp() {
        return tp;
    }

    @Override
    public Set<TopicPartition> partitions() {
        return Collections.singleton(this.tp);
    }
}
