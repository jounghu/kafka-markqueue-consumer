package com.dobest.kafka.executor.async;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 2020/4/13 14:35
 *
 * @author hujiansong@dobest.com
 * @since 1.8
 */
public class TopicNameThreadFactory implements ThreadFactory {
    private String topicName;
    private static final AtomicLong incr = new AtomicLong(0);

    public TopicNameThreadFactory(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName(topicName + "-" + incr.getAndIncrement());
        return thread;
    }
}
