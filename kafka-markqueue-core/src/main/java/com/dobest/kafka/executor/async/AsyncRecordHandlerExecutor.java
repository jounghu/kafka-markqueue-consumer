package com.dobest.kafka.executor.async;

import com.dobest.kafka.AbstractRecordHandlerExecutor;
import com.dobest.kafka.RecordContext;
import com.dobest.kafka.RecordProcessorChain;
import com.dobest.kafka.properties.KafkaProperties;
import com.dobest.kafka.reject.RejectHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * maxRunningSize = poolSize 个数 + queue 个数 + 1 (CallerRunsPolicy代表有一个允许在调用线程运行)
 * 由于压缩队列中连续标记完成的数据可以压缩为一条，如 0 0 1 1 1 0 1 --> 0 0 1 0 1，
 * 假设未完成的有N个（0的个数）,压缩后1的个数最多为1 + N，即1 0 1 0 ...0 1 这种情况,
 * 此时队列大小最小为2N + 1
 *
 * @author shaohongliang
 * @since 2019/8/8 14:16
 */
@Slf4j
public class AsyncRecordHandlerExecutor<T> extends AbstractRecordHandlerExecutor<T> {

    private ThreadPoolExecutor executorService;

    private Map<TopicPartition, MarkCompactQueue> partitionStates = new ConcurrentHashMap<>();

    private int compactQueueSize;


    public AsyncRecordHandlerExecutor(
            KafkaProperties kafkaProperties,
            RecordProcessorChain recordProcessorChain
    ) {
        super(kafkaProperties, recordProcessorChain);
        this.executorService = initExecutors();
        this.compactQueueSize = 2 * this.kafkaProperties.getAsyncProcessThreadNum();
    }

    public AsyncRecordHandlerExecutor(
            KafkaProperties kafkaProperties,
            RecordProcessorChain recordProcessorChain,
            RejectHandler rejectHandler
    ) {
        // used to config rejectHandler
        super(kafkaProperties, recordProcessorChain,rejectHandler);
        this.executorService = initExecutors();
        this.compactQueueSize = 2 * this.kafkaProperties.getAsyncProcessThreadNum();
    }

    private ThreadPoolExecutor initExecutors() {
        int poolSize = this.kafkaProperties.getAsyncProcessThreadNum();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                poolSize,
                poolSize,
                0L,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue(true),
                new TopicNameThreadFactory(this.kafkaProperties.getTopic()),
                (r, th) -> {
                    try {
                        th.getQueue().put(r);
                    } catch (InterruptedException e) {
                        //noop
                    }
                });
        return executor;
    }


    @Override
    public void submit(ConsumerRecord<String, T> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        MarkCompactQueue queue = getQueue(tp);
        AsyncRecordContext asyncRecordContext = new AsyncRecordContext(record, queue);
        if(queue.put(asyncRecordContext)){
            executorService.submit(newRecordHandler(asyncRecordContext));
        }
    }


    public MarkCompactQueue getQueue(TopicPartition tp) {
        MarkCompactQueue queue = partitionStates.get(tp);
        if (queue == null) {
            synchronized (this) {
                // double check
                queue = partitionStates.get(tp);
                if (queue == null) {
                    queue = new MarkCompactQueue(compactQueueSize);
                    partitionStates.put(tp, queue);
                }
            }
        }
        return queue;
    }


    /**
     * 整个应用共享一个RecordHandlerExecutor实例，需要传入Consumer对象区分不同的消费线程
     *
     * @param kafkaConsumer
     */
    @Override
    public void commitOffset(KafkaConsumer<String, T> kafkaConsumer) {
        Set<TopicPartition> topicPartitions = kafkaConsumer.assignment();
        log.debug("{} start commit offset, TopicPartitions {}", Thread.currentThread().getName(), topicPartitions);
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        List<MarkCompactQueue> allowSlideQueueList = new ArrayList<>();
        for (TopicPartition tp : topicPartitions) {
            MarkCompactQueue queue = partitionStates.get(tp);
            log.debug("{} assign MarkCompactQueue {}", Thread.currentThread().getName(), queue);
            // fix : when manual add  partition ,will throws NPE
            if (queue != null) {
                RecordContext context = queue.allowSlideWindowRecord();
                log.debug("{} allowSlideWindowRecord {}", Thread.currentThread().getName(), context);
                if (context != null) {
                    log.info("[{}] start commit , topic-partition: {}, offset: {}", Thread.currentThread().getName(), tp, context.getConsumerRecord().offset() + 1);
                    offsets.put(tp, new OffsetAndMetadata(context.getConsumerRecord().offset() + 1));
                    allowSlideQueueList.add(queue);
                }
            }
        }
        kafkaConsumer.commitSync(offsets);

        // 提交成功后，滑动窗口滚动
        allowSlideQueueList.forEach(circularQueue -> circularQueue.slideWindow());
    }

}
