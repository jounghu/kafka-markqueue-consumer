package com.dobest.kafka.executor.async;


import com.dobest.kafka.RecordContext;
import com.dobest.kafka.exception.QueueOffsetInvalidException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;

/**
 * 带标记的压缩队列
 *
 * @author shaohongliang
 * @since 2019/8/8 14:36
 */
@Slf4j
public class MarkCompactQueue {
    private int front;//指向队首
    private int rear;//指向队尾C
    private AsyncRecordContext[] elem;
    private int maxSize;//最大容量
    private int allowSlideWindowIndex = -1;
    private Object obj = new Object(); // 阻塞锁

    public MarkCompactQueue(int maxSize) {
        this.elem = new AsyncRecordContext[maxSize];
        this.maxSize = maxSize;
        this.front = 0;
        this.rear = 0;
    }

    public boolean isEmpty() {
        return rear == front;
    }

    public boolean isFull() {
        return (rear + 1) % maxSize == front;
    }

    public long getLastOffset() {
        long queueMaxOffset = -1;
        AsyncRecordContext lastRecord = elem[(rear - 1 + maxSize) % maxSize];
        if (lastRecord != null) {
            queueMaxOffset = lastRecord.getConsumerRecord().offset();
        }
        return queueMaxOffset;
    }

    public long getFrontOffset() {
        long frontOffset = 0;
        if (elem[front] != null) {
            frontOffset = elem[front].getConsumerRecord().offset();
        }
        return frontOffset;
    }

    public List<AsyncRecordContext> getQueueContent() {
        int start = this.front;
        int end = this.rear < this.front ? rear + maxSize : rear;
        List<AsyncRecordContext> contexts = new ArrayList<>();
        for (int i = start; i < end; i++) {
            contexts.add(this.elem[i % maxSize]);
        }
        return contexts;
    }


    public int getMaxSize() {
        return maxSize;
    }

    public boolean put(AsyncRecordContext recordContext) {
        // 只有当前一个offset小于当前offset那么才放进去
        ConsumerRecord record = recordContext.getConsumerRecord();
        long curOffset = record.offset();

        if (curOffset == getLastOffset() + 1 || isEmpty()) {
            while (isFull()) {
                // 如果已满，尝试压缩
                if (!compact()) {
                    synchronized (obj) {
                        try {
                            obj.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            elem[rear++] = recordContext;
            rear %= maxSize;
            return true;
        } else if (curOffset >= getFrontOffset() && curOffset <= getLastOffset()) {
            log.warn("Record has been processed! curOffset {}, frontOffset {}, rearOffset {}", curOffset, getFrontOffset(), getLastOffset());
            return false;
        }
        throw new QueueOffsetInvalidException("非法Offset! rearOffset=" + getLastOffset() + ", frontOffset=" + getFrontOffset() + ", curOffset=" + curOffset
                , new TopicPartition(record.topic(), record.partition()), getLastOffset() + 1);

    }

    private boolean invalidOffset(long curOffset) {
        long lastOffset = getLastOffset();
        long frontOffset = getFrontOffset();
        if (isEmpty() || lastOffset == -1) {
            return false;
        }
        return curOffset > lastOffset + 1 || frontOffset > curOffset;
    }


    public RecordContext allowSlideWindowRecord() {
        RecordContext record = null;

        int start = front;
        int end = rear < front ? rear + maxSize : rear;

        for (int i = start; i < end; i++) {
            int index = i >= maxSize ? i - maxSize : i;
            if (elem[index].isAcked()) {
                record = elem[index];
                this.allowSlideWindowIndex = index;
            } else {
                break;
            }
        }

        return record;
    }

    public void slideWindow() {
        if (allowSlideWindowIndex > 0) {
            this.front = (allowSlideWindowIndex + 1) % maxSize;
            this.allowSlideWindowIndex = -1;
        }
    }

    /**
     * @return 压缩成功与否
     */
    private boolean compact() {
        int start = front;
        int end = (rear < front ? rear + maxSize : rear) - 1;

        boolean lastProcessed = false;
        boolean hasMoved = false;
        int compactedLen = 0;
        for (int i = end; i >= start; i--) {
            int index = i >= maxSize ? i - maxSize : i;
            if (elem[index].isAcked()) {
                if (lastProcessed) {
                    // 如果上一个也是标记的，可压缩数量+1
                    compactedLen++;
                }
                lastProcessed = true;
            } else {
                lastProcessed = false;
                hasMoved = true;
            }

            // 如果此前移动过，那么就需要一直移动
            // 移动的步长：compactedLen
            if (hasMoved) {
                if (index + compactedLen >= maxSize) {
                    elem[index + compactedLen - maxSize] = elem[index];
                } else {
                    elem[index + compactedLen] = elem[index];
                }
            }

        }

        if (compactedLen > 0) {
            front += compactedLen;
            if (front >= maxSize) {
                front -= maxSize;
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * 标记发生变化，唤醒put方法的阻塞
     */
    public void tigger() {
        synchronized (obj) {
            obj.notify();
        }
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        AsyncRecordContext frontCtx = this.elem[front];
        AsyncRecordContext rearCtx = this.elem[(rear - 1 + maxSize) % maxSize];
        sb.append("MarkQueue")
                .append(" frontCtx: ").append(frontCtx)
                .append(" rearCtx: ").append(rearCtx);
        return sb.toString();
    }
}
