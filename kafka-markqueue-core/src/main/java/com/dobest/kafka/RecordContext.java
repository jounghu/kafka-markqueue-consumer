package com.dobest.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.Map;

/*
 *包装类, 封装了Consumer对象供提交Offset时使用
 * @author shaohongliang
 * @since 2019/8/8 15:42
 */
public interface RecordContext<T> {

    void putAllProcessVal(List<Map<String, Object>> vals);

    List<Map<String, Object>> getProcessVal();

    ConsumerRecord<String, T> getConsumerRecord();

    void ack();

    boolean isAcked();

    default T value() {
        return getConsumerRecord().value();
    }

    void setHandlerName(String handlerThreadName);

    /**
     * to clear handler threadname when this record have been acked
     */
    void clearHandlerName();
}
