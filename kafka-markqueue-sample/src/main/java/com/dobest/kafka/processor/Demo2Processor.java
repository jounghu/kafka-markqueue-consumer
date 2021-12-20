package com.dobest.kafka.processor;

import com.alibaba.fastjson.JSON;
import com.dobest.kafka.RecordContext;
import com.dobest.kafka.RecordProcessor;
import lombok.extern.slf4j.Slf4j;

/**
 * 2020/4/20 14:50
 *
 * @author hujiansong@dobest.com
 * @since 1.8
 */
@Slf4j
public class Demo2Processor extends RecordProcessor<User> {
    @Override
    public void process(RecordContext<User> recordContext) {
        // do processor
        log.info("consumer {}", JSON.toJSONString(recordContext.value()));
        recordContext.ack();
    }
}
