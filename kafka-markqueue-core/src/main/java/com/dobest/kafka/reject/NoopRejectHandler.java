package com.dobest.kafka.reject;

import com.dobest.kafka.RecordContext;
import lombok.extern.slf4j.Slf4j;

/**
 * Copyright © 2020杭州游卡桌游. All rights reserved.
 *
 * @author hujiansong
 * @version V1.0
 * @Package cn.sunrisecolors.datalake.kafka.reject
 * @date 2020/3/18 16:26
 */
@Slf4j
public class NoopRejectHandler implements RejectHandler {

    @Override
    public <T> boolean reject(RecordContext<T> recordContext) {
        // noop will always retry
        log.info(" {}-{} {} Do noop reject...",
                recordContext.getConsumerRecord().topic(),
                recordContext.getConsumerRecord().partition(),
                recordContext.getConsumerRecord().offset()
        );
        return false;
    }
}
