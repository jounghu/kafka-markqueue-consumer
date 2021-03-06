package com.dobest.kafka.reject;

import com.dobest.kafka.RecordContext;
import lombok.extern.slf4j.Slf4j;

/**
 * Copyright © 2020杭州游卡桌游. All rights reserved.
 *
 * @author hujiansong
 * @version V1.0
 * @Package cn.sunrisecolors.datalake.kafka.reject
 * @date 2020/3/18 15:13
 */
@Slf4j
public class DiscardRejectHandler implements RejectHandler {
    @Override
    public <T> boolean reject(RecordContext<T> recordContext) {
        log.info("Retry max times, will discard this record! {}", recordContext);
        recordContext.ack();
        return true;
    }
}
