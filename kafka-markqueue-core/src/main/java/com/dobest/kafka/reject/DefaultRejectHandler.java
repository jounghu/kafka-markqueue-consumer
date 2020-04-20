package com.dobest.kafka.reject;

import com.dobest.kafka.RecordContext;
import lombok.extern.slf4j.Slf4j;

/**
 * Copyright © 2020杭州游卡桌游. All rights reserved.
 *
 * @author hujiansong
 * @version V1.0
 * @Package cn.sunrisecolors.datalake.kafka.reject
 * @date 2020/3/25 14:04
 */
@Slf4j
public class DefaultRejectHandler implements RejectHandler {
    @Override
    public <T> boolean reject(RecordContext<T> recordContext) {
        log.error("Record retry error! {}", recordContext);
        throw new RuntimeException("Record retry error! " + recordContext);
    }
}
