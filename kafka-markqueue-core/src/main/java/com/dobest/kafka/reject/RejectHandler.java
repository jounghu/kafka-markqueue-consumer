package com.dobest.kafka.reject;


import com.dobest.kafka.RecordContext;

/**
 * Copyright © 2020杭州游卡桌游. All rights reserved.
 *
 * @author hujiansong
 * @version V1.0
 * @Package cn.sunrisecolors.datalake.kafka.reject
 * @date 2020/3/18 15:11
 */
public interface RejectHandler {

    <T> boolean reject(RecordContext<T> recordContext);
}
