package com.dobest.kafka.properties;

import lombok.Data;

/**
 * @author :hujiansong
 * @date :2019/6/19 17:44
 * @since :1.8
 */
@Data
public class KafkaProperties {

    /**
     * kafka brokerlist
     */
    private String brokerList;

    /**
     * kafka consumer groupId
     */
    private String groupId;

    /**
     * Kafka topic
     */
    private String topic;


    /**
     * auto offset reset
     */
    private String autoOffsetReset = "latest";


    /**
     * consumer fetcher thread nums
     */
    private Integer fetcherThreadNum = Runtime.getRuntime().availableProcessors();


    /**
     * pool timeout milliseconds, also is offset commit duration
     */
    private long pollTimeoutMs = 30000;


    /**
     * use async thread pool process kafka record
     */
    private Boolean asyncProcess = true;


    /**
     * record handler thread nums
     */
    private Integer asyncProcessThreadNum;

    /**
     * consumer max.poll.record
     */
    private Integer maxPollRecord;

    /**
     * 失败重试次数re
     */
    private Integer maxRetryNums;

    private String deserializerClazz;

    /**
     * 默认为 json序列化 true
     */
    private boolean jsonDeserializer = true;


}
