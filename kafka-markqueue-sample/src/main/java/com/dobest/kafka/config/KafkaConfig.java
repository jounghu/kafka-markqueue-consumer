package com.dobest.kafka.config;

import com.dobest.kafka.RecordProcessorChain;
import com.dobest.kafka.processor.DemoProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 2020/4/20 15:02
 *
 * @author hujiansong@dobest.com
 * @since 1.8
 */
@Configuration
public class KafkaConfig {

    @Bean
    RecordProcessorChain recordProcessorChain(){
        return new RecordProcessorChain()
                .addProcessor(new DemoProcessor());
    }
}
