package com.dobest.kafka;


import com.alibaba.fastjson.JSON;
import com.dobest.kafka.processor.User;
import com.dobest.kafka.properties.KafkaProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * @author hujiansong
 */
@SpringBootApplication
public class BootApplication {

    public static void main(String[] args) throws IOException {
        SpringApplication.run(BootApplication.class, args);
        System.in.read();
    }


    /**
     * 启动生产者
     *
     * @param kafkaProperties kafka config for producer
     * @return
     */
    @Bean
    CommandLineRunner producer(KafkaProperties kafkaProperties) {
        return (args) -> {
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBrokerList());
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

            for (int i = 0; i < 100; i++) {
                producer.send(new ProducerRecord<>(
                        "mark-queue-test-1", UUID.randomUUID().toString(), JSON.toJSONString(
                        new User("userName" + i, "password" + i)
                )
                ));
            }

        };
    }

    /**
     * 启动消费者线程
     *
     * @param consumerGroups
     * @return
     */
    @Bean
    CommandLineRunner init(List<KafkaConsumerThread> consumerGroups) {
        return (args) -> {
            for (KafkaConsumerThread consumerThread : consumerGroups) {
                consumerThread.start();
            }
        };
    }
}
