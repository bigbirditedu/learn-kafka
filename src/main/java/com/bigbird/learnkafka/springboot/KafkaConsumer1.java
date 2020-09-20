package com.bigbird.learnkafka.springboot;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * 通过 @KafkaListener 注解即可开启kafka消费者
 * 每一个@KafkaListener注解相当于开启一个单独的consumer实例(消费者线程/进程)
 */
@Component
public class KafkaConsumer1 {
    @KafkaListener(topics = "topic_test")
    public void onMessage(String message) {
        System.out.println("msg1:" + message);
    }

    @KafkaListener(topics = "topic_test")
    public void onMessage1(ConsumerRecord<?, ?> record) {
        System.out.println("msg2:" + record.value());
    }

    @KafkaListener(topics = "topic_test")
    public void onMessage2(ConsumerRecord<?, ?> record, Acknowledgment ack, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        System.out.println("topic:" + topic);
        System.out.println("msg3:" + record.value());
        System.out.println("ack:" + ack);
    }
}
