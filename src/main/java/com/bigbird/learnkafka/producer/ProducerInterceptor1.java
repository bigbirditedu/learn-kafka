package com.bigbird.learnkafka.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 拦截器案例-第1个拦截器在消息体前增加时间戳信息
 */
public class ProducerInterceptor1 implements ProducerInterceptor<String, String> {

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 在消息体前加上时间戳
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(),
                System.currentTimeMillis() + "_" + record.value(), record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
