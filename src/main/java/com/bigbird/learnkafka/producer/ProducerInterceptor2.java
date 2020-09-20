package com.bigbird.learnkafka.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 拦截器案例-第2个拦截器统计发送成功和失败的消息
 */
public class ProducerInterceptor2 implements ProducerInterceptor<String, String> {

    private AtomicInteger successCounter = new AtomicInteger();
    private AtomicInteger failCounter = new AtomicInteger();

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            successCounter.getAndIncrement();
        } else {
            failCounter.getAndIncrement();
        }
    }

    @Override
    public void close() {
        System.out.println("successCounter:" + successCounter.get());
        System.out.println("failCounter:" + failCounter.get());
        System.out.println("发送成功率:" + successCounter.get() / (failCounter.get() + successCounter.get()));
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
