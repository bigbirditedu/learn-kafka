package com.bigbird.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 自定义消费者拦截器
 * Kafka consumer在poll()方法返回之前会先调用拦截器的onConsume方法，可以在此方法里预先对消息进行定制化操作
 * Kafka consumer在提交完消费位移之后会调用拦截器的onCommit方法，
 * 本拦截器功能：
 * 对消息的时间戳进行判断，过滤掉不满足时效（过期）的消息
 * 消费完成后打印位移信息
 */
public class ConsumerInterceptor1 implements ConsumerInterceptor<String, String> {

    private static final long EXPIRE_INTERVAL = 10000;

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        HashMap<TopicPartition, List<ConsumerRecord<String, String>>> newRecords = new HashMap<>(64);
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> recordsInPartition = records.records(partition);
            List<ConsumerRecord<String, String>> filteredRecords = new ArrayList<>();
            for (ConsumerRecord<String, String> record : recordsInPartition) {
                if (System.currentTimeMillis() - record.timestamp() < EXPIRE_INTERVAL) {
                    filteredRecords.add(record);
                }
            }
            if (!filteredRecords.isEmpty()) {
                newRecords.put(partition, filteredRecords);
            }
        }
        return new ConsumerRecords<>(newRecords);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((tp, offset) -> {
            System.out.println(tp + ":" + offset.offset());
        });
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
