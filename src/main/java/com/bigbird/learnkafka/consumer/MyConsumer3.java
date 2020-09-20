package com.bigbird.learnkafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * 手动异步提交offset
 */
public class MyConsumer3 {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "vm1:9092,vm2:9092,vm3:9092");
        //group.id相同的属于同一个消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_test");
        //关闭自动提交offset,手动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Arrays.asList("topic_test"));
        //消费者启动死循环不断消费
        while (true) {
            //一旦拉取到数据就返回，否则最多等待duration设定的时间
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            records.forEach(record -> {
                System.out.printf("topic = %s ,partition = %d,offset = %d, key = %s, value = %s%n", record.topic(), record.partition(),
                        record.offset(), record.key(), record.value());
            });

            //异步提交，可以带回调函数，线程不会阻塞
            kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception != null) {
                        System.out.println("提交失败：" + offsets);
                    }
                }
            });
        }
    }
}
