package com.bigbird.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * 使用自定义消费者拦截器
 */
public class ConsumerWithInterceptor1 {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "vm1:9092");
        //group.id相同的属于同一个消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_test");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //自动提交offset,每1s提交一次
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //使用自定义消费者拦截器
        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerInterceptor1.class.getName());
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Arrays.asList("topic_test"));
        //消费者启动死循环不断消费
        while (true) {
            //一旦拉取到数据就返回，否则最多等待duration设定的时间
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            records.forEach(record -> {
                System.out.printf("topic = %s ,partition = %d,offset = %d, key = %s, key = %d, value = %s%n", record.topic(), record.partition(),
                        record.offset(), record.key(), record.timestamp(), record.value());
            });
        }
    }
}
