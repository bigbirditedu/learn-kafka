package com.bigbird.learnkafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 测试ConsumerWithInterceptor中时间戳过滤是否生效
 * 可以在发送消息时手动修改Producer Record的timestamp
 */
public class Producer4ConsumerWithInterceptor1 {
    private static final long EXPIRE_INTERVAL = 10000;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "vm1:9092,vm2:9092,vm3:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16 * 1024);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32 * 1024 * 1024);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record;
            if (i % 2 == 0) {
                record = new ProducerRecord<>("topic_test", "hello " + i);
            } else {
                //奇数序号消息时间戳向前调整，测试消费者拦截器是否能过滤掉
                record = new ProducerRecord<>("topic_test", null, System.currentTimeMillis() - EXPIRE_INTERVAL, null, "hello " + i);
            }
            kafkaProducer.send(record);
        }

        //关闭producer，才能触发Interceptor的close方法
        kafkaProducer.close();
    }
}
