package com.bigbird.learnkafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 使用自定义生产者拦截器
 * Kafka Producer会在消息序列化和计算分区之前调用拦截器的onSend方法，可以在此方法中进行业务定制。一般不修改ProducerRecord的topic、key、partition等信息
 * Kafka Producer会在消息被应答(ack)之前或者消息发送失败时调用拦截器的 onAcknowledgement 方法，此方法在用户设置的异步CallBack方法之前执行。
 * onAcknowledgement方法的业务逻辑越简单越好，否则会影响发送性能，因为该方法运行在Producer的I/O线程中
 */
public class ProducerWithInterceptor {
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

        //构建拦截器链
        List<String> interceptors = new ArrayList<>();
        interceptors.add(ProducerInterceptor1.class.getName());
        interceptors.add(ProducerInterceptor2.class.getName());
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("topic_test", "hello " + i);
            kafkaProducer.send(record);
        }

        //关闭producer，才能触发Interceptor的close方法
        kafkaProducer.close();
    }
}
