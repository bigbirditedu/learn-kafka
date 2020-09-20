package com.bigbird.learnkafka.springboot;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Springboot提供了自动配置的KafakTemplate，只要classpath中引入KafakTemplate、在配置文件中配置spring.kafka开头的配置项，即可在程序中注入KafakTemplate使用
 * 当然，我们也可以根据需要自定义配置KafkaTemplate,如果没有特别的配置，无需自己编写配置类，采用自动化配置即可;如果需要非常全面的参数配置，建议自定义配置类
 */
@Configuration
@EnableKafka
public class KafkaConfig {

    /**
     * 配置参数可以写在application.properties里，通过类似KafkaProperties方式扫描前缀注入
     * 这里简单写死在配置类里
     */
    private static final String KAFKA_SERVERS_CONFIG = "vm1:9092,vm2:9092,vm3:9092";
    private static final String LOCAL_GROUP_ID = "test_kafka_config";

    /**
     * 配置消费者
     * @return
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String>
    kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVERS_CONFIG);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, LOCAL_GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVERS_CONFIG);
        //当设置了缓冲区，如果消息总不够条数、或者消息不够buffer大小就不发送了吗?当消息超过linger时间，也会发送
        props.put(ProducerConfig.LINGER_MS_CONFIG, 2);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    /**
     * 配置生产者
     * @return
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<String, String>(producerFactory());
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

}
