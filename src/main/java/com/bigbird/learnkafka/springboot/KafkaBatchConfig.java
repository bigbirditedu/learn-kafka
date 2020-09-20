//package com.bigbird.learnkafka.springboot;
//
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.kafka.annotation.EnableKafka;
//import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
//import org.springframework.kafka.core.*;
//
//import java.util.HashMap;
//import java.util.Map;
//
///**
// * 通过自定义Kafka配置的方式增加批量消费的Listener
// */
//@Configuration
//@EnableKafka
//public class KafkaBatchConfig {
//
//    @Value("${spring.kafka.bootstrap-servers}")
//    private String bootstrapServers;
//
//    @Value("${spring.kafka.consumer.group-id}")
//    private String consumerGroupId;
//
//    @Value("${spring.kafka.producer.retries}")
//    private int retries;
//
//    @Value("${spring.kafka.producer.batch-size}")
//    private long batchSize;
//
//    @Value("${spring.kafka.producer.buffer-memory}")
//    private long bufferMemory;
//
//    @Value("${spring.kafka.producer.key-serializer}")
//    private String producerKeySerializer;
//
//    @Value("${spring.kafka.producer.value-serializer}")
//    private String producerValueSerializer;
//
//    @Value("${spring.kafka.consumer.key-deserializer}")
//    private String consumerKeyDeserializer;
//
//    @Value("${spring.kafka.consumer.value-deserializer}")
//    private String consumerValueDeserializer;
//
//    @Bean
//    ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory());
//        //设置消费者并发线程数,该值要根据topic的实际分区数来定,超过分区数的线程无效
//        //每一个消费者线程也可以看成一个独立的消费者进程，即一个独立的消费者
//        factory.setConcurrency(2);
//        factory.getContainerProperties().setPollTimeout(1000);
//        //设置为批量消费，批次数量在Kafka配置参数ConsumerConfig.MAX_POLL_RECORDS_CONFIG中设置
//        factory.setBatchListener(true);
//        return factory;
//    }
//
//    @Bean
//    public ConsumerFactory<String, String> consumerFactory() {
//        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
//    }
//
//    @Bean
//    public Map<String, Object> consumerConfigs() {
//        Map<String, Object> configProps = new HashMap<>();
//        // 只要指定1个broker地址，自动发现集群中的其它broker。指定多个，防止服务器故障
//        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, consumerKeyDeserializer);
//        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, consumerValueDeserializer);
//        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
//        // 批量消费消息数量
//        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
//
//        // -----------------------------可选配置--------------------------
//        // 自动提交偏移量
//        // 如果设置成true,偏移量由auto.commit.interval.ms控制自动提交的频率
//        // 如果设置成false,不需要定时的提交offset，可以自行控制提交offset
//        // 自动提交的动作是在consumer的poll方法里完成：每次发起拉取请求之前检查是否可以提交位移，如可以则提交上一次轮询的位移
//        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        // 自动提交的频率，可以通过减小时间间隔缩短消息重复消费的时间窗口大小
//        configProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
//        // Session超时设置
//        configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
//
//        // 该属性指定了消费者在读取一个没有偏移量的分区或者偏移量无效的情况下该作何处理：
//        // latest（默认值）在偏移量无效的情况下，消费者将从最新的记录开始读取数据（在消费者启动之后生成的记录）
//        // earliest：在偏移量无效的情况下，消费者将从起始位置读取分区的记录
//        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
//        return configProps;
//    }
//
//    @Bean
//    public Map<String, Object> producerConfigs() {
//        Map<String, Object> configProps = new HashMap<>();
//        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, producerKeySerializer);
//        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, producerValueSerializer);
//        // ----------------------------可选配置--------------------------
//        // 重试，0为不启用重试机制
//        configProps.put(ProducerConfig.RETRIES_CONFIG, retries);
//        // 控制批发送的大小，单位为字节。数据达到此大小时，批次里的所有消息被发送出去
//        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
//        // 批量发送时延迟的毫秒数，启用该功能能有效减少生产者发送消息次数，从而提高并发量
//        // 该参数指定了生产者在发送批次之前等待更多消息加入批次的时间，即数据达到批次大小或者linger.ms达到上限时都会触发发送
//        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 1);
//        // 生产者可以使用的总内存大小(字节)来缓冲等待发送的数据
//        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
//        //其它参数参考官方文档
//        return configProps;
//    }
//
//    @Bean
//    public KafkaTemplate<String, String> kafkaTemplate() {
//        return new KafkaTemplate<>(producerFactory());
//    }
//
//    @Bean
//    public ProducerFactory<String, String> producerFactory() {
//        return new DefaultKafkaProducerFactory<>(producerConfigs());
//    }
//}
