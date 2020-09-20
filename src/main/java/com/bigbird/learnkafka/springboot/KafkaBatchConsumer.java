//package com.bigbird.learnkafka.springboot;
//
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.stereotype.Component;
//
//import java.util.List;
//import java.util.Optional;
//
///**
// * kafka消费者开启批量消费
// */
//@Component
//public class KafkaBatchConsumer {
//
//    /**
//     * 也可以指定containerFactory，即@KafkaListener(topics = "${spring.kafka.topic}", containerFactory="batchFactory")
//     * 这里没有其它containerFactory实例，所以可以不用手动指定名称
//     *
//     * @param records
//     */
//    @KafkaListener(topics = "${spring.kafka.topic}")
//    public void onMessage(List<ConsumerRecord<?, ?>> records) {
//        System.out.println("接收到消息数量：" + records.size());
//        for (ConsumerRecord record : records) {
//            Optional<?> kafkaMessage = Optional.ofNullable(record.value());
//            if (kafkaMessage.isPresent()) {
//                Object message = record.value();
//                String topic = record.topic();
//                System.out.println("接收到topic: " + topic + " 的消息：" + message);
//            }
//        }
//    }
//}
