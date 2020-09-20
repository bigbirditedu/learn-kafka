package com.bigbird.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * 自定义offset提交
 * 在Kafka中，offset默认存储在broker的内置Topic中，我们可以自定义存储位置
 * 比如为了保证消费和提交偏移量同时成功或失败，我们可以利用数据库事务来实现，把offset存储在Mysql即可
 * 下面的例子仅为示例代码，其中getOffset和commitOffset方法可以根据所选的offset存储系统(比如mysql)自行实现
 */
public class MyConsumer4 {
    public static Map<TopicPartition, Long> currentOffset = new HashMap<>();

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
        kafkaConsumer.subscribe(Arrays.asList("topic_test"), new ConsumerRebalanceListener() {
            //该方法会在Rebalanced之前调用
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                commitOffset(currentOffset);
            }

            //该方法会在Rebalanced之后调用
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                currentOffset.clear();
                for (TopicPartition partition : partitions) {
                    //定位到每个分区最近提交的offset位置继续消费
                    kafkaConsumer.seek(partition, getOffset(partition));
                }
            }
        });

        //消费者启动死循环不断消费
        while (true) {
            //一旦拉取到数据就返回，否则最多等待duration设定的时间
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            records.forEach(record -> {
                System.out.printf("topic = %s ,partition = %d,offset = %d, key = %s, value = %s%n", record.topic(), record.partition(),
                        record.offset(), record.key(), record.value());
                currentOffset.put(new TopicPartition(record.topic(), record.partition()), record.offset());
            });

            //提交offset
            commitOffset(currentOffset);
        }
    }

    /**
     * 获取某分区最新的offset
     *
     * @param partition
     * @return
     */
    private static long getOffset(TopicPartition partition) {
        return 0;
    }

    /**
     * 提交该消费者所有分区的offset
     *
     * @param currentOffset
     */
    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {
    }
}
