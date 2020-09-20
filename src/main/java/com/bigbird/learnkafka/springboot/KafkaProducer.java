package com.bigbird.learnkafka.springboot;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * 只要classpath下引入kafkaTemplate类所在的jar包依赖，就会触发自动配置，因此可以在应用程序中直接注入KafkaTemplate
 * Kafka自动配置类会根据application.properties中的配置实例化kafkaTemplate
 * 当然也可以自定义构建kafkaTemplate的配置类，此时自动配置类检测到容器中已经有了KafkaTemplate实例，则不会进行自动配置
 */
@Component
public class KafkaProducer {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 普通异步发送
     *
     * @param msg
     * @return
     */
    public boolean sendMsgAsync(String msg) {
        //普通异步发送
        System.out.println("开始发送：" + LocalDateTime.now());
        kafkaTemplate.send("topic_test", msg);
        System.out.println("完成发送：" + LocalDateTime.now());

        //使用ProducerRecord发送消息
        ProducerRecord record = new ProducerRecord("topic_test", "use ProducerRecord to send message:" + msg);
        kafkaTemplate.send(record);

        //使用Message发送消息
        Map<String, Object> map = new HashMap<>(3);
        map.put(KafkaHeaders.TOPIC, "topic_test");
        map.put(KafkaHeaders.PARTITION_ID, 0);
        map.put(KafkaHeaders.MESSAGE_KEY, 0);
        GenericMessage message = new GenericMessage("use Message to send message:" + msg, new MessageHeaders(map));
        kafkaTemplate.send(message);

        return true;
    }

    /**
     * 同步发送
     * Future模式中采取异步执行事件，在需要返回值的地方调用get方法获取future的返回值，get方法会阻塞直到future执行完成返回
     *
     * @param msg
     * @return
     */
    public boolean sendMsgSync(String msg) throws ExecutionException, InterruptedException {
        System.out.println("开始发送：" + LocalDateTime.now());
        kafkaTemplate.send("topic_test", msg).get();
        System.out.println("完成发送：" + LocalDateTime.now());
        return true;
    }

    /**
     * 带回调函数的异步发送
     * 可以断网或者停掉kafka测试
     *
     * @param msg
     * @return
     */
    public boolean sendMsgAsyncWithCallback(String msg) {
        kafkaTemplate.setProducerListener(new ProducerListener<String, String>() {
            @Override
            public void onSuccess(ProducerRecord<String, String> producerRecord, RecordMetadata recordMetadata) {
                System.out.println("Message send success : " + producerRecord.value());
            }

            @Override
            public void onError(ProducerRecord<String, String> producerRecord, Exception exception) {
                System.out.println("Message send fail : " + producerRecord.toString());
            }
        });

        kafkaTemplate.send("topic_test", msg);
        return true;
    }
}
