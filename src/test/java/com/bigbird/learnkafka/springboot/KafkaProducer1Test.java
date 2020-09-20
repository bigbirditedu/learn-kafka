package com.bigbird.learnkafka.springboot;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
class KafkaProducer1Test {

    @Autowired
    KafkaProducer kafkaProducer;

    @Test
    void sendMsgAsync() {
        kafkaProducer.sendMsgAsync("hello,I am Async test");
    }

    @Test
    void sendMsgSync() throws Exception{
        kafkaProducer.sendMsgSync("hello,I am Sync test");
    }

    @Test
    void sendMsgAsyncWithCallback() throws Exception{
        kafkaProducer.sendMsgAsyncWithCallback("hello,I am sendMsgAsyncWithCallback test");
    }
}
