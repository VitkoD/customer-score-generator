package com.fabriik.customerscoregenerator.service;

import com.fabriik.customerscoregenerator.domain.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.kafka.core.KafkaTemplate;

import javax.annotation.PostConstruct;
import java.util.Random;

@Service
public class CustomerGenerator {

    private final Logger log = LoggerFactory.getLogger(CustomerGenerator.class);
    private final Random randomizer = new Random();

    @Autowired
    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public CustomerGenerator(KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostConstruct
    public void runApplication() {
        while (true) {
            sendMessage();;
        }
    }

    private void sendMessage() {
        byte[] message = createCustomer();
        ListenableFuture<SendResult<String, byte[]>> future = kafkaTemplate.send("customers", message);
        future.addCallback(new ListenableFutureCallback<SendResult<String, byte[]>>() {

            @Override
            public void onSuccess(SendResult<String, byte[]> result) {
//             log.debug("Sent message=[" + message +
//                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            }

            @Override
            public void onFailure(Throwable ex) {
                log.error("Unable to send message=[ " + message + " ] due to : " + ex.getMessage());
            }
        });
    }

    private byte[] createCustomer() {
        return Customer.customer
                .newBuilder()
                .setId(getRandomNumberInRange(1000000))
                .setScore(getRandomNumberInRange(100))
                .build()
                .toByteArray();
    }

    private int getRandomNumberInRange(int max) {
        return randomizer.nextInt(max) + 1;
    }
}