package com.kafka.client;

import java.io.IOException;

// Handle starting producer or consumer
public class Run {
    public static void main(String[] args) throws IOException {

        String brokers = "localhost:9092";
        String topicName = "consumer";
        String groupId = "0";

        Consumer.consume(brokers, groupId, topicName);
    }
}