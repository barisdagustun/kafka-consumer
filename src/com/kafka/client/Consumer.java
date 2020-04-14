package com.kafka.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class Consumer {
    public static int consume(String brokers, String groupId, String topicName) {
        // Create a consumer
        KafkaConsumer consumer;
        // Configure the consumer
        Properties properties = new Properties();
        // Point it to the brokers
        properties.setProperty("bootstrap.servers", brokers);
        // Set the consumer group (all consumers must belong to a group).
        properties.setProperty("group.id", groupId);
        // Set how to serialize key/value pairs
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");

        // When a group is first created, it has no offset stored to start reading from. This tells it to start
        // with the earliest record in the stream.
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("enable.auto.commit", "false");

        consumer = new KafkaConsumer(properties);
        int count = 0;
        try {
            Duration interval = Duration.ofMinutes(1);
            consumer.subscribe(Arrays.asList(topicName));
            ObjectMapper mapper = new ObjectMapper();

            KafkaProducer producer = new KafkaProducer(properties);
            while (true) {
                // Poll for records
                ConsumerRecords records = consumer.poll(interval);
                Set partitions = records.partitions();
                partitions.forEach((partition) -> {
                    List partitionRecords = records.records((TopicPartition) partition);
                    for (int i = 0; i < partitionRecords.size(); i++) {
                        ConsumerRecord record = (ConsumerRecord) partitionRecords.get(i);
                        JsonNode jsonNode = (JsonNode) record.value();

                        if (jsonNode.get("firstName").asText().equals("onder")) {
                            ProducerRecord rec = new ProducerRecord("filter", jsonNode);
                            producer.send(rec);
                        }
                        try {
                            System.out.println(mapper.treeToValue(jsonNode, Contact.class));
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                        System.out.printf(count + ":offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());

                    }
                });
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            //consumer.close();
        }
        return count;
    }
}