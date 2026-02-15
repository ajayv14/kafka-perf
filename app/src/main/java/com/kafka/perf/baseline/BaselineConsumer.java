package com.kafka.perf.baseline;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Scalable Kafka Consumer for Docker deployment
 * Configuration via environment variables:
 * - KAFKA_BROKERS: Bootstrap servers (default: localhost:9092)
 * - TOPIC: Topic to consume from (default: test-topic)
 * - GROUP_ID: Consumer group ID (default: scalable-consumer-group)
 * - ISOLATION_LEVEL: read_committed or read_uncommitted (default: read_committed)
 */
public class BaselineConsumer {

    private static final String DEFAULT_BROKERS = "localhost:9092";
    private static final String DEFAULT_TOPIC = "test-topic";
    private static final String DEFAULT_GROUP = "scalable-consumer-group";
    private static final String DEFAULT_ISOLATION_LEVEL = "read_committed";
    
    private static volatile boolean running = true;

    public static void main(String[] args) throws Exception {
        
        // Get configuration from environment variables
        String brokers = System.getenv("KAFKA_BROKERS") != null 
            ? System.getenv("KAFKA_BROKERS") 
            : DEFAULT_BROKERS;
        String topic = System.getenv("TOPIC") != null 
            ? System.getenv("TOPIC") 
            : DEFAULT_TOPIC;
        String groupId = System.getenv("GROUP_ID") != null 
            ? System.getenv("GROUP_ID") 
            : DEFAULT_GROUP;
        String isolationLevel = System.getenv("ISOLATION_LEVEL") != null 
            ? System.getenv("ISOLATION_LEVEL") 
            : DEFAULT_ISOLATION_LEVEL;
        
        // Print startup info
        System.out.println("╔" + "═".repeat(60) + "╗");
        System.out.println("║  SCALABLE KAFKA CONSUMER" + " ".repeat(35) + "║");
        System.out.println("╠" + "═".repeat(60) + "╣");
        System.out.println("║  Brokers: " + brokers + " ".repeat(Math.max(0, 50 - brokers.length())) + "║");
        System.out.println("║  Topic: " + topic + " ".repeat(Math.max(0, 52 - topic.length())) + "║");
        System.out.println("║  Group: " + groupId + " ".repeat(Math.max(0, 52 - groupId.length())) + "║");
        System.out.println("║  Isolation Level: " + isolationLevel + " ".repeat(Math.max(0, 42 - isolationLevel.length())) + "║");
        System.out.println("╚" + "═".repeat(60) + "╝\n");
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            System.out.println("\nShutdown initiated.");
        }));
        
        // Create and run consumer
        consume(brokers, topic, groupId, isolationLevel);
    }

    private static void consume(String brokers, String topic, String groupId, 
                               String isolationLevel) throws Exception {
        
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.StringDeserializer");
        
        // Consumer configuration
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        
        System.out.println("Consumer started, listening on topic: " + topic + "\n");
        
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : records) {
                        // Process message here if needed
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

}