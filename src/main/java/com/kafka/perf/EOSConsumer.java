
package com.kafka.perf;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class EOSConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "phase1-eos-consumer");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        /* EOS essentials */
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        /* Disable auto commit (important for correctness) */
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        /* Stable behavior */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("eos-topic"));

        try {
            while (true) {
                ConsumerRecords<String, String> records
                        = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf(
                            "Consumed offset=%d key=%s value=%s%n",
                            record.offset(), record.key(), record.value());
                }

                // Commit offsets AFTER processing
                consumer.commitSync();
            }
        } finally {
            consumer.close();
        }

    }

}
