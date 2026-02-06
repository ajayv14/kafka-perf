package com.kafka.perf;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

public class TxProducer {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        /* EOS essentials */
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        /* Transaction */
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "phase1-tx-producer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

// MUST be called exactly once per producer instance
        producer.initTransactions();

        try {
            producer.beginTransaction();

            for (int i = 0; i < 1000; i++) {
                ProducerRecord<String, String> record
                        = new ProducerRecord<>("eos-topic", "key-" + i, "value-" + i);

                producer.send(record);
            }

            producer.commitTransaction();
            System.out.println("Transaction committed");

        } catch (ProducerFencedException
                | OutOfOrderSequenceException
                | AuthorizationException fatal) {

            // Cannot recover → close producer
            producer.close();
            throw fatal;

        } catch (KafkaException e) {
            // Abort & continue safely
            producer.abortTransaction();
        } finally {
            producer.close();
        }

    }

}
