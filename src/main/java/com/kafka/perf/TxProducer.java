package com.kafka.perf;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

public class TxProducer {

    private static final int NUM_RECORDS = 1000;

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");
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
        producer.initTransactions();

        List<Long> latenciesMs = new ArrayList<>(NUM_RECORDS);
        CountDownLatch latch = new CountDownLatch(NUM_RECORDS);

        long txnStartNs = System.nanoTime();
        long runStartNs = txnStartNs;

        try {
            producer.beginTransaction();

            for (int i = 0; i < NUM_RECORDS; i++) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>("eos-topic", "key-" + i, "value-" + i);

                long sendStartNs = System.nanoTime();

                producer.send(record, (metadata, exception) -> {
                    long latencyMs =
                            (System.nanoTime() - sendStartNs) / 1_000_000;

                    synchronized (latenciesMs) {
                        latenciesMs.add(latencyMs);
                    }
                    latch.countDown();

                    if (exception != null) {
                        exception.printStackTrace();
                    }
                });
            }

            latch.await(); // wait for all acks
            producer.commitTransaction();

        } catch (ProducerFencedException |
                 OutOfOrderSequenceException |
                 AuthorizationException fatal) {

            producer.close();
            throw fatal;

        } catch (KafkaException e) {
            producer.abortTransaction();
        } finally {
            producer.close();
        }

        long runEndNs = System.nanoTime();
        long txnEndNs = runEndNs;

        // ---- Metrics computation ----

        latenciesMs.sort(Long::compare);

        double runSeconds = (runEndNs - runStartNs) / 1_000_000_000.0;
        double txnDurationMs = (txnEndNs - txnStartNs) / 1_000_000.0;
        double throughput = NUM_RECORDS / runSeconds;

        long p50 = percentile(latenciesMs, 50);
        long p95 = percentile(latenciesMs, 95);
        long p99 = percentile(latenciesMs, 99);
        double avg = latenciesMs.stream().mapToLong(Long::longValue).average().orElse(0);

        System.out.println("==== Phase 1 Producer Metrics ====");
        System.out.printf("Records sent           : %d%n", NUM_RECORDS);
        System.out.printf("Throughput (rec/sec)   : %.2f%n", throughput);
        System.out.printf("Transaction duration ms: %.2f%n", txnDurationMs);
        System.out.printf("Avg latency ms         : %.2f%n", avg);
        System.out.printf("p50 latency ms         : %d%n", p50);
        System.out.printf("p95 latency ms         : %d%n", p95);
        System.out.printf("p99 latency ms         : %d%n", p99);
    }

    private static long percentile(List<Long> values, int percentile) {
        if (values.isEmpty()) return 0;
        int index = (int) Math.ceil(percentile / 100.0 * values.size()) - 1;
        return values.get(Math.max(index, 0));
    }
}
