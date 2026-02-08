package com.kafka.perf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * Collects broker-side metrics from Kafka using JMX.
 * 
 * Prerequisites:
 * 1. Enable JMX on Kafka brokers in docker-compose.yml:
 *    environment:
 *      KAFKA_JMX_PORT: 10000
 *      KAFKA_JMX_HOSTNAME: kafka-1
 *      KAFKA_JMX_OPTS: "-Djava.rmi.server.hostname=kafka-1 
 *                       -Dcom.sun.management.jmxremote 
 *                       -Dcom.sun.management.jmxremote.authenticate=false 
 *                       -Dcom.sun.management.jmxremote.ssl=false
 *                       -Dcom.sun.management.jmxremote.rmi.port=10000"
 * 
 * 2. Expose JMX ports in docker-compose.yml:
 *    ports:
 *      - "9092:9092"
 *      - "10000:10000"
 * 
 * Usage (Single Broker):
 *   BrokerMetricsCollector collector = new BrokerMetricsCollector("localhost", 10000);
 *   collector.connect();
 *   BrokerMetrics before = collector.collectMetrics();
 *   // ... run your producer test ...
 *   BrokerMetrics after = collector.collectMetrics();
 *   collector.printDelta(before, after);
 *   collector.disconnect();
 * 
 * Usage (All Brokers):
 *   BrokerMetricsCollectorGroup group = new BrokerMetricsCollectorGroup();
 *   group.connectAll();
 *   Map<Integer, BrokerMetrics> before = group.collectMetricsFromAllBrokers();
 *   // ... run your producer test ...
 *   Map<Integer, BrokerMetrics> after = group.collectMetricsFromAllBrokers();
 *   group.printAllMetrics(after);
 *   group.disconnectAll();
 */
public class BrokerMetricsCollector {

    private final String host;
    private final int jmxPort;
    private MBeanServerConnection mbsc;
    private JMXConnector jmxConnector;

    public BrokerMetricsCollector(String host, int jmxPort) {
        this.host = host;
        this.jmxPort = jmxPort;
    }

    public void connect() throws IOException {
        String jmxUrl = String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", host, jmxPort);
        JMXServiceURL serviceUrl = new JMXServiceURL(jmxUrl);
        jmxConnector = JMXConnectorFactory.connect(serviceUrl, null);
        mbsc = jmxConnector.getMBeanServerConnection();
        System.out.println("Connected to JMX: " + jmxUrl);
    }

    public void disconnect() throws IOException {
        if (jmxConnector != null) {
            jmxConnector.close();
        }
    }

    public BrokerMetrics collectMetrics() throws Exception {
        BrokerMetrics metrics = new BrokerMetrics();

        // Messages in per second
        metrics.messagesInPerSec = getDoubleAttribute(
                "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec",
                "OneMinuteRate"
        );

        // Bytes in per second
        metrics.bytesInPerSec = getDoubleAttribute(
                "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec",
                "OneMinuteRate"
        );

        // Bytes out per second
        metrics.bytesOutPerSec = getDoubleAttribute(
                "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec",
                "OneMinuteRate"
        );

        // Produce request metrics
        metrics.produceRequestTotalTimeMs = getDoubleAttribute(
                "kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce",
                "Mean"
        );

        metrics.produceRequestQueueTimeMs = getDoubleAttribute(
                "kafka.network:type=RequestMetrics,name=RequestQueueTimeMs,request=Produce",
                "Mean"
        );

        metrics.produceLocalTimeMs = getDoubleAttribute(
                "kafka.network:type=RequestMetrics,name=LocalTimeMs,request=Produce",
                "Mean"
        );

        metrics.produceRemoteTimeMs = getDoubleAttribute(
                "kafka.network:type=RequestMetrics,name=RemoteTimeMs,request=Produce",
                "Mean"
        );

        metrics.produceResponseQueueTimeMs = getDoubleAttribute(
                "kafka.network:type=RequestMetrics,name=ResponseQueueTimeMs,request=Produce",
                "Mean"
        );

        // Log flush metrics
        metrics.logFlushRateAndTimeMs = getDoubleAttribute(
                "kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs",
                "Mean"
        );

        // Request handler metrics
        metrics.requestHandlerAvgIdlePercent = getDoubleAttribute(
                "kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent",
                "OneMinuteRate"
        );

        // Network processor metrics
        metrics.networkProcessorAvgIdlePercent = getDoubleAttribute(
                "kafka.network:type=Processor,name=IdlePercent,networkProcessor=0",
                "Value"
        );

        // Transaction coordinator metrics (if available)
        try {
            metrics.transactionCommitTimeMs = getDoubleAttribute(
                    "kafka.coordinator.transaction:type=TransactionMarkerChannelManager,name=TransactionCommitRequestTimeMs",
                    "Mean"
            );
        } catch (Exception e) {
            // Transaction metrics might not be available
            metrics.transactionCommitTimeMs = -1.0;
        }

        return metrics;
    }

    private double getDoubleAttribute(String objectName, String attribute) throws Exception {
        ObjectName name = new ObjectName(objectName);
        Object value = mbsc.getAttribute(name, attribute);
        
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return 0.0;
    }

    public void printMetrics(BrokerMetrics metrics) {
        printMetrics(null, metrics);
    }

    public void printMetrics(String brokerName, BrokerMetrics metrics) {
        String header = brokerName != null ? "\n==== Broker Metrics (" + brokerName + ") ====" : "\n==== Broker Metrics ====";
        System.out.println(header);
        System.out.printf("Messages in/sec        : %.2f%n", metrics.messagesInPerSec);
        System.out.printf("Bytes in/sec           : %.2f%n", metrics.bytesInPerSec);
        System.out.printf("Bytes out/sec          : %.2f%n", metrics.bytesOutPerSec);
        System.out.println("\n-- Request Latency (ms) --");
        System.out.printf("Total time             : %.2f%n", metrics.produceRequestTotalTimeMs);
        System.out.printf("Request queue time     : %.2f%n", metrics.produceRequestQueueTimeMs);
        System.out.printf("Local time             : %.2f%n", metrics.produceLocalTimeMs);
        System.out.printf("Remote time            : %.2f%n", metrics.produceRemoteTimeMs);
        System.out.printf("Response queue time    : %.2f%n", metrics.produceResponseQueueTimeMs);
        System.out.println("\n-- Other Metrics --");
        System.out.printf("Log flush time (ms)    : %.2f%n", metrics.logFlushRateAndTimeMs);
        System.out.printf("Request handler idle %%: %.2f%n", metrics.requestHandlerAvgIdlePercent);
        System.out.printf("Network proc idle %%   : %.2f%n", metrics.networkProcessorAvgIdlePercent);
        if (metrics.transactionCommitTimeMs >= 0) {
            System.out.printf("Txn commit time (ms)  : %.2f%n", metrics.transactionCommitTimeMs);
        }
    }

    public void printDelta(BrokerMetrics before, BrokerMetrics after) {
        System.out.println("\n==== Broker Metrics (Delta) ====");
        System.out.printf("Δ Messages in/sec      : %.2f%n", after.messagesInPerSec - before.messagesInPerSec);
        System.out.printf("Δ Bytes in/sec         : %.2f%n", after.bytesInPerSec - before.bytesInPerSec);
        System.out.printf("Δ Total time (ms)      : %.2f%n", after.produceRequestTotalTimeMs - before.produceRequestTotalTimeMs);
        System.out.printf("Δ Log flush time (ms)  : %.2f%n", after.logFlushRateAndTimeMs - before.logFlushRateAndTimeMs);
    }

    // Data class to hold metrics
    public static class BrokerMetrics {
        double messagesInPerSec;
        double bytesInPerSec;
        double bytesOutPerSec;
        double produceRequestTotalTimeMs;
        double produceRequestQueueTimeMs;
        double produceLocalTimeMs;
        double produceRemoteTimeMs;
        double produceResponseQueueTimeMs;
        double logFlushRateAndTimeMs;
        double requestHandlerAvgIdlePercent;
        double networkProcessorAvgIdlePercent;
        double transactionCommitTimeMs;
    }

    // Example usage
    public static void main(String[] args) {
        try {
            // Example: Connect to a single broker
            BrokerMetricsCollector collector = new BrokerMetricsCollector("localhost", 10000);
            collector.connect();
            
            BrokerMetrics metrics = collector.collectMetrics();
            collector.printMetrics("Broker-1", metrics);
            
            collector.disconnect();
            
            // Example: Connect to all brokers
            System.out.println("\n\n========== Collecting from All Brokers ==========");
            BrokerMetricsCollectorGroup group = new BrokerMetricsCollectorGroup();
            group.connectAll();
            
            Map<Integer, BrokerMetrics> allMetrics = group.collectMetricsFromAllBrokers();
            group.printAllMetrics(allMetrics);
            
            group.disconnectAll();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

/**
 * Manages JMX connections to all Kafka brokers for collecting metrics.
 * Supports connecting to all three brokers and collecting/printing metrics.
 */
class BrokerMetricsCollectorGroup {
    
    private static final int NUM_BROKERS = 3;
    private static final String[] BROKER_HOSTS = {"localhost", "localhost", "localhost"};
    private static final int[] BROKER_JMX_PORTS = {10000, 10001, 10002};
    private static final String[] BROKER_NAMES = {"kafka-broker-1", "kafka-broker-2", "kafka-3"};
    
    private final Map<Integer, BrokerMetricsCollector> collectors = new HashMap<>();
    
    /**
     * Connect to all broker JMX endpoints
     */
    public void connectAll() throws Exception {
        System.out.println("Connecting to all Kafka brokers...");
        for (int i = 0; i < NUM_BROKERS; i++) {
            try {
                BrokerMetricsCollector collector = new BrokerMetricsCollector(BROKER_HOSTS[i], BROKER_JMX_PORTS[i]);
                collector.connect();
                collectors.put(i + 1, collector);
                System.out.printf("✓ Connected to %s (port %d)%n", BROKER_NAMES[i], BROKER_JMX_PORTS[i]);
            } catch (Exception e) {
                System.err.printf("✗ Failed to connect to %s: %s%n", BROKER_NAMES[i], e.getMessage());
                // Continue trying other brokers
            }
        }
        
        if (collectors.isEmpty()) {
            throw new Exception("Could not connect to any brokers!");
        }
    }
    
    /**
     * Disconnect from all broker JMX endpoints
     */
    public void disconnectAll() throws Exception {
        System.out.println("\nDisconnecting from all brokers...");
        for (Map.Entry<Integer, BrokerMetricsCollector> entry : collectors.entrySet()) {
            try {
                entry.getValue().disconnect();
                System.out.printf("✓ Disconnected from %s%n", BROKER_NAMES[entry.getKey() - 1]);
            } catch (Exception e) {
                System.err.printf("✗ Error disconnecting from broker %d: %s%n", entry.getKey(), e.getMessage());
            }
        }
    }
    
    /**
     * Collect metrics from all connected brokers
     */
    public Map<Integer, BrokerMetricsCollector.BrokerMetrics> collectMetricsFromAllBrokers() throws Exception {
        Map<Integer, BrokerMetricsCollector.BrokerMetrics> allMetrics = new HashMap<>();
        
        for (Map.Entry<Integer, BrokerMetricsCollector> entry : collectors.entrySet()) {
            int brokerId = entry.getKey();
            try {
                BrokerMetricsCollector.BrokerMetrics metrics = entry.getValue().collectMetrics();
                allMetrics.put(brokerId, metrics);
            } catch (Exception e) {
                System.err.printf("✗ Error collecting metrics from broker %d: %s%n", brokerId, e.getMessage());
            }
        }
        
        return allMetrics;
    }
    
    /**
     * Print metrics from all brokers in a formatted table
     */
    public void printAllMetrics(Map<Integer, BrokerMetricsCollector.BrokerMetrics> allMetrics) {
        System.out.println("\n╔════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                        METRICS FROM ALL BROKERS                          ║");
        System.out.println("╚════════════════════════════════════════════════════════════════════════════════════╝");
        
        for (int i = 1; i <= NUM_BROKERS; i++) {
            if (allMetrics.containsKey(i)) {
                BrokerMetricsCollector.BrokerMetrics metrics = allMetrics.get(i);
                String brokerName = BROKER_NAMES[i - 1];
                
                System.out.println();
                System.out.printf("┌─ Broker: %s (Port %d) ────────────────────────────────────────────────────────────────┐%n",
                        brokerName, BROKER_JMX_PORTS[i - 1]);
                System.out.printf("│ Messages in/sec         : %15.2f                                             │%n", metrics.messagesInPerSec);
                System.out.printf("│ Bytes in/sec            : %15.2f                                             │%n", metrics.bytesInPerSec);
                System.out.printf("│ Bytes out/sec           : %15.2f                                             │%n", metrics.bytesOutPerSec);
                System.out.printf("│ Request total time (ms) : %15.2f                                             │%n", metrics.produceRequestTotalTimeMs);
                System.out.printf("│ Request queue time (ms) : %15.2f                                             │%n", metrics.produceRequestQueueTimeMs);
                System.out.printf("│ Log flush time (ms)     : %15.2f                                             │%n", metrics.logFlushRateAndTimeMs);
                System.out.printf("│ Request handler idle %%  : %15.2f                                             │%n", metrics.requestHandlerAvgIdlePercent);
                System.out.printf("│ Network proc idle %%     : %15.2f                                             │%n", metrics.networkProcessorAvgIdlePercent);
                System.out.println("└────────────────────────────────────────────────────────────────────────────────────────┘");
            }
        }
    }
    
    /**
     * Print aggregate metrics (average, min, max) across all brokers
     */
    public void printAggregateMetrics(Map<Integer, BrokerMetricsCollector.BrokerMetrics> allMetrics) {
        if (allMetrics.isEmpty()) {
            System.out.println("No metrics collected.");
            return;
        }
        
        System.out.println("\n╔════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                     AGGREGATE METRICS ACROSS ALL BROKERS                  ║");
        System.out.println("╚════════════════════════════════════════════════════════════════════════════════════╝");
        
        // Calculate aggregates
        double msgAvg = allMetrics.values().stream().mapToDouble(m -> m.messagesInPerSec).average().orElse(0);
        double bytesInAvg = allMetrics.values().stream().mapToDouble(m -> m.bytesInPerSec).average().orElse(0);
        double bytesOutAvg = allMetrics.values().stream().mapToDouble(m -> m.bytesOutPerSec).average().orElse(0);
        double totalTimeAvg = allMetrics.values().stream().mapToDouble(m -> m.produceRequestTotalTimeMs).average().orElse(0);
        double queueTimeAvg = allMetrics.values().stream().mapToDouble(m -> m.produceRequestQueueTimeMs).average().orElse(0);
        double logFlushAvg = allMetrics.values().stream().mapToDouble(m -> m.logFlushRateAndTimeMs).average().orElse(0);
        double handlerIdleAvg = allMetrics.values().stream().mapToDouble(m -> m.requestHandlerAvgIdlePercent).average().orElse(0);
        double procIdleAvg = allMetrics.values().stream().mapToDouble(m -> m.networkProcessorAvgIdlePercent).average().orElse(0);
        
        System.out.println();
        System.out.printf("├─ Messages in/sec (avg)         : %.2f%n", msgAvg);
        System.out.printf("├─ Bytes in/sec (avg)            : %.2f%n", bytesInAvg);
        System.out.printf("├─ Bytes out/sec (avg)           : %.2f%n", bytesOutAvg);
        System.out.printf("├─ Request total time/sec (avg)  : %.2f ms%n", totalTimeAvg);
        System.out.printf("├─ Request queue time/sec (avg)  : %.2f ms%n", queueTimeAvg);
        System.out.printf("├─ Log flush time (avg)          : %.2f ms%n", logFlushAvg);
        System.out.printf("├─ Request handler idle %% (avg)  : %.2f%%%n", handlerIdleAvg);
        System.out.printf("└─ Network proc idle %% (avg)     : %.2f%%%n", procIdleAvg);
    }
}
