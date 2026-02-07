package com.kafka.perf;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.DoubleSummaryStatistics;
import java.util.List;

public class ProducerMetricsUtil {

    private static final int NUM_RECORDS = 100000;

    public static void printSummaryStatistics(List<IterationResult> results) {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("SUMMARY STATISTICS (n=" + results.size() + ")");
        System.out.println("=".repeat(70));

        // Throughput statistics
        DoubleSummaryStatistics throughputStats = results.stream()
                .mapToDouble(r -> r.throughput)
                .summaryStatistics();
        double throughputStdDev = calculateListStdDev(
                results.stream().mapToDouble(r -> r.throughput).toArray(),
                throughputStats.getAverage()
        );
        double throughputCV = (throughputStdDev / throughputStats.getAverage()) * 100;

        System.out.println("\n-- Throughput --");
        System.out.printf("Mean ± StdDev: %.2f ± %.2f rec/sec%n",
                throughputStats.getAverage(), throughputStdDev);
        System.out.printf("Min / Max: %.2f / %.2f rec/sec%n",
                throughputStats.getMin(), throughputStats.getMax());
        System.out.printf("Coefficient of Variation: %.2f%%%n", throughputCV);

        // Transaction duration statistics
        DoubleSummaryStatistics txnStats = results.stream()
                .mapToDouble(r -> r.txnDurationMs)
                .summaryStatistics();
        double txnStdDev = calculateListStdDev(
                results.stream().mapToDouble(r -> r.txnDurationMs).toArray(),
                txnStats.getAverage()
        );

        System.out.println("\n-- Transaction Duration --");
        System.out.printf("Mean ± StdDev: %.2f ± %.2f ms%n",
                txnStats.getAverage(), txnStdDev);
        System.out.printf("Min / Max: %.2f / %.2f ms%n",
                txnStats.getMin(), txnStats.getMax());

        // Latency statistics
        System.out.println("\n-- Latency Statistics (ms) --");
        printLatencyStats("Min", results.stream().mapToDouble(r -> r.minLatency).toArray());
        printLatencyStats("Avg", results.stream().mapToDouble(r -> r.avgLatency).toArray());
        printLatencyStats("p50", results.stream().mapToDouble(r -> r.p50Latency).toArray());
        printLatencyStats("p95", results.stream().mapToDouble(r -> r.p95Latency).toArray());
        printLatencyStats("p99", results.stream().mapToDouble(r -> r.p99Latency).toArray());
        printLatencyStats("p99.9", results.stream().mapToDouble(r -> r.p999Latency).toArray());
        printLatencyStats("Max", results.stream().mapToDouble(r -> r.maxLatency).toArray());

        // Batch size statistics
        DoubleSummaryStatistics avgBatchStats = results.stream()
                .mapToDouble(r -> r.avgBatchSize)
                .summaryStatistics();
        DoubleSummaryStatistics maxBatchStats = results.stream()
                .mapToDouble(r -> r.maxBatchSize)
                .summaryStatistics();

        System.out.println("\n-- Batch Size (bytes) --");
        System.out.printf("Avg batch size: %.2f ± %.2f bytes%n",
                avgBatchStats.getAverage(),
                calculateListStdDev(
                        results.stream().mapToDouble(r -> r.avgBatchSize).toArray(),
                        avgBatchStats.getAverage()
                )
        );
        System.out.printf("Max batch size: %.2f ± %.2f bytes%n",
                maxBatchStats.getAverage(),
                calculateListStdDev(
                        results.stream().mapToDouble(r -> r.maxBatchSize).toArray(),
                        maxBatchStats.getAverage()
                )
        );

        System.out.println("\n" + "=".repeat(70));
        System.out.println("\nNOTE: Collect broker-side metrics separately using JMX or monitoring tools.");
        System.out.println("See accompanying BrokerMetricsCollector.java for automated collection.");
    }

    public static void exportIterationsToCSV(List<IterationResult> results, String filename) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
            // Write header
            writer.println("iteration,throughput_rec_per_sec,txn_duration_ms," +
                    "min_latency_ms,avg_latency_ms,stddev_latency_ms," +
                    "p50_latency_ms,p95_latency_ms,p99_latency_ms,p999_latency_ms,max_latency_ms," +
                    "avg_batch_size_bytes,max_batch_size_bytes");

            // Write data
            for (IterationResult result : results) {
                writer.printf("%d,%.2f,%.2f,%d,%.2f,%.2f,%d,%d,%d,%d,%d,%.2f,%.2f%n",
                        result.iteration,
                        result.throughput,
                        result.txnDurationMs,
                        result.minLatency,
                        result.avgLatency,
                        result.stdDevLatency,
                        result.p50Latency,
                        result.p95Latency,
                        result.p99Latency,
                        result.p999Latency,
                        result.maxLatency,
                        result.avgBatchSize,
                        result.maxBatchSize
                );
            }
        } catch (IOException e) {
            System.err.println("Error writing iterations CSV: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void exportSummaryToCSV(List<IterationResult> results, String filename) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
            // Calculate all summary statistics
            DoubleSummaryStatistics throughputStats = results.stream()
                    .mapToDouble(r -> r.throughput)
                    .summaryStatistics();
            double throughputStdDev = calculateListStdDev(
                    results.stream().mapToDouble(r -> r.throughput).toArray(),
                    throughputStats.getAverage()
            );
            double throughputCV = (throughputStdDev / throughputStats.getAverage()) * 100;

            DoubleSummaryStatistics txnStats = results.stream()
                    .mapToDouble(r -> r.txnDurationMs)
                    .summaryStatistics();
            double txnStdDev = calculateListStdDev(
                    results.stream().mapToDouble(r -> r.txnDurationMs).toArray(),
                    txnStats.getAverage()
            );

            // Latency stats
            double[] minLatencies = results.stream().mapToDouble(r -> r.minLatency).toArray();
            double[] avgLatencies = results.stream().mapToDouble(r -> r.avgLatency).toArray();
            double[] p50Latencies = results.stream().mapToDouble(r -> r.p50Latency).toArray();
            double[] p95Latencies = results.stream().mapToDouble(r -> r.p95Latency).toArray();
            double[] p99Latencies = results.stream().mapToDouble(r -> r.p99Latency).toArray();
            double[] p999Latencies = results.stream().mapToDouble(r -> r.p999Latency).toArray();
            double[] maxLatencies = results.stream().mapToDouble(r -> r.maxLatency).toArray();

            DoubleSummaryStatistics avgBatchStats = results.stream()
                    .mapToDouble(r -> r.avgBatchSize)
                    .summaryStatistics();
            DoubleSummaryStatistics maxBatchStats = results.stream()
                    .mapToDouble(r -> r.maxBatchSize)
                    .summaryStatistics();

            // Write metadata
            writer.println("# Kafka Performance Test Summary");
            writer.println("# Generated: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            writer.println("# Number of iterations: " + results.size());
            writer.println("# Records per iteration: " + NUM_RECORDS);
            writer.println();

            // Write summary in key-value format
            writer.println("metric,mean,stddev,min,max,coefficient_of_variation");
            
            writer.printf("throughput_rec_per_sec,%.2f,%.2f,%.2f,%.2f,%.2f%%%n",
                    throughputStats.getAverage(),
                    throughputStdDev,
                    throughputStats.getMin(),
                    throughputStats.getMax(),
                    throughputCV
            );

            writer.printf("txn_duration_ms,%.2f,%.2f,%.2f,%.2f,%n",
                    txnStats.getAverage(),
                    txnStdDev,
                    txnStats.getMin(),
                    txnStats.getMax()
            );

            writer.println();
            writer.println("# Latency Statistics (ms)");
            writer.println("latency_metric,mean,stddev");
            
            writeLatencyStatRow(writer, "min_latency_ms", minLatencies);
            writeLatencyStatRow(writer, "avg_latency_ms", avgLatencies);
            writeLatencyStatRow(writer, "p50_latency_ms", p50Latencies);
            writeLatencyStatRow(writer, "p95_latency_ms", p95Latencies);
            writeLatencyStatRow(writer, "p99_latency_ms", p99Latencies);
            writeLatencyStatRow(writer, "p999_latency_ms", p999Latencies);
            writeLatencyStatRow(writer, "max_latency_ms", maxLatencies);

            writer.println();
            writer.println("# Batch Size Statistics (bytes)");
            writer.println("batch_metric,mean,stddev");
            
            writer.printf("avg_batch_size_bytes,%.2f,%.2f%n",
                    avgBatchStats.getAverage(),
                    calculateListStdDev(
                            results.stream().mapToDouble(r -> r.avgBatchSize).toArray(),
                            avgBatchStats.getAverage()
                    )
            );

            writer.printf("max_batch_size_bytes,%.2f,%.2f%n",
                    maxBatchStats.getAverage(),
                    calculateListStdDev(
                            results.stream().mapToDouble(r -> r.maxBatchSize).toArray(),
                            maxBatchStats.getAverage()
                    )
            );

        } catch (IOException e) {
            System.err.println("Error writing summary CSV: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void writeLatencyStatRow(PrintWriter writer, String label, double[] values) {
        double mean = 0;
        for (double v : values) mean += v;
        mean /= values.length;

        double stddev = calculateListStdDev(values, mean);

        writer.printf("%s,%.2f,%.2f%n", label, mean, stddev);
    }

    private static void printLatencyStats(String label, double[] values) {
        double mean = 0;
        for (double v : values) mean += v;
        mean /= values.length;

        double stddev = calculateListStdDev(values, mean);

        System.out.printf("%-6s: %.2f ± %.2f ms%n", label, mean, stddev);
    }

    public static long percentile(List<Long> values, double percentile) {
        if (values.isEmpty()) return 0;
        int index = (int) Math.ceil(percentile / 100.0 * values.size()) - 1;
        return values.get(Math.max(index, 0));
    }

    public static double calculateStdDev(List<Long> values, double mean) {
        if (values.isEmpty()) return 0;

        double sumSquaredDiff = 0;
        for (Long value : values) {
            double diff = value - mean;
            sumSquaredDiff += diff * diff;
        }

        return Math.sqrt(sumSquaredDiff / values.size());
    }

    public static double calculateListStdDev(double[] values, double mean) {
        if (values.length == 0) return 0;

        double sumSquaredDiff = 0;
        for (double value : values) {
            double diff = value - mean;
            sumSquaredDiff += diff * diff;
        }

        return Math.sqrt(sumSquaredDiff / values.length);
    }

    // Inner class to store iteration results
    public static class IterationResult {
        public int iteration;
        public double throughput;
        public double txnDurationMs;
        public long minLatency;
        public double avgLatency;
        public double stdDevLatency;
        public long p50Latency;
        public long p95Latency;
        public long p99Latency;
        public long p999Latency;
        public long maxLatency;
        public double avgBatchSize;
        public double maxBatchSize;

        public IterationResult(int iteration, double throughput, double txnDurationMs,
                        long minLatency, double avgLatency, double stdDevLatency,
                        long p50Latency, long p95Latency, long p99Latency,
                        long p999Latency, long maxLatency,
                        double avgBatchSize, double maxBatchSize) {
            this.iteration = iteration;
            this.throughput = throughput;
            this.txnDurationMs = txnDurationMs;
            this.minLatency = minLatency;
            this.avgLatency = avgLatency;
            this.stdDevLatency = stdDevLatency;
            this.p50Latency = p50Latency;
            this.p95Latency = p95Latency;
            this.p99Latency = p99Latency;
            this.p999Latency = p999Latency;
            this.maxLatency = maxLatency;
            this.avgBatchSize = avgBatchSize;
            this.maxBatchSize = maxBatchSize;
        }
    }
}
