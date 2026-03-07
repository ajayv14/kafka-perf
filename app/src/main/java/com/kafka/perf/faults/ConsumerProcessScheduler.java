package com.kafka.perf.faults;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Launches multiple consumer JVM processes from one Java entry point.
 *
 * Usage:
 *   java -cp <classpath> com.kafka.perf.faults.ConsumerProcessScheduler [count] [mainClass]
 *
 * Defaults:
 *   count=3
 *   mainClass=com.kafka.perf.faults.FaultInjectorConsumer
 */
public class ConsumerProcessScheduler {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerProcessScheduler.class);

    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        String mainClass = args.length > 1 ? args[1] : "com.kafka.perf.faults.FaultInjectorConsumer";

        if (count <= 0) {
            throw new IllegalArgumentException("count must be > 0");
        }

        String javaBin = System.getProperty("java.home") + "/bin/java";
        String classPath = System.getProperty("java.class.path");

        logger.info("Starting {} consumer process(es) using {}", count, mainClass);

        List<Process> processes = new ArrayList<>();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown requested, terminating child consumer processes...");
            for (Process p : processes) {
                if (p.isAlive()) {
                    p.destroy();
                }
            }
        }));

        for (int i = 0; i < count; i++) {
            ProcessBuilder pb = new ProcessBuilder(
                javaBin,
                "-cp", classPath,
                mainClass
            );

            pb.inheritIO();
            pb.environment().put("CONSUMER_INSTANCE_ID", String.valueOf(i + 1));

            Process p = pb.start();
            processes.add(p);
            logger.info("Started consumer process {} with pid={}", i + 1, p.pid());
        }

        int exitCode = 0;
        for (int i = 0; i < processes.size(); i++) {
            int code = processes.get(i).waitFor();
            logger.warn("Consumer process {} exited with code {}", i + 1, code);
            if (code != 0) {
                exitCode = code;
            }
        }

        System.exit(exitCode);
    }
}
