package com.artemis.stress.producer;

import com.artemis.stress.config.StressConfig;
import com.artemis.stress.consumer.ConsumerOrchestrator;
import com.artemis.stress.metrics.MetricsCollector;
import com.artemis.stress.xml.MessageIdGenerator;
import com.artemis.stress.xml.XmlPayloadGenerator;
import com.artemis.stress.xml.XmlSchemaValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Orchestrates producer threads and, in BOTH mode, starts consumers
 * concurrently then signals them to stop after all producers finish.
 */
public class ProducerOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(ProducerOrchestrator.class);

    private final StressConfig     config;
    private final MetricsCollector metrics;

    public ProducerOrchestrator(StressConfig config, MetricsCollector metrics) {
        this.config  = config;
        this.metrics = metrics;
    }

    public void run() throws InterruptedException {
        AtomicLong    globalCounter = new AtomicLong(0);
        AtomicBoolean stopFlag      = new AtomicBoolean(false);

        MessageIdGenerator idGenerator = new MessageIdGenerator(config.getIdStartValue());

        XmlSchemaValidator validator = null;
        if (config.isSchemaValidation()) {
            validator = new XmlSchemaValidator(config.getSchemaPath());
            log.info("Schema validation ENABLED — source={}", validator.getSchemaSource());
        } else {
            log.info("Schema validation DISABLED");
        }

        XmlPayloadGenerator xmlGen;
        try {
            xmlGen = new XmlPayloadGenerator(config, idGenerator);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialise XmlPayloadGenerator", e);
        }

        // Preview
        try {
            String preview = xmlGen.generate(121L, 333);
            log.info("Generated XML payload {}", preview);
            log.info("XML payload size: {} bytes", preview.getBytes().length);
        } catch (Exception e) {
            log.warn("Could not generate preview: {}", e.getMessage());
        }

        log.info("Opening connection pool ({} connection(s))...", config.getConnectionPoolSize());
        ConnectionPool pool;
        try {
            pool = new ConnectionPool(config);
        } catch (Exception e) {
            log.error("Cannot connect to broker: {}", e.getMessage());
            throw new RuntimeException(e);
        }

        // Duration timer
        ScheduledExecutorService durationTimer = null;
        if (config.getDurationSeconds() > 0) {
            durationTimer = Executors.newSingleThreadScheduledExecutor(
                    r -> { Thread t = new Thread(r, "duration-timer"); t.setDaemon(true); return t; });
            durationTimer.schedule(() -> {
                log.info("Duration limit ({}s) reached — stopping", config.getDurationSeconds());
                stopFlag.set(true);
            }, config.getDurationSeconds(), TimeUnit.SECONDS);
        }

        // BOTH mode: start consumers BEFORE producers so no messages are missed
        Thread consumerThread = null;
        if (config.getMode() == StressConfig.Mode.BOTH) {
            ConsumerOrchestrator co = new ConsumerOrchestrator(config, pool, metrics);
            final AtomicBoolean sf = stopFlag;
            consumerThread = new Thread(() -> {
                try { co.run(sf); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }, "consumer-orchestrator");
            consumerThread.setDaemon(false);
            consumerThread.start();
            log.info("Consumers started — producers starting now");
        }

        // Launch producers
        ExecutorService exec = Executors.newFixedThreadPool(
                config.getThreads(), new ProducerThreadFactory());

        List<Future<?>> futures = new ArrayList<>(config.getThreads());
        for (int i = 0; i < config.getThreads(); i++) {
            futures.add(exec.submit(new XmlProducer(
                    i, config, pool, xmlGen, validator, metrics, globalCounter, stopFlag)));
        }
        log.info("All {} producer thread(s) started", config.getThreads());

        exec.shutdown();
        exec.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

        // Signal consumers and wait for them to drain
        stopFlag.set(true);
        if (consumerThread != null) {
            consumerThread.join((long) config.getConsumerReceiveTimeout() * 2 + 5000);
        }

        if (durationTimer != null) durationTimer.shutdownNow();
        pool.close();

        if (validator != null)
            log.info("Schema validation — passed={} failed={}",
                    validator.getPassCount(), validator.getFailCount());
        log.info("ID generator — last issued id={}", idGenerator.lastIssued());

        int errorCount = 0;
        for (Future<?> f : futures) {
            try { f.get(); }
            catch (ExecutionException e) {
                log.error("Producer thread failed: {}", e.getCause().getMessage(), e.getCause());
                errorCount++;
            }
        }
        log.info("All producers finished. Total sent={}, errors={}", globalCounter.get(), errorCount);
    }

    private static class ProducerThreadFactory implements ThreadFactory {
        private final AtomicLong idx = new AtomicLong(0);
        @Override public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "producer-" + idx.getAndIncrement());
            t.setDaemon(false);
            return t;
        }
    }
}
