package com.artemis.stress.producer;

import com.artemis.stress.config.StressConfig;
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
 * Orchestrates the lifecycle of all producer threads.
 *
 * <p>Shared objects created here and passed down to every thread:
 * <ul>
 *   <li>{@link MessageIdGenerator} — single AtomicLong, guarantees unique IDs.</li>
 *   <li>{@link XmlSchemaValidator} — Schema compiled once, Validator per call.</li>
 *   <li>{@link XmlPayloadGenerator} — DOM factory, thread-locals inside.</li>
 *   <li>{@link ConnectionPool} — round-robin JMS connections.</li>
 * </ul>
 * </p>
 */
public class ProducerOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(ProducerOrchestrator.class);

    private final StressConfig config;
    private final MetricsCollector metrics;

    public ProducerOrchestrator(StressConfig config, MetricsCollector metrics) {
        this.config  = config;
        this.metrics = metrics;
    }

    public void run() throws InterruptedException {
        AtomicLong   globalCounter = new AtomicLong(0);
        AtomicBoolean stopFlag     = new AtomicBoolean(false);

        // ── Unique numeric-ID generator (shared across ALL threads) ───────────
        MessageIdGenerator idGenerator = new MessageIdGenerator(config.getIdStartValue());

        // ── Schema validator (shared; Schema is thread-safe after construction) ─
        XmlSchemaValidator validator = null;
        if (config.isSchemaValidation()) {
            validator = new XmlSchemaValidator(config.getSchemaPath());
            log.info("Schema validation ENABLED — source={}",
                    validator.getSchemaSource());
        } else {
            log.info("Schema validation DISABLED");
        }

        // ── XML payload generator ─────────────────────────────────────────────
        XmlPayloadGenerator xmlGen;
        try {
            xmlGen = new XmlPayloadGenerator(config, idGenerator);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialise XmlPayloadGenerator", e);
        }
        // TODO BRI
        String xml = xmlGen.generate(121, 333);
        log.info("Generated XML payload {} ", xml);
        // ── Connection pool ───────────────────────────────────────────────────
        log.info("Opening connection pool ({} connection(s))...", config.getConnectionPoolSize());
        ConnectionPool pool;
        try {
            pool = new ConnectionPool(config);
        } catch (Exception e) {
            log.error("Cannot connect to broker: {}", e.getMessage());
            throw new RuntimeException(e);
        }

        // ── Duration timer ────────────────────────────────────────────────────
        ScheduledExecutorService durationTimer = null;
        if (config.getDurationSeconds() > 0) {
            durationTimer = Executors.newSingleThreadScheduledExecutor(
                    r -> { Thread t = new Thread(r, "duration-timer"); t.setDaemon(true); return t; });
            durationTimer.schedule(
                    () -> {
                        log.info("Duration limit ({}s) reached — signalling stop",
                                config.getDurationSeconds());
                        stopFlag.set(true);
                    },
                    config.getDurationSeconds(), TimeUnit.SECONDS);
        }

        // ── Launch producer threads ───────────────────────────────────────────
        ExecutorService executor = Executors.newFixedThreadPool(
                config.getThreads(),
                new ProducerThreadFactory());

        List<Future<?>> futures = new ArrayList<>(config.getThreads());
        for (int i = 0; i < config.getThreads(); i++) {
            XmlProducer producer = new XmlProducer(
                    i, config, pool, xmlGen, validator, metrics, globalCounter, stopFlag);
            futures.add(executor.submit(producer));
        }

        log.info("All {} producer thread(s) started", config.getThreads());

        executor.shutdown();
        boolean finished = executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        if (!finished) {
            log.warn("Executor timed out — forcing shutdown");
            executor.shutdownNow();
        }

        if (durationTimer != null) durationTimer.shutdownNow();
        pool.close();

        // Report validation summary
        if (validator != null) {
            log.info("Schema validation summary — passed={} failed={}",
                    validator.getPassCount(), validator.getFailCount());
        }
        log.info("ID generator summary — last issued id={}",
                idGenerator.lastIssued());

        int errorCount = 0;
        for (Future<?> f : futures) {
            try { f.get(); }
            catch (ExecutionException e) {
                log.error("Producer thread failed: {}", e.getCause().getMessage(), e.getCause());
                errorCount++;
            }
        }

        log.info("All producers finished. Total sent={}, errors={}",
                globalCounter.get(), errorCount);
    }

    // ─── Thread factory ───────────────────────────────────────────────────────

    private static class ProducerThreadFactory implements java.util.concurrent.ThreadFactory {
        private final AtomicLong idx = new AtomicLong(0);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "producer-" + idx.getAndIncrement());
            t.setDaemon(false);
            return t;
        }
    }
}
