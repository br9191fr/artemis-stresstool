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
 * Orchestrates producer threads across one or more runs.
 *
 * <p>The effective message limit per run is computed as:
 * <pre>
 *   effectiveLimit = totalMessages × threadCount
 * </pre>
 * This ensures {@code --messages N} means <em>N messages per thread</em>,
 * not N messages shared across all threads.</p>
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
        AtomicBoolean stopFlag   = new AtomicBoolean(false);
        MessageIdGenerator idGen = new MessageIdGenerator(config.getIdStartValue());

        XmlSchemaValidator validator = null;
        if (config.isSchemaValidation()) {
            validator = new XmlSchemaValidator(config.getSchemaPath());
            log.info("Schema validation ENABLED — source={}", validator.getSchemaSource());
        } else {
            log.info("Schema validation DISABLED");
        }

        XmlPayloadGenerator xmlGen;
        try {
            xmlGen = new XmlPayloadGenerator(config, idGen);
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

        // effectiveLimit = messages per thread × number of threads
        // 0 means unlimited (when totalMessages == 0)
        final long effectiveLimit = config.getTotalMessages() == 0
                ? 0
                : config.getTotalMessages() * config.getThreads();

        int totalRuns = Math.max(1, config.getRuns());

        if (effectiveLimit > 0) {
            log.info("{} thread(s) × {} msg/thread = {} msg/run × {} run(s) = {} total messages",
                    config.getThreads(), config.getTotalMessages(),
                    effectiveLimit, totalRuns,
                    effectiveLimit * totalRuns);
        } else {
            log.info("{} thread(s) unlimited × {} run(s)", config.getThreads(), totalRuns);
        }
        if (config.getBurstSize() > 0) {
            log.info("Burst: {} msg then pause {}s",
                    config.getBurstSize(), config.getBurstPauseSeconds());
        }

        // Duration timer (covers total wall time across all runs)
        ScheduledExecutorService durationTimer = null;
        if (config.getDurationSeconds() > 0) {
            durationTimer = Executors.newSingleThreadScheduledExecutor(
                    r -> { Thread t = new Thread(r, "duration-timer"); t.setDaemon(true); return t; });
            durationTimer.schedule(() -> {
                log.info("Duration limit ({}s) reached — stopping", config.getDurationSeconds());
                stopFlag.set(true);
            }, config.getDurationSeconds(), TimeUnit.SECONDS);
        }

        // BOTH mode: start consumers ONCE before the first run
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
            log.info("Consumers started — will run across all {} run(s)", totalRuns);
        }

        // ── Run loop ──────────────────────────────────────────────────────────
        for (int run = 0; run < totalRuns && !stopFlag.get(); run++) {
            long runStart = System.currentTimeMillis();

            // Fresh counter per run — reset to 0 so each run allows effectiveLimit messages
            AtomicLong runCounter = new AtomicLong(0);

            log.info("═══ RUN {}/{} starting (limit={}) ═══",
                    run + 1, totalRuns,
                    effectiveLimit == 0 ? "unlimited" : effectiveLimit + " msg");

            ExecutorService exec = Executors.newFixedThreadPool(
                    config.getThreads(), new ProducerThreadFactory(run));

            List<Future<?>> futures = new ArrayList<>(config.getThreads());
            final XmlSchemaValidator fValidator = validator;
            for (int i = 0; i < config.getThreads(); i++) {
                futures.add(exec.submit(new XmlProducer(
                        i, run, effectiveLimit,
                        config, pool, xmlGen, fValidator,
                        metrics, runCounter, stopFlag)));
            }
            log.info("Run {}/{} — {} producer thread(s) started", run + 1, totalRuns, config.getThreads());

            exec.shutdown();
            exec.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

            long runMs = System.currentTimeMillis() - runStart;
            log.info("═══ RUN {}/{} finished — sent={} elapsed={}ms ═══",
                    run + 1, totalRuns, runCounter.get(), runMs);

            for (Future<?> f : futures) {
                try { f.get(); }
                catch (ExecutionException e) {
                    log.error("Producer thread failed in run {}: {}",
                            run + 1, e.getCause().getMessage());
                }
            }

            // Pause between runs (skip after the last run)
            if (run < totalRuns - 1 && !stopFlag.get()) {
                int pauseSec = config.getRunPauseSeconds();
                if (pauseSec > 0) {
                    log.info("Pausing {}s before run {}/{}...", pauseSec, run + 2, totalRuns);
                    Thread.sleep(pauseSec * 1000L);
                }
            }
        }

        // Signal consumers to stop after all runs complete
        stopFlag.set(true);
        if (consumerThread != null) {
            consumerThread.join((long) config.getConsumerReceiveTimeout() * 2 + 5000);
        }

        if (durationTimer != null) durationTimer.shutdownNow();
        pool.close();

        if (validator != null)
            log.info("Schema validation — passed={} failed={}",
                    validator.getPassCount(), validator.getFailCount());
        log.info("ID generator — last issued id={}", idGen.lastIssued());
        log.info("All {} run(s) completed. Grand total sent={}",
                totalRuns, idGen.lastIssued() - config.getIdStartValue() + 1);
    }

    private static class ProducerThreadFactory implements ThreadFactory {
        private final int run;
        private final AtomicLong idx = new AtomicLong(0);
        ProducerThreadFactory(int run) { this.run = run; }
        @Override public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "producer-r" + run + "-" + idx.getAndIncrement());
            t.setDaemon(false);
            return t;
        }
    }
}
