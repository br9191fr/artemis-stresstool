package com.artemis.stress.consumer;

import com.artemis.stress.config.StressConfig;
import com.artemis.stress.metrics.MetricsCollector;
import com.artemis.stress.producer.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Launches and manages consumer threads.
 * In BOTH mode the stopFlag is shared with the producer side so consumers
 * stop automatically when all producers finish.
 */
public class ConsumerOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(ConsumerOrchestrator.class);

    private final StressConfig     config;
    private final ConnectionPool   pool;
    private final MetricsCollector metrics;

    public ConsumerOrchestrator(StressConfig config,
                                ConnectionPool pool,
                                MetricsCollector metrics) {
        this.config  = config;
        this.pool    = pool;
        this.metrics = metrics;
    }

    public void run(AtomicBoolean stopFlag) throws InterruptedException {
        AtomicLong globalReceived = new AtomicLong(0);
        AtomicLong threadIdx      = new AtomicLong(0);

        ExecutorService exec = Executors.newFixedThreadPool(
                config.getConsumerThreads(),
                r -> new Thread(r, "consumer-" + threadIdx.getAndIncrement()));

        List<Future<?>> futures = new ArrayList<>(config.getConsumerThreads());
        for (int i = 0; i < config.getConsumerThreads(); i++) {
            futures.add(exec.submit(
                    new XmlConsumer(i, config, pool, metrics, globalReceived, stopFlag)));
        }

        log.info("All {} consumer thread(s) started", config.getConsumerThreads());
        exec.shutdown();
        exec.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

        int errors = 0;
        for (Future<?> f : futures) {
            try { f.get(); }
            catch (ExecutionException e) {
                log.error("Consumer thread failed: {}", e.getCause().getMessage(), e.getCause());
                errors++;
            }
        }
        log.info("All consumers finished. total received={} thread-errors={}", globalReceived.get(), errors);
    }
}
