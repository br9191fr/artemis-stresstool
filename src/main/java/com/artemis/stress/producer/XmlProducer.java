package com.artemis.stress.producer;

import com.artemis.stress.config.StressConfig;
import com.artemis.stress.metrics.MetricsCollector;
import com.artemis.stress.xml.XmlPayloadGenerator;
import com.artemis.stress.xml.XmlSchemaValidator;
import com.artemis.stress.xml.XmlSchemaValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A single producer thread.
 *
 * <p>Each producer:
 * <ol>
 *   <li>Borrows a {@link Connection} from the pool.</li>
 *   <li>Creates its own transacted or auto-ack {@link Session}.</li>
 *   <li>Generates a schema-conformant XML document with a unique
 *       {@code <NumericId>} via {@link XmlPayloadGenerator}.</li>
 *   <li>Optionally validates the document against the XSD before sending.</li>
 *   <li>Sends the XML as a JMS {@link TextMessage}.</li>
 *   <li>Respects an optional per-thread rate limit (token-bucket).</li>
 *   <li>Commits every N messages when {@code batchSize > 0}.</li>
 *   <li>Records latency, throughput and errors in the shared
 *       {@link MetricsCollector}.</li>
 * </ol>
 * </p>
 */
public class XmlProducer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(XmlProducer.class);

    private final int id;
    private final StressConfig config;
    private final ConnectionPool pool;
    private final XmlPayloadGenerator xmlGen;
    /** May be null when schema validation is disabled. */
    private final XmlSchemaValidator validator;
    private final MetricsCollector metrics;
    private final AtomicLong globalCounter;
    private final AtomicBoolean stopFlag;

    // Rate limiter state (token bucket)
    private long tokenBucketTokens;
    private long tokenBucketLastRefill;
    private static final long TOKEN_BUCKET_REFILL_NS = 1_000_000; // refill every 1 ms

    public XmlProducer(int id,
                       StressConfig config,
                       ConnectionPool pool,
                       XmlPayloadGenerator xmlGen,
                       XmlSchemaValidator validator,
                       MetricsCollector metrics,
                       AtomicLong globalCounter,
                       AtomicBoolean stopFlag) {
        this.id            = id;
        this.config        = config;
        this.pool          = pool;
        this.xmlGen        = xmlGen;
        this.validator     = validator;
        this.metrics       = metrics;
        this.globalCounter = globalCounter;
        this.stopFlag      = stopFlag;

        // Token bucket: tokens = messages allowed per refill period
        this.tokenBucketTokens = config.getRatePerThread();
        this.tokenBucketLastRefill = System.nanoTime();
    }

    @Override
    public void run() {
        log.debug("Producer #{} starting", id);

        Connection connection = pool.borrow();
        boolean transacted = config.getBatchSize() > 0;
        int batchCount = 0;

        try (Session session = connection.createSession(
                transacted,
                transacted ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE)) {

            Destination destination = config.isUseTopic()
                    ? session.createTopic(config.getQueue())
                    : session.createQueue(config.getQueue());

            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(config.isPersistent()
                    ? DeliveryMode.PERSISTENT
                    : DeliveryMode.NON_PERSISTENT);
            producer.setDisableMessageID(false);   // keep for dedup / tracing
            producer.setDisableMessageTimestamp(false);

            log.info("Producer #{} ready — destination={} transacted={} rate={} msg/s",
                    id, config.getQueue(), transacted,
                    config.getRatePerThread() == 0 ? "max" : config.getRatePerThread());

            while (!stopFlag.get() && !shouldStop()) {
                // Rate limiting
                if (config.getRatePerThread() > 0) {
                    throttle();
                }

                // Generate payload
                long seq = globalCounter.incrementAndGet();
                if (!shouldSend(seq)) {
                    globalCounter.decrementAndGet(); // give back — another thread will take it
                    break;
                }

                String xml = xmlGen.generate(seq, id);

                // ── Schema validation (optional) ─────────────────────────────
                if (validator != null) {
                    try {
                        validator.validate(xml);
                    } catch (XmlSchemaValidator.XmlValidationException ve) {
                        // Validation failure: count as error, skip send, log first 5
                        metrics.recordError();
                        if (metrics.getErrorCount() <= 5) {
                            log.error("Producer #{} validation error at seq={}: {}",
                                    id, seq, ve.getMessage());
                        }
                        continue;
                    }
                }

                TextMessage msg = session.createTextMessage(xml);
                msg.setLongProperty("seq", seq);
                msg.setIntProperty("producerId", id);
                msg.setStringProperty("contentType", "application/xml");

                long sendStart = System.nanoTime();
                try {
                    producer.send(msg);
                    long latencyNs = System.nanoTime() - sendStart;
                    metrics.recordSuccess(latencyNs, xml.length());

                    batchCount++;
                    if (transacted && batchCount >= config.getBatchSize()) {
                        session.commit();
                        batchCount = 0;
                    }
                } catch (JMSException e) {
                    metrics.recordError();
                    log.warn("Producer #{} send error at seq={}: {}", id, seq, e.getMessage());
                    // Back off on error
                    Thread.sleep(50);
                    if (transacted) {
                        try { session.rollback(); } catch (JMSException ignored) {}
                        batchCount = 0;
                    }
                }
            }

            // Commit remaining batch
            if (transacted && batchCount > 0) {
                try {
                    session.commit();
                } catch (JMSException e) {
                    log.warn("Producer #{} final commit failed: {}", id, e.getMessage());
                }
            }

            producer.close();
            log.debug("Producer #{} finished", id);

        } catch (JMSException | InterruptedException e) {
            log.error("Producer #{} fatal error: {}", id, e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    /** Checks the global counter against the configured total. */
    private boolean shouldStop() {
        long total = config.getTotalMessages();
        return total > 0 && globalCounter.get() >= total;
    }

    /**
     * After incrementing the global counter, verify we haven't overshot.
     * (Race condition: multiple threads can increment past the target.)
     */
    private boolean shouldSend(long seq) {
        long total = config.getTotalMessages();
        return total == 0 || seq <= total;
    }

    /**
     * Simple token-bucket rate limiter.
     * Refills {@code ratePerThread} tokens every millisecond and sleeps
     * when the bucket is empty.
     */
    private void throttle() {
        long now = System.nanoTime();
        long elapsed = now - tokenBucketLastRefill;
        if (elapsed >= TOKEN_BUCKET_REFILL_NS) {
            long periods = elapsed / TOKEN_BUCKET_REFILL_NS;
            tokenBucketTokens = Math.min(
                    config.getRatePerThread(),
                    tokenBucketTokens + (long)(config.getRatePerThread() * periods / 1000.0));
            tokenBucketLastRefill = now;
        }

        if (tokenBucketTokens <= 0) {
            // Sleep until next refill window
            long sleepNs = TOKEN_BUCKET_REFILL_NS - (System.nanoTime() - tokenBucketLastRefill);
            if (sleepNs > 0) {
                try {
                    Thread.sleep(sleepNs / 1_000_000, (int)(sleepNs % 1_000_000));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        } else {
            tokenBucketTokens--;
        }
    }
}
