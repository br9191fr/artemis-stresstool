package com.artemis.stress.producer;

import com.artemis.stress.config.StressConfig;
import com.artemis.stress.consumer.XmlConsumer;
import com.artemis.stress.metrics.MetricsCollector;
import com.artemis.stress.xml.XmlPayloadGenerator;
import com.artemis.stress.xml.XmlSchemaValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A single producer thread.
 *
 * <p>{@code effectiveLimit} is the global message ceiling for this run,
 * computed by the orchestrator as {@code totalMessages × threadCount}.
 * This ensures that {@code --messages N} means N messages <em>per thread</em>,
 * not N messages shared across all threads.</p>
 */
public class XmlProducer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(XmlProducer.class);

    private final int              id;
    private final int              runIndex;
    private final long             effectiveLimit;   // totalMessages * threads
    private final StressConfig     config;
    private final ConnectionPool   pool;
    private final XmlPayloadGenerator xmlGen;
    private final XmlSchemaValidator  validator;
    private final MetricsCollector metrics;
    private final AtomicLong       globalCounter;
    private final AtomicBoolean    stopFlag;

    private long tokenBucketTokens;
    private long tokenBucketLastRefill;
    private static final long TOKEN_BUCKET_REFILL_NS = 1_000_000;

    public XmlProducer(int id,
                       int runIndex,
                       long effectiveLimit,
                       StressConfig config,
                       ConnectionPool pool,
                       XmlPayloadGenerator xmlGen,
                       XmlSchemaValidator validator,
                       MetricsCollector metrics,
                       AtomicLong globalCounter,
                       AtomicBoolean stopFlag) {
        this.id             = id;
        this.runIndex       = runIndex;
        this.effectiveLimit = effectiveLimit;
        this.config         = config;
        this.pool           = pool;
        this.xmlGen         = xmlGen;
        this.validator      = validator;
        this.metrics        = metrics;
        this.globalCounter  = globalCounter;
        this.stopFlag       = stopFlag;
        this.tokenBucketTokens     = config.getRatePerThread();
        this.tokenBucketLastRefill = System.nanoTime();
    }

    @Override
    public void run() {
        Connection connection = pool.borrow();
        boolean transacted = config.getBatchSize() > 0;
        int batchCount     = 0;

        try (Session session = connection.createSession(
                transacted,
                transacted ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE)) {

            Destination dest = config.isUseTopic()
                    ? session.createTopic(config.getQueue())
                    : session.createQueue(config.getQueue());

            MessageProducer producer = session.createProducer(dest);
            producer.setDeliveryMode(config.isPersistent()
                    ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
            producer.setDisableMessageID(false);
            producer.setDisableMessageTimestamp(false);

            boolean burstMode = config.getBurstSize() > 0;
            log.info("Producer #{} run={} ready — destination={} burst={} pause={}s limit={}",
                    id, runIndex, config.getQueue(),
                    burstMode ? config.getBurstSize() + " msg" : "off",
                    burstMode ? config.getBurstPauseSeconds() : 0,
                    effectiveLimit == 0 ? "unlimited" : effectiveLimit);

            int burstCount = 0;

            while (!stopFlag.get() && !shouldStop()) {
                if (config.getRatePerThread() > 0) throttle();

                // ── Burst pause ───────────────────────────────────────────────
                if (burstMode && burstCount >= config.getBurstSize()) {
                    int pauseSec = config.getBurstPauseSeconds();
                    if (pauseSec > 0) {
                        log.debug("Producer #{} run={} burst complete ({} msg) — pausing {}s",
                                id, runIndex, config.getBurstSize(), pauseSec);
                        Thread.sleep(pauseSec * 1000L);
                    }
                    burstCount = 0;
                    if (stopFlag.get() || shouldStop()) break;
                }

                long seq = globalCounter.incrementAndGet();
                if (!shouldSend(seq)) { globalCounter.decrementAndGet(); break; }

                String xml = xmlGen.generate(seq, id);

                if (validator != null) {
                    try {
                        validator.validate(xml);
                    } catch (XmlSchemaValidator.XmlValidationException ve) {
                        metrics.recordSendError();
                        if (metrics.getSendErrors() <= 5)
                            log.error("Producer #{} validation error seq={}: {}",
                                    id, seq, ve.getMessage());
                        continue;
                    }
                }

                TextMessage msg = session.createTextMessage(xml);
                String msgUuid = UUID.randomUUID().toString();
                msg.setStringProperty(XmlConsumer.PROP_MESSAGE_UUID, msgUuid);
                long sendTs = System.nanoTime();
                msg.setLongProperty(XmlConsumer.PROP_SEND_TS,    sendTs);
                msg.setLongProperty(XmlConsumer.PROP_NUMERIC_ID, seq);
                msg.setIntProperty("producerId", id);
                msg.setIntProperty("runIndex",   runIndex);
                msg.setStringProperty("contentType", "application/xml");

                try {
                    producer.send(msg);
                    metrics.recordSend(System.nanoTime() - sendTs, xml.length());
                    batchCount++;
                    burstCount++;

                    if (transacted && batchCount >= config.getBatchSize()) {
                        session.commit();
                        batchCount = 0;
                    }
                } catch (JMSException e) {
                    metrics.recordSendError();
                    log.warn("Producer #{} run={} send error seq={}: {}",
                            id, runIndex, seq, e.getMessage());
                    Thread.sleep(50);
                    if (transacted) {
                        try { session.rollback(); } catch (JMSException ignored) {}
                        batchCount = 0;
                    }
                }
            }

            if (transacted && batchCount > 0) {
                try { session.commit(); }
                catch (JMSException e) {
                    log.warn("Producer #{} final commit failed: {}", id, e.getMessage());
                }
            }

            producer.close();
            log.debug("Producer #{} run={} finished", id, runIndex);

        } catch (JMSException | InterruptedException e) {
            log.error("Producer #{} run={} fatal error: {}", id, runIndex, e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Stop when the shared counter has reached the effective limit
     * (totalMessages × threadCount). Each thread contributes ~totalMessages
     * messages before the limit is hit.
     */
    private boolean shouldStop() {
        return effectiveLimit > 0 && globalCounter.get() >= effectiveLimit;
    }

    private boolean shouldSend(long seq) {
        return effectiveLimit == 0 || seq <= effectiveLimit;
    }

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
            long sleepNs = TOKEN_BUCKET_REFILL_NS - (System.nanoTime() - tokenBucketLastRefill);
            if (sleepNs > 0) {
                try { Thread.sleep(sleepNs / 1_000_000, (int)(sleepNs % 1_000_000)); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        } else {
            tokenBucketTokens--;
        }
    }
}
