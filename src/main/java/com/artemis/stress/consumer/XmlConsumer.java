package com.artemis.stress.consumer;

import com.artemis.stress.config.StressConfig;
import com.artemis.stress.metrics.MetricsCollector;
import com.artemis.stress.producer.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A single consumer thread.
 *
 * Duplicate detection uses the {@code messageUuid} string property stamped by
 * the producer. A UUID is unique per message regardless of delivery order, so
 * it is the correct dedup key when multiple producer threads send concurrently
 * (which causes out-of-order numericId values — that is normal and expected).
 *
 * A plain LinkedHashMap (insertion-order) acts as a bounded LRU cache: once it
 * exceeds UUID_CACHE_SIZE entries the oldest is evicted manually. No anonymous
 * subclasses or removeEldestEntry overrides are used.
 */
public class XmlConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(XmlConsumer.class);

    /** Per-message unique UUID — primary duplicate detection key. */
    public static final String PROP_MESSAGE_UUID = "messageUuid";

    /** Send timestamp in nanoseconds (System.nanoTime()) for E2E latency. */
    public static final String PROP_SEND_TS      = "sendTimestampNs";

    /** Monotonic document ID (informational only, not used for dedup). */
    public static final String PROP_NUMERIC_ID   = "numericId";

    private static final int UUID_CACHE_SIZE = 100_000;

    private final int              id;
    private final StressConfig     config;
    private final ConnectionPool   pool;
    private final MetricsCollector metrics;
    private final AtomicLong       globalReceived;
    private final AtomicBoolean    stopFlag;

    // Plain LinkedHashMap — insertion order, manual size cap, synchronized via method
    private final LinkedHashMap<String, Boolean> uuidCache =
            new LinkedHashMap<>(UUID_CACHE_SIZE, 0.75f, false);

    public XmlConsumer(int id,
                       StressConfig config,
                       ConnectionPool pool,
                       MetricsCollector metrics,
                       AtomicLong globalReceived,
                       AtomicBoolean stopFlag) {
        this.id             = id;
        this.config         = config;
        this.pool           = pool;
        this.metrics        = metrics;
        this.globalReceived = globalReceived;
        this.stopFlag       = stopFlag;
    }

    @Override
    public void run() {
        log.info("Consumer #{} starting", id);
        Connection connection = pool.borrow();

        try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            Destination destination = config.isUseTopic()
                    ? session.createTopic(config.getQueue())
                    : session.createQueue(config.getQueue());

            MessageConsumer consumer = session.createConsumer(destination);
            log.info("Consumer #{} ready — destination={} timeout={}ms",
                    id, config.getQueue(), config.getConsumerReceiveTimeout());

            long totalReceived   = 0;
            long totalDuplicates = 0;

            while (!stopFlag.get() && !shouldStop()) {
                Message msg = consumer.receive(config.getConsumerReceiveTimeout());
                if (msg == null) continue;  // timeout — re-check stop flag

                long receiveNs = System.nanoTime();
                globalReceived.incrementAndGet();
                totalReceived++;

                try {
                    // ── UUID duplicate detection ──────────────────────────────
                    if (msg.propertyExists(PROP_MESSAGE_UUID)) {
                        String uuid = msg.getStringProperty(PROP_MESSAGE_UUID);
                        if (!checkAndRegisterUuid(uuid)) {
                            totalDuplicates++;
                            long numId = msg.propertyExists(PROP_NUMERIC_ID)
                                    ? msg.getLongProperty(PROP_NUMERIC_ID) : -1;
                            log.warn("Consumer #{} DUPLICATE message — uuid={} numericId={}",
                                    id, uuid, numId);
                            metrics.recordE2eError();
                            continue;  // do not record latency for duplicates
                        }
                    } else {
                        log.debug("Consumer #{} message missing {} property — skipping dedup",
                                id, PROP_MESSAGE_UUID);
                    }

                    // ── End-to-end latency ────────────────────────────────────
                    long e2eNs = 0;
                    if (msg.propertyExists(PROP_SEND_TS)) {
                        long sendNs = msg.getLongProperty(PROP_SEND_TS);
                        e2eNs = Math.max(0, receiveNs - sendNs);
                    }

                    metrics.recordE2e(e2eNs);

                } catch (JMSException e) {
                    log.warn("Consumer #{} processing error: {}", id, e.getMessage());
                    metrics.recordE2eError();
                }
            }

            consumer.close();
            log.info("Consumer #{} finished — received={} duplicates={}",
                    id, totalReceived, totalDuplicates);

        } catch (JMSException e) {
            log.error("Consumer #{} fatal error: {}", id, e.getMessage(), e);
        }
    }

    /**
     * Registers a UUID as seen. Returns true if this is the first time this
     * UUID has been seen (not a duplicate), false if it was already present.
     *
     * Evicts the oldest entry when the cache exceeds UUID_CACHE_SIZE to bound
     * memory usage. Synchronized because a single consumer thread calls this,
     * but synchronization is cheap here since there is no contention.
     */
    private synchronized boolean checkAndRegisterUuid(String uuid) {
        if (uuidCache.containsKey(uuid)) {
            return false;  // duplicate
        }
        uuidCache.put(uuid, Boolean.TRUE);
        if (uuidCache.size() > UUID_CACHE_SIZE) {
            // Remove the oldest inserted entry
            uuidCache.remove(uuidCache.keySet().iterator().next());
        }
        return true;
    }

    private boolean shouldStop() {
        long total = config.getTotalMessages();
        return total > 0 && globalReceived.get() >= total;
    }
}
