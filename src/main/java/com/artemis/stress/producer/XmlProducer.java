package com.artemis.stress.producer;

import com.artemis.stress.config.StressConfig;
import com.artemis.stress.consumer.XmlConsumer;
import com.artemis.stress.metrics.MetricsCollector;
import com.artemis.stress.xml.XmlPayloadGenerator;
import com.artemis.stress.xml.XmlSchemaValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A single producer thread. Stamps sendTimestampNs and numericId as JMS
 * properties on every message so the consumer can measure end-to-end latency
 * and detect duplicates.
 */
public class XmlProducer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(XmlProducer.class);

    private final int              id;
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
                       StressConfig config,
                       ConnectionPool pool,
                       XmlPayloadGenerator xmlGen,
                       XmlSchemaValidator validator,
                       MetricsCollector metrics,
                       AtomicLong globalCounter,
                       AtomicBoolean stopFlag) {
        this.id             = id;
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
        int batchCount = 0;

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

            log.info("Producer #{} ready — destination={} transacted={} rate={} msg/s",
                    id, config.getQueue(), transacted,
                    config.getRatePerThread() == 0 ? "max" : config.getRatePerThread());

            while (!stopFlag.get() && !shouldStop()) {
                if (config.getRatePerThread() > 0) throttle();

                long seq = globalCounter.incrementAndGet();
                if (!shouldSend(seq)) { globalCounter.decrementAndGet(); break; }

                String xml = xmlGen.generate(seq, id);

                if (validator != null) {
                    try {
                        validator.validate(xml);
                    } catch (XmlSchemaValidator.XmlValidationException ve) {
                        metrics.recordSendError();
                        if (metrics.getSendErrors() <= 5)
                            log.error("Producer #{} validation error seq={}: {}", id, seq, ve.getMessage());
                        continue;
                    }
                }

                TextMessage msg = session.createTextMessage(xml);

                // Stamp send time (nanoTime) and numericId for consumer-side measurement
                long sendTs = System.nanoTime();
                msg.setLongProperty(XmlConsumer.PROP_SEND_TS,    sendTs);
                msg.setLongProperty(XmlConsumer.PROP_NUMERIC_ID, seq);
                msg.setLongProperty("seq", seq);
                msg.setIntProperty("producerId", id);
                msg.setStringProperty("contentType", "application/xml");

                try {
                    producer.send(msg);
                    metrics.recordSend(System.nanoTime() - sendTs, xml.length());
                    batchCount++;
                    if (transacted && batchCount >= config.getBatchSize()) {
                        session.commit();
                        batchCount = 0;
                    }
                } catch (JMSException e) {
                    metrics.recordSendError();
                    log.warn("Producer #{} send error seq={}: {}", id, seq, e.getMessage());
                    Thread.sleep(50);
                    if (transacted) {
                        try { session.rollback(); } catch (JMSException ignored) {}
                        batchCount = 0;
                    }
                }
            }

            if (transacted && batchCount > 0) {
                try { session.commit(); }
                catch (JMSException e) { log.warn("Producer #{} final commit failed: {}", id, e.getMessage()); }
            }

            producer.close();
            log.debug("Producer #{} finished", id);

        } catch (JMSException | InterruptedException e) {
            log.error("Producer #{} fatal error: {}", id, e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }

    private boolean shouldStop() {
        long total = config.getTotalMessages();
        return total > 0 && globalCounter.get() >= total;
    }

    private boolean shouldSend(long seq) {
        long total = config.getTotalMessages();
        return total == 0 || seq <= total;
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
