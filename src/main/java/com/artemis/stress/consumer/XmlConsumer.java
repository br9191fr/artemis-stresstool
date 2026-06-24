package com.artemis.stress.consumer;

import com.artemis.stress.config.StressConfig;
import com.artemis.stress.metrics.MetricsCollector;
import com.artemis.stress.producer.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A single consumer thread.
 *
 * Measures end-to-end latency using the {@code sendTimestampNs} property
 * stamped by XmlProducer immediately before each send call.
 * Also performs duplicate/out-of-order detection via the {@code numericId} property.
 */
public class XmlConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(XmlConsumer.class);

    /** JMS property stamped by the producer just before send — used for E2E latency. */
    public static final String PROP_SEND_TS   = "sendTimestampNs";
    /** JMS property carrying the unique numeric document ID. */
    public static final String PROP_NUMERIC_ID = "numericId";

    private final int            id;
    private final StressConfig   config;
    private final ConnectionPool pool;
    private final MetricsCollector metrics;
    private final AtomicLong     globalReceived;
    private final AtomicBoolean  stopFlag;

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
        long lastNumericId = 0;

        try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            Destination destination = config.isUseTopic()
                    ? session.createTopic(config.getQueue())
                    : session.createQueue(config.getQueue());

            MessageConsumer consumer = session.createConsumer(destination);
            log.info("Consumer #{} ready — destination={} timeout={}ms",
                    id, config.getQueue(), config.getConsumerReceiveTimeout());

            while (!stopFlag.get() && !shouldStop()) {
                Message msg = consumer.receive(config.getConsumerReceiveTimeout());
                if (msg == null) continue;  // timeout, re-check stop flag

                long receiveNs = System.nanoTime();
                globalReceived.incrementAndGet();

                try {
                    // End-to-end latency
                    long e2eNs = 0;
                    if (msg.propertyExists(PROP_SEND_TS)) {
                        long sendNs = msg.getLongProperty(PROP_SEND_TS);
                        e2eNs = Math.max(0, receiveNs - sendNs);
                    }

                    // Duplicate / out-of-order check
                    if (msg.propertyExists(PROP_NUMERIC_ID)) {
                        long numId = msg.getLongProperty(PROP_NUMERIC_ID);
                        if (numId <= lastNumericId) {
                            log.warn("Consumer #{} out-of-order/duplicate NumericId={} (last={})",
                                    id, numId, lastNumericId);
                            metrics.recordE2eError();
                        } else {
                            lastNumericId = numId;
                        }
                    }

                    metrics.recordE2e(e2eNs);

                } catch (JMSException e) {
                    log.warn("Consumer #{} processing error: {}", id, e.getMessage());
                    metrics.recordE2eError();
                }
            }

            consumer.close();
            log.info("Consumer #{} finished — received={} lastNumericId={}",
                    id, globalReceived.get(), lastNumericId);

        } catch (JMSException e) {
            log.error("Consumer #{} fatal error: {}", id, e.getMessage(), e);
        }
    }

    private boolean shouldStop() {
        long total = config.getTotalMessages();
        return total > 0 && globalReceived.get() >= total;
    }
}
