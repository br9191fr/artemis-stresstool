package com.artemis.stress.producer;

import com.artemis.stress.config.StressConfig;
import com.artemis.stress.ssl.SslContextFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages a pool of JMS {@link Connection} objects.
 *
 * <p>Thread-safety: connections are pre-created at startup. Individual
 * {@link jakarta.jms.Session} objects are NOT pooled here — each producer
 * thread creates its own session from the shared connection.</p>
 */
public class ConnectionPool implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConnectionPool.class);

    private final List<Connection> connections;
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);

    public ConnectionPool(StressConfig config) {
        // Apply SSL system properties before creating the factory
        SslContextFactory.configureSystemSslProperties(config);

        connections = new ArrayList<>(config.getConnectionPoolSize());

        ActiveMQConnectionFactory factory = buildFactory(config);

        for (int i = 0; i < config.getConnectionPoolSize(); i++) {
            try {
                Connection conn = factory.createConnection();
                conn.start();
                connections.add(conn);
                log.info("JMS connection #{} established to {}", i + 1, config.getBrokerUrl());
            } catch (JMSException e) {
                closeAll(); // clean up already-opened connections
                throw new RuntimeException(
                        "Failed to create JMS connection #" + (i + 1) + " to " + config.getBrokerUrl(), e);
            }
        }

        log.info("Connection pool ready: {} connection(s)", connections.size());
    }

    /**
     * Returns the next connection in round-robin order.
     */
    public Connection borrow() {
        int idx = Math.abs(roundRobinIndex.getAndIncrement() % connections.size());
        return connections.get(idx);
    }

    @Override
    public void close() {
        closeAll();
    }

    private void closeAll() {
        for (Connection c : connections) {
            try {
                c.close();
            } catch (JMSException e) {
                log.warn("Error closing JMS connection: {}", e.getMessage());
            }
        }
        connections.clear();
        log.info("Connection pool closed");
    }

    // ─── Factory builder ─────────────────────────────────────────────────────

    private ActiveMQConnectionFactory buildFactory(StressConfig config) {
        try {
            String url = buildUrl(config);
            log.debug("Creating ActiveMQConnectionFactory with URL: {}", url);
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url);

            // Tune internal transport parameters for high throughput
            factory.setConfirmationWindowSize(1024 * 1024);       // 1 MB confirmation window
            factory.setProducerWindowSize(10 * 1024 * 1024);      // 10 MB producer flow-control
            factory.setCallTimeout(30_000);                        // 30 s call timeout
            factory.setConnectionTTL(60_000);                      // 60 s keep-alive
            factory.setRetryInterval(1_000);
            factory.setReconnectAttempts(3);

            return factory;
        } catch (Exception e) {
            throw new RuntimeException("Failed to build ActiveMQConnectionFactory", e);
        }
    }

    /**
     * Appends SSL query parameters to the broker URL when keystore is configured.
     *
     * <p>Artemis URL format:
     * {@code ssl://host:port?sslEnabled=true&keyStorePath=...&keyStorePassword=...}
     * </p>
     */
    private String buildUrl(StressConfig config) {
        String base = config.getBrokerUrl();

        // If the user provided a plain tcp:// URL with SSL config, upgrade it
        if (base.startsWith("tcp://") && config.getKeystorePath() != null) {
            base = "ssl://" + base.substring("tcp://".length());
        }

        if (config.getKeystorePath() == null) {
            // Non-SSL — return as-is
            return base;
        }

        // Append SSL parameters (avoid duplicates if user already embedded them)
        StringBuilder sb = new StringBuilder(base);
        char sep = base.contains("?") ? '&' : '?';

        sb.append(sep).append("sslEnabled=true");
        sb.append("&keyStorePath=").append(encode(config.getKeystorePath()));
        sb.append("&keyStorePassword=").append(encode(config.getKeystorePassword()));
        sb.append("&keyStoreType=").append(config.getKeystoreType());
        sb.append("&trustStorePath=").append(encode(config.getTruststorePath()));
        sb.append("&trustStorePassword=").append(encode(config.getTruststorePassword()));
        sb.append("&trustStoreType=").append(config.getKeystoreType());
        sb.append("&enabledProtocols=").append(config.getTlsVersion());
        sb.append("&needClientAuth=true");   // enforce mutual TLS on client side param

        return sb.toString();
    }

    private String encode(String value) {
        if (value == null) return "";
        // Basic URI encoding for passwords / paths with special chars
        return value.replace(" ", "%20").replace("&", "%26");
    }

    public List<Connection> getConnections() {
        return Collections.unmodifiableList(connections);
    }
}
