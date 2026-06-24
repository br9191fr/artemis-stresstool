package com.artemis.stress.producer;

import com.artemis.stress.config.StressConfig;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages a pool of shared JMS connections.
 *
 * Credential priority:
 *   1. Explicit --user / --password CLI flags
 *   2. user= / password= embedded in broker URL query string
 *   3. null → X.509 certificate auth or anonymous
 */
public class ConnectionPool implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConnectionPool.class);

    private final List<Connection> connections;
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);

    public ConnectionPool(StressConfig config) {
        connections = new ArrayList<>(config.getConnectionPoolSize());

        String user     = resolveCredential(config.getUser(),     "user",     config.getBrokerUrl());
        String password = resolveCredential(config.getPassword(), "password", config.getBrokerUrl());

        ConnectionFactory factory = buildFactory(config);

        for (int i = 0; i < config.getConnectionPoolSize(); i++) {
            try {
                Connection conn = (user != null)
                        ? factory.createConnection(user, password)
                        : factory.createConnection();
                conn.start();
                connections.add(conn);
                log.info("JMS connection #{} established", i + 1);
            } catch (JMSException e) {
                closeAll();
                throw new RuntimeException(
                        "Failed to create JMS connection #" + (i + 1)
                        + " to " + config.getBrokerUrl(), e);
            }
        }
        log.info("Connection pool ready: {} connection(s)", connections.size());
    }

    public Connection borrow() {
        int idx = Math.abs(roundRobinIndex.getAndIncrement() % connections.size());
        return connections.get(idx);
    }

    @Override
    public void close() { closeAll(); }

    private void closeAll() {
        for (Connection c : connections) {
            try { c.close(); }
            catch (JMSException e) { log.warn("Error closing connection: {}", e.getMessage()); }
        }
        connections.clear();
        log.info("Connection pool closed");
    }

    public List<Connection> getConnections() { return Collections.unmodifiableList(connections); }

    // ─── Factory ─────────────────────────────────────────────────────────────

    private ConnectionFactory buildFactory(StressConfig config) {
        String host = "localhost";
        int    port = 61616;
        try {
            String raw = config.getBrokerUrl()
                    .replaceFirst("\\?.*$", "")
                    .replaceAll("^(ssl|tcp)://", "tcp://");
            URI uri = new URI(raw);
            if (uri.getHost() != null) host = uri.getHost();
            if (uri.getPort() > 0)    port  = uri.getPort();
        } catch (Exception e) {
            log.warn("Cannot parse broker URL '{}', using {}:{}", config.getBrokerUrl(), host, port);
        }

        Map<String, Object> params = new HashMap<>();
        params.put(TransportConstants.HOST_PROP_NAME, host);
        params.put(TransportConstants.PORT_PROP_NAME, port);

        if (config.getKeystorePath() != null) {
            String ksPath = resolveAbsolutePath(config.getKeystorePath());
            String tsPath = resolveAbsolutePath(config.getTruststorePath());
            String ksType = detectStoreType(config.getKeystoreType(), ksPath);
            String tsType = detectStoreType(config.getKeystoreType(), tsPath);

            params.put(TransportConstants.SSL_ENABLED_PROP_NAME,         true);
            params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME,       ksPath);
            params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME,   config.getKeystorePassword());
            params.put(TransportConstants.KEYSTORE_TYPE_PROP_NAME,       ksType);
            params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME,     tsPath);
            params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, config.getTruststorePassword());
            params.put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME,     tsType);
            params.put(TransportConstants.ENABLED_PROTOCOLS_PROP_NAME,   config.getTlsVersion());

            log.info("SSL transport — host={} port={} ks={} ts={}", host, port, ksPath, tsPath);
        } else {
            log.info("Plain TCP transport — host={} port={}", host, port);
        }

        TransportConfiguration tc = new TransportConfiguration(
                NettyConnectorFactory.class.getName(), params);
        try {
            return ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, tc);
        } catch (Exception e) {
            throw new RuntimeException("Failed to build ConnectionFactory", e);
        }
    }

    // ─── Credential helpers ───────────────────────────────────────────────────

    private String resolveCredential(String explicit, String paramName, String brokerUrl) {
        if (explicit != null && !explicit.isBlank()) {
            log.info("Using explicit --{} flag", paramName);
            return explicit;
        }
        String fromUrl = extractQueryParam(brokerUrl, paramName);
        if (fromUrl != null) log.info("Using {} from broker URL query string", paramName);
        return fromUrl;
    }

    private String extractQueryParam(String url, String name) {
        int q = url.indexOf('?');
        if (q < 0) return null;
        for (String pair : url.substring(q + 1).split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0 && pair.substring(0, eq).trim().equals(name))
                return pair.substring(eq + 1).trim();
        }
        return null;
    }

    // ─── Path / type helpers ──────────────────────────────────────────────────

    private String resolveAbsolutePath(String path) {
        if (path == null) return null;
        Path p   = Paths.get(path);
        Path abs = p.isAbsolute() ? p : Paths.get("").toAbsolutePath().resolve(p).normalize();
        if (!Files.exists(abs)) {
            log.error("Store file NOT FOUND: {} (resolved from '{}')", abs, path);
            log.error("Working dir: {}", Paths.get("").toAbsolutePath());
        } else {
            log.info("  store OK: {}", abs);
        }
        return abs.toString();
    }

    private String detectStoreType(String configured, String path) {
        if (path == null) return configured;
        String lower = path.toLowerCase();
        if (lower.endsWith(".p12") || lower.endsWith(".pfx")) {
            if (!"PKCS12".equalsIgnoreCase(configured))
                log.info("  auto-detected PKCS12 from extension (was: {})", configured);
            return "PKCS12";
        }
        if (lower.endsWith(".jks")) return "JKS";
        return configured;
    }
}
