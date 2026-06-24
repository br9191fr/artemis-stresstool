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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages a pool of JMS connections using the programmatic TransportConfiguration
 * API (bypasses Artemis URL schema registry entirely).
 *
 * Credential handling:
 *  - SSL connections  → authenticated by X.509 client certificate (no user/password)
 *  - Plain TCP        → user/password extracted from broker URL query string
 *                       (e.g. tcp://localhost:61616?user=admin&password=secret)
 *                       and passed to createConnection(user, password)
 */
public class ConnectionPool implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConnectionPool.class);

    private final List<Connection> connections;
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);

    public ConnectionPool(StressConfig config) {
        connections = new ArrayList<>(config.getConnectionPoolSize());

        // Parse credentials from URL before building the factory
        String[] creds = extractCredentials(config.getBrokerUrl());
        String user     = creds[0];  // null if not present
        String password = creds[1];  // null if not present

        ConnectionFactory factory = buildFactory(config);

        for (int i = 0; i < config.getConnectionPoolSize(); i++) {
            try {
                Connection conn;
                if (user != null) {
                    log.info("Authenticating as user '{}'", user);
                    conn = factory.createConnection(user, password);
                } else {
                    conn = factory.createConnection();
                }
                conn.start();
                connections.add(conn);
                log.info("JMS connection #{} established to {}", i + 1, config.getBrokerUrl());
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

    @Override public void close() { closeAll(); }

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
        // Parse host:port only (credentials handled separately via createConnection)
        String host = "localhost";
        int    port  = 61616;
        try {
            String raw = config.getBrokerUrl()
                    .replaceFirst("\\?.*$", "")
                    .replaceAll("^(ssl|tcp)://", "tcp://");
            URI uri = new URI(raw);
            if (uri.getHost() != null) host = uri.getHost();
            if (uri.getPort() > 0)    port = uri.getPort();
        } catch (Exception e) {
            log.warn("Cannot parse broker URL '{}', using {}:{}", config.getBrokerUrl(), host, port);
        }

        Map<String, Object> params = new HashMap<>();
        params.put(TransportConstants.HOST_PROP_NAME, host);
        params.put(TransportConstants.PORT_PROP_NAME, port);

        boolean useSsl = config.getKeystorePath() != null;
        if (useSsl) {
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

            log.info("SSL transport — host={} port={}", host, port);
            log.info("  keystore  : {} (type={})", ksPath, ksType);
            log.info("  truststore: {} (type={})", tsPath, tsType);
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

    // ─── Credential extraction ────────────────────────────────────────────────

    /**
     * Extracts {@code user} and {@code password} from the broker URL query string.
     *
     * <p>Example: {@code tcp://localhost:61616?user=admin&password=secret}
     * returns {@code ["admin", "secret"]}.</p>
     *
     * <p>Returns {@code [null, null]} when neither parameter is present
     * (SSL certificate auth or anonymous).</p>
     */
    private String[] extractCredentials(String brokerUrl) {
        String user     = null;
        String password = null;

        int qmark = brokerUrl.indexOf('?');
        if (qmark < 0) return new String[]{null, null};

        String query = brokerUrl.substring(qmark + 1);
        Map<String, String> params = parseQueryString(query);
        user     = params.get("user");
        password = params.get("password");

        return new String[]{user, password};
    }

    /** Minimal query-string parser — handles {@code key=value&key=value}. */
    private Map<String, String> parseQueryString(String query) {
        Map<String, String> map = new LinkedHashMap<>();
        if (query == null || query.isEmpty()) return map;
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                String k = pair.substring(0, eq).trim();
                String v = pair.substring(eq + 1).trim();
                map.put(k, v);
            }
        }
        return map;
    }

    // ─── Path / type helpers ──────────────────────────────────────────────────

    private String resolveAbsolutePath(String path) {
        if (path == null) return null;
        Path p   = Paths.get(path);
        Path abs = p.isAbsolute() ? p : Paths.get("").toAbsolutePath().resolve(p).normalize();
        if (!Files.exists(abs)) {
            log.error("┌─ STORE FILE NOT FOUND ─────────────────────────────────");
            log.error("│  Input path  : {}", path);
            log.error("│  Resolved to : {}", abs);
            log.error("│  Working dir : {}", Paths.get("").toAbsolutePath());
            log.error("└─ Use an absolute path, e.g.: /home/user/certs/client.p12");
        } else {
            log.info("  store file OK: {}", abs);
        }
        return abs.toString();
    }

    /** Auto-detects PKCS12 from .p12/.pfx extension. */
    private String detectStoreType(String configuredType, String path) {
        if (path == null) return configuredType;
        String lower = path.toLowerCase();
        if (lower.endsWith(".p12") || lower.endsWith(".pfx")) {
            if (!"PKCS12".equalsIgnoreCase(configuredType)) {
                log.info("  auto-detected store type PKCS12 (was configured as: {})", configuredType);
            }
            return "PKCS12";
        }
        if (lower.endsWith(".jks")) return "JKS";
        return configuredType;
    }
}
