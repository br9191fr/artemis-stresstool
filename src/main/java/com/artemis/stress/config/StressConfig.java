package com.artemis.stress.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Unified configuration model.
 * Can be populated from CLI flags or deserialized from a JSON file.
 *
 * JSON example:
 * {
 *   "brokerUrl": "ssl://broker-host:61617",
 *   "queue": "PERF.TEST",
 *   "threads": 20,
 *   "totalMessages": 500000,
 *   "durationSeconds": 120,
 *   "messageSize": 4096,
 *   "ssl": {
 *     "keystorePath": "/certs/client.jks",
 *     "keystorePassword": "changeit",
 *     "truststorePath": "/certs/truststore.jks",
 *     "truststorePassword": "changeit",
 *     "keystoreType": "JKS",
 *     "tlsVersion": "TLSv1.3"
 *   }
 * }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StressConfig {

    // ── Broker ────────────────────────────────────────────────────────────────
    private String brokerUrl = "ssl://localhost:61617";
    private String queue = "STRESS.TEST";
    private boolean useTopic = false;

    // ── SSL (flat or nested) ──────────────────────────────────────────────────
    private String keystorePath;
    private String keystorePassword;
    private String keyPassword;
    private String truststorePath;
    private String truststorePassword;
    private String keystoreType = "JKS";
    private String tlsVersion = "TLSv1.3";

    // Nested SSL block (JSON convenience)
    private SslConfig ssl;

    // ── Load ──────────────────────────────────────────────────────────────────
    private int threads = 4;
    private long totalMessages = 10_000;
    private long durationSeconds = 0;
    private int ratePerThread = 0;
    private int messageSize = 1024;
    private String xmlTemplatePath;

    // ── JMS ───────────────────────────────────────────────────────────────────
    private boolean persistent = true;
    private int batchSize = 0;
    private int connectionPoolSize = 1;

    // ── Schema validation ─────────────────────────────────────────────────────
    /** Enable XML schema validation before every send. */
    private boolean schemaValidation = true;
    /**
     * Path to a custom XSD file.  When null the bundled {@code stress-message.xsd}
     * is used (only applicable in generated mode).
     */
    private String schemaPath;
    /**
     * Starting value for the numeric-ID counter.
     * Useful when running multiple tool instances against the same queue:
     * set non-overlapping ranges (e.g. instance A: 1, instance B: 10_000_001).
     */
    private long idStartValue = 1L;

    // ── Reporting ─────────────────────────────────────────────────────────────
    private int warmupMessages = 100;
    private int reportIntervalSeconds = 5;

    // ─── Validation ───────────────────────────────────────────────────────────

    public void validate() {
        // Merge nested ssl block if present
        if (ssl != null) {
            if (ssl.getKeystorePath() != null) keystorePath = ssl.getKeystorePath();
            if (ssl.getKeystorePassword() != null) keystorePassword = ssl.getKeystorePassword();
            if (ssl.getKeyPassword() != null) keyPassword = ssl.getKeyPassword();
            if (ssl.getTruststorePath() != null) truststorePath = ssl.getTruststorePath();
            if (ssl.getTruststorePassword() != null) truststorePassword = ssl.getTruststorePassword();
            if (ssl.getKeystoreType() != null) keystoreType = ssl.getKeystoreType();
            if (ssl.getTlsVersion() != null) tlsVersion = ssl.getTlsVersion();
        }

        if (keyPassword == null) keyPassword = keystorePassword;

        if (brokerUrl == null || brokerUrl.isBlank())
            throw new IllegalArgumentException("brokerUrl must be specified");
        if (threads < 1)
            throw new IllegalArgumentException("threads must be >= 1");
        if (totalMessages < 0)
            throw new IllegalArgumentException("totalMessages must be >= 0");
        if (durationSeconds < 0)
            throw new IllegalArgumentException("durationSeconds must be >= 0");
        if (totalMessages == 0 && durationSeconds == 0)
            throw new IllegalArgumentException("Either totalMessages > 0 or durationSeconds > 0 must be set");
        if (connectionPoolSize < 1)
            throw new IllegalArgumentException("connectionPoolSize must be >= 1");
        if (messageSize < 64)
            throw new IllegalArgumentException("messageSize must be >= 64 bytes");

        // SSL is mandatory for ssl:// URLs
        if (brokerUrl.startsWith("ssl://") || brokerUrl.startsWith("amqps://")) {
            if (keystorePath == null)
                throw new IllegalArgumentException("keystorePath is required for SSL connections");
            if (keystorePassword == null)
                throw new IllegalArgumentException("keystorePassword is required for SSL connections");
            if (truststorePath == null)
                throw new IllegalArgumentException("truststorePath is required for SSL connections");
            if (truststorePassword == null)
                throw new IllegalArgumentException("truststorePassword is required for SSL connections");
        }
    }

    public String summary() {
        return String.format("""
            Broker URL      : %s
            Destination     : %s (%s)
            Threads         : %d
            Total messages  : %s
            Duration limit  : %s
            Rate/thread     : %s msg/s
            Message size    : %d bytes
            Persistent      : %s
            Batch size      : %d (0=AUTO_ACK)
            Connection pool : %d
            Keystore        : %s (%s)
            Truststore      : %s
            TLS version     : %s
            Schema validate : %s
            Schema path     : %s
            ID start value  : %d
            Warmup msgs     : %d
            Report interval : %ds
            """,
            brokerUrl,
            queue, useTopic ? "TOPIC" : "QUEUE",
            threads,
            totalMessages == 0 ? "unlimited" : String.valueOf(totalMessages),
            durationSeconds == 0 ? "unlimited" : durationSeconds + "s",
            ratePerThread == 0 ? "max" : String.valueOf(ratePerThread),
            messageSize,
            persistent,
            batchSize,
            connectionPoolSize,
            keystorePath != null ? keystorePath : "(none)",
            keystoreType,
            truststorePath != null ? truststorePath : "(none)",
            tlsVersion,
            schemaValidation,
            schemaPath != null ? schemaPath : "(bundled stress-message.xsd)",
            idStartValue,
            warmupMessages,
            reportIntervalSeconds
        );
    }

    // ─── Nested SSL config ────────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SslConfig {
        private String keystorePath;
        private String keystorePassword;
        private String keyPassword;
        private String truststorePath;
        private String truststorePassword;
        private String keystoreType;
        private String tlsVersion;

        public String getKeystorePath() { return keystorePath; }
        public void setKeystorePath(String v) { this.keystorePath = v; }
        public String getKeystorePassword() { return keystorePassword; }
        public void setKeystorePassword(String v) { this.keystorePassword = v; }
        public String getKeyPassword() { return keyPassword; }
        public void setKeyPassword(String v) { this.keyPassword = v; }
        public String getTruststorePath() { return truststorePath; }
        public void setTruststorePath(String v) { this.truststorePath = v; }
        public String getTruststorePassword() { return truststorePassword; }
        public void setTruststorePassword(String v) { this.truststorePassword = v; }
        public String getKeystoreType() { return keystoreType; }
        public void setKeystoreType(String v) { this.keystoreType = v; }
        public String getTlsVersion() { return tlsVersion; }
        public void setTlsVersion(String v) { this.tlsVersion = v; }
    }

    // ─── Getters / Setters ────────────────────────────────────────────────────

    public String getBrokerUrl() { return brokerUrl; }
    public void setBrokerUrl(String v) { this.brokerUrl = v; }

    public String getQueue() { return queue; }
    public void setQueue(String v) { this.queue = v; }

    public boolean isUseTopic() { return useTopic; }
    public void setUseTopic(boolean v) { this.useTopic = v; }

    public String getKeystorePath() { return keystorePath; }
    public void setKeystorePath(String v) { this.keystorePath = v; }

    public String getKeystorePassword() { return keystorePassword; }
    public void setKeystorePassword(String v) { this.keystorePassword = v; }

    public String getKeyPassword() { return keyPassword; }
    public void setKeyPassword(String v) { this.keyPassword = v; }

    public String getTruststorePath() { return truststorePath; }
    public void setTruststorePath(String v) { this.truststorePath = v; }

    public String getTruststorePassword() { return truststorePassword; }
    public void setTruststorePassword(String v) { this.truststorePassword = v; }

    public String getKeystoreType() { return keystoreType; }
    public void setKeystoreType(String v) { this.keystoreType = v; }

    public String getTlsVersion() { return tlsVersion; }
    public void setTlsVersion(String v) { this.tlsVersion = v; }

    public SslConfig getSsl() { return ssl; }
    public void setSsl(SslConfig v) { this.ssl = v; }

    public int getThreads() { return threads; }
    public void setThreads(int v) { this.threads = v; }

    public long getTotalMessages() { return totalMessages; }
    public void setTotalMessages(long v) { this.totalMessages = v; }

    public long getDurationSeconds() { return durationSeconds; }
    public void setDurationSeconds(long v) { this.durationSeconds = v; }

    public int getRatePerThread() { return ratePerThread; }
    public void setRatePerThread(int v) { this.ratePerThread = v; }

    public int getMessageSize() { return messageSize; }
    public void setMessageSize(int v) { this.messageSize = v; }

    public String getXmlTemplatePath() { return xmlTemplatePath; }
    public void setXmlTemplatePath(String v) { this.xmlTemplatePath = v; }

    public boolean isPersistent() { return persistent; }
    public void setPersistent(boolean v) { this.persistent = v; }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int v) { this.batchSize = v; }

    public int getConnectionPoolSize() { return connectionPoolSize; }
    public void setConnectionPoolSize(int v) { this.connectionPoolSize = v; }

    public int getWarmupMessages() { return warmupMessages; }
    public void setWarmupMessages(int v) { this.warmupMessages = v; }

    public int getReportIntervalSeconds() { return reportIntervalSeconds; }
    public void setReportIntervalSeconds(int v) { this.reportIntervalSeconds = v; }

    public boolean isSchemaValidation() { return schemaValidation; }
    public void setSchemaValidation(boolean v) { this.schemaValidation = v; }

    public String getSchemaPath() { return schemaPath; }
    public void setSchemaPath(String v) { this.schemaPath = v; }

    public long getIdStartValue() { return idStartValue; }
    public void setIdStartValue(long v) { this.idStartValue = v; }
}
