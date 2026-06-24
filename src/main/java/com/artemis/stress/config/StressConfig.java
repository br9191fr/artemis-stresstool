package com.artemis.stress.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StressConfig {

    public enum Mode { PRODUCE, CONSUME, BOTH }
    private Mode mode = Mode.PRODUCE;

    private String  brokerUrl  = "tcp://localhost:61616";
    private String  queue      = "STRESS.TEST";
    private boolean useTopic   = false;

    private String user;
    private String password;

    private String  keystorePath;
    private String  keystorePassword;
    private String  keyPassword;
    private String  truststorePath;
    private String  truststorePassword;
    private String  keystoreType = "JKS";
    private String  tlsVersion   = "TLSv1.3";
    private SslConfig ssl;

    private int    threads         = 4;
    private long   totalMessages   = 10_000;
    private long   durationSeconds = 0;
    private int    ratePerThread   = 0;
    private int    messageSize     = 1024;
    private String xmlTemplatePath;

    private int  consumerThreads        = 1;
    private int  consumerReceiveTimeout = 5000;

    private boolean schemaValidation = true;
    private String  schemaPath;
    private long    idStartValue = 1L;

    private boolean persistent         = true;
    private int     batchSize          = 0;
    private int     connectionPoolSize = 1;

    private int warmupMessages        = 100;
    private int reportIntervalSeconds = 5;

    public void validate() {
        if (ssl != null) {
            if (ssl.getKeystorePath()       != null) keystorePath       = ssl.getKeystorePath();
            if (ssl.getKeystorePassword()   != null) keystorePassword   = ssl.getKeystorePassword();
            if (ssl.getKeyPassword()        != null) keyPassword        = ssl.getKeyPassword();
            if (ssl.getTruststorePath()     != null) truststorePath     = ssl.getTruststorePath();
            if (ssl.getTruststorePassword() != null) truststorePassword = ssl.getTruststorePassword();
            if (ssl.getKeystoreType()       != null) keystoreType       = ssl.getKeystoreType();
            if (ssl.getTlsVersion()         != null) tlsVersion         = ssl.getTlsVersion();
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
        if (mode != Mode.CONSUME && totalMessages == 0 && durationSeconds == 0)
            throw new IllegalArgumentException("Either totalMessages > 0 or durationSeconds > 0 must be set");
        if (connectionPoolSize < 1)
            throw new IllegalArgumentException("connectionPoolSize must be >= 1");
        if (messageSize < 64)
            throw new IllegalArgumentException("messageSize must be >= 64 bytes");
        if (consumerThreads < 1)
            throw new IllegalArgumentException("consumerThreads must be >= 1");
    }

    public String summary() {
        return String.format("""
            Mode            : %s
            Broker URL      : %s
            Destination     : %s (%s)
            User            : %s
            Threads         : %d (producers) / %d (consumers)
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
            mode, brokerUrl,
            queue, useTopic ? "TOPIC" : "QUEUE",
            user != null ? user : "(certificate / anonymous)",
            threads, consumerThreads,
            totalMessages == 0 ? "unlimited" : String.valueOf(totalMessages),
            durationSeconds == 0 ? "unlimited" : durationSeconds + "s",
            ratePerThread == 0 ? "max" : String.valueOf(ratePerThread),
            messageSize, persistent, batchSize, connectionPoolSize,
            keystorePath != null ? keystorePath : "(none)", keystoreType,
            truststorePath != null ? truststorePath : "(none)",
            tlsVersion, schemaValidation,
            schemaPath != null ? schemaPath : "(bundled stress-message.xsd)",
            idStartValue, warmupMessages, reportIntervalSeconds);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SslConfig {
        private String keystorePath, keystorePassword, keyPassword;
        private String truststorePath, truststorePassword, keystoreType, tlsVersion;
        public String getKeystorePath()            { return keystorePath; }
        public void   setKeystorePath(String v)    { this.keystorePath = v; }
        public String getKeystorePassword()        { return keystorePassword; }
        public void   setKeystorePassword(String v){ this.keystorePassword = v; }
        public String getKeyPassword()             { return keyPassword; }
        public void   setKeyPassword(String v)     { this.keyPassword = v; }
        public String getTruststorePath()          { return truststorePath; }
        public void   setTruststorePath(String v)  { this.truststorePath = v; }
        public String getTruststorePassword()      { return truststorePassword; }
        public void   setTruststorePassword(String v){ this.truststorePassword = v; }
        public String getKeystoreType()            { return keystoreType; }
        public void   setKeystoreType(String v)    { this.keystoreType = v; }
        public String getTlsVersion()              { return tlsVersion; }
        public void   setTlsVersion(String v)      { this.tlsVersion = v; }
    }

    public Mode    getMode()                        { return mode; }
    public void    setMode(Mode v)                  { this.mode = v; }
    public String  getBrokerUrl()                   { return brokerUrl; }
    public void    setBrokerUrl(String v)           { this.brokerUrl = v; }
    public String  getQueue()                       { return queue; }
    public void    setQueue(String v)               { this.queue = v; }
    public boolean isUseTopic()                     { return useTopic; }
    public void    setUseTopic(boolean v)           { this.useTopic = v; }
    public String  getUser()                        { return user; }
    public void    setUser(String v)                { this.user = v; }
    public String  getPassword()                    { return password; }
    public void    setPassword(String v)            { this.password = v; }
    public String  getKeystorePath()                { return keystorePath; }
    public void    setKeystorePath(String v)        { this.keystorePath = v; }
    public String  getKeystorePassword()            { return keystorePassword; }
    public void    setKeystorePassword(String v)    { this.keystorePassword = v; }
    public String  getKeyPassword()                 { return keyPassword; }
    public void    setKeyPassword(String v)         { this.keyPassword = v; }
    public String  getTruststorePath()              { return truststorePath; }
    public void    setTruststorePath(String v)      { this.truststorePath = v; }
    public String  getTruststorePassword()          { return truststorePassword; }
    public void    setTruststorePassword(String v)  { this.truststorePassword = v; }
    public String  getKeystoreType()                { return keystoreType; }
    public void    setKeystoreType(String v)        { this.keystoreType = v; }
    public String  getTlsVersion()                  { return tlsVersion; }
    public void    setTlsVersion(String v)          { this.tlsVersion = v; }
    public SslConfig getSsl()                       { return ssl; }
    public void    setSsl(SslConfig v)              { this.ssl = v; }
    public int     getThreads()                     { return threads; }
    public void    setThreads(int v)                { this.threads = v; }
    public long    getTotalMessages()               { return totalMessages; }
    public void    setTotalMessages(long v)         { this.totalMessages = v; }
    public long    getDurationSeconds()             { return durationSeconds; }
    public void    setDurationSeconds(long v)       { this.durationSeconds = v; }
    public int     getRatePerThread()               { return ratePerThread; }
    public void    setRatePerThread(int v)          { this.ratePerThread = v; }
    public int     getMessageSize()                 { return messageSize; }
    public void    setMessageSize(int v)            { this.messageSize = v; }
    public String  getXmlTemplatePath()             { return xmlTemplatePath; }
    public void    setXmlTemplatePath(String v)     { this.xmlTemplatePath = v; }
    public int     getConsumerThreads()             { return consumerThreads; }
    public void    setConsumerThreads(int v)        { this.consumerThreads = v; }
    public int     getConsumerReceiveTimeout()      { return consumerReceiveTimeout; }
    public void    setConsumerReceiveTimeout(int v) { this.consumerReceiveTimeout = v; }
    public boolean isSchemaValidation()             { return schemaValidation; }
    public void    setSchemaValidation(boolean v)   { this.schemaValidation = v; }
    public String  getSchemaPath()                  { return schemaPath; }
    public void    setSchemaPath(String v)          { this.schemaPath = v; }
    public long    getIdStartValue()                { return idStartValue; }
    public void    setIdStartValue(long v)          { this.idStartValue = v; }
    public boolean isPersistent()                   { return persistent; }
    public void    setPersistent(boolean v)         { this.persistent = v; }
    public int     getBatchSize()                   { return batchSize; }
    public void    setBatchSize(int v)              { this.batchSize = v; }
    public int     getConnectionPoolSize()          { return connectionPoolSize; }
    public void    setConnectionPoolSize(int v)     { this.connectionPoolSize = v; }
    public int     getWarmupMessages()              { return warmupMessages; }
    public void    setWarmupMessages(int v)         { this.warmupMessages = v; }
    public int     getReportIntervalSeconds()       { return reportIntervalSeconds; }
    public void    setReportIntervalSeconds(int v)  { this.reportIntervalSeconds = v; }
}
