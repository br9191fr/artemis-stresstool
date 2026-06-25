package com.artemis.stress;

import com.artemis.stress.config.StressConfig;
import com.artemis.stress.consumer.ConsumerOrchestrator;
import com.artemis.stress.metrics.MetricsCollector;
import com.artemis.stress.metrics.ReportPrinter;
import com.artemis.stress.otel.OtelMetricsExporter;
import com.artemis.stress.producer.ConnectionPool;
import com.artemis.stress.producer.ProducerOrchestrator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

@Command(
    name = "artemis-stress",
    mixinStandardHelpOptions = true,
    version = "1.2.0",
    description = "Multi-threaded XML stress tool for Apache Artemis"
)
public class StressToolMain implements Callable<Integer> {

    private static final Logger log = LoggerFactory.getLogger(StressToolMain.class);

    @Option(names = {"-c", "--config"},
            description = "JSON config file (overrides all other options)")
    private File configFile;

    @Option(names = {"--mode"},
            description = "PRODUCE | CONSUME | BOTH (default: PRODUCE)",
            defaultValue = "PRODUCE")
    private StressConfig.Mode mode;

    @Option(names = {"-u", "--broker-url"},
            description = "Broker URL (default: tcp://localhost:61616)",
            defaultValue = "tcp://localhost:61616")
    private String brokerUrl;

    @Option(names = {"-q", "--queue"},
            description = "Queue/topic name (default: STRESS.TEST)",
            defaultValue = "STRESS.TEST")
    private String queue;

    @Option(names = {"--topic"}, description = "Use topic instead of queue")
    private boolean useTopic;

    @Option(names = {"--user"},
            description = "JMS username (takes priority over user= in broker URL)")
    private String user;

    @Option(names = {"--password"},
            description = "JMS password (takes priority over password= in broker URL)")
    private String password;

    @Option(names = {"-k", "--keystore"},          description = "Client keystore path (JKS or PKCS12)")
    private String keystorePath;

    @Option(names = {"--keystore-password"},       description = "Keystore password")
    private String keystorePassword;

    @Option(names = {"--key-password"},            description = "Private key password")
    private String keyPassword;

    @Option(names = {"-t", "--truststore"},        description = "Truststore path")
    private String truststorePath;

    @Option(names = {"--truststore-password"},     description = "Truststore password")
    private String truststorePassword;

    @Option(names = {"--keystore-type"},           description = "JKS or PKCS12 (auto-detected from .p12/.pfx)", defaultValue = "JKS")
    private String keystoreType;

    @Option(names = {"--tls-version"},             description = "TLS protocol version (default: TLSv1.3)", defaultValue = "TLSv1.3")
    private String tlsVersion;

    @Option(names = {"-n", "--threads"},           description = "Producer threads (default: 4)",               defaultValue = "4")
    private int threads;

    @Option(names = {"-m", "--messages"},          description = "Total messages; 0=unlimited (default: 10000)", defaultValue = "10000")
    private long totalMessages;

    @Option(names = {"--duration"},                description = "Max duration seconds; 0=no limit (default: 0)", defaultValue = "0")
    private long durationSeconds;

    @Option(names = {"--rate"},                    description = "Target msg/s per producer thread; 0=max (default: 0)", defaultValue = "0")
    private int ratePerThread;

    @Option(names = {"--message-size"},            description = "Approx XML payload bytes (default: 1024)",    defaultValue = "1024")
    private int messageSize;

    @Option(names = {"--xml-template"},            description = "XML template file")
    private String xmlTemplatePath;

    @Option(names = {"--consumer-threads"},        description = "Consumer threads (default: 1)",               defaultValue = "1")
    private int consumerThreads;

    @Option(names = {"--consumer-timeout"},        description = "Consumer receive timeout ms (default: 5000)", defaultValue = "5000")
    private int consumerReceiveTimeout;

    @Option(names = {"--persistent"},              description = "Persistent delivery (default: true)",          defaultValue = "true")
    private boolean persistent;

    @Option(names = {"--batch-size"},              description = "Transacted batch size; 0=AUTO_ACK (default: 0)", defaultValue = "0")
    private int batchSize;

    @Option(names = {"--connection-pool"},         description = "Shared JMS connections (default: 1)",          defaultValue = "1")
    private int connectionPoolSize;

    @Option(names = {"--schema-validation"},       description = "Validate XML before send (default: true)",    defaultValue = "true")
    private boolean schemaValidation;

    @Option(names = {"--schema-path"},             description = "Custom XSD (default: bundled stress-message.xsd)")
    private String schemaPath;

    @Option(names = {"--id-start"},                description = "First numeric ID value (default: 1)",          defaultValue = "1")
    private long idStartValue;

    @Option(names = {"--warmup"},                  description = "Warmup messages excluded from metrics (default: 100)", defaultValue = "100")
    private int warmupMessages;

    @Option(names = {"--report-interval"},         description = "Live metrics interval seconds (default: 5)",   defaultValue = "5")
    private int reportIntervalSeconds;

    @Option(names = {"--output"},                  description = "CSV report output file")
    private File outputFile;

    // ── OpenTelemetry ─────────────────────────────────────────────────────────

    @Option(names = {"--otel-endpoint"},
            description = "OTLP/gRPC endpoint for OpenTelemetry export, e.g. http://localhost:4317. " +
                          "When omitted, OTel export is disabled.")
    private String otelEndpoint;

    @Option(names = {"--otel-service-name"},
            description = "Value of the service.name resource attribute (default: artemis-stress-tool)",
            defaultValue = "artemis-stress-tool")
    private String otelServiceName;

    @Option(names = {"--otel-interval"},
            description = "OTel metric export interval in milliseconds (default: 5000)",
            defaultValue = "5000")
    private long otelIntervalMs;

    // ─────────────────────────────────────────────────────────────────────────

    public static void main(String[] args) {
        System.exit(new CommandLine(new StressToolMain()).execute(args));
    }

    @Override
    public Integer call() throws Exception {
        printBanner();

        StressConfig config = configFile != null
                ? new ObjectMapper().readValue(configFile, StressConfig.class)
                : buildConfigFromCli();

        config.validate();
        log.info("Configuration validated:\n{}", config.summary());

        MetricsCollector metrics  = new MetricsCollector(config.getWarmupMessages());
        ReportPrinter    reporter = new ReportPrinter(metrics, config);
        Thread reporterThread = new Thread(reporter, "metrics-reporter");
        reporterThread.setDaemon(true);
        reporterThread.start();

        // Start OTel exporter if endpoint is configured
        OtelMetricsExporter otelExporter = null;
        if (otelEndpoint != null && !otelEndpoint.isBlank()) {
            otelExporter = new OtelMetricsExporter(
                    metrics, config, otelEndpoint, otelIntervalMs, otelServiceName);
        }

        long startMs = System.currentTimeMillis();
        try {
            switch (config.getMode()) {

                case PRODUCE ->
                    new ProducerOrchestrator(config, metrics).run();

                case CONSUME -> {
                    ConnectionPool pool     = new ConnectionPool(config);
                    AtomicBoolean stopFlag  = new AtomicBoolean(false);
                    if (config.getDurationSeconds() > 0) {
                        Thread timer = new Thread(() -> {
                            try { Thread.sleep(config.getDurationSeconds() * 1000L); }
                            catch (InterruptedException ignored) {}
                            log.info("Duration limit reached — stopping consumers");
                            stopFlag.set(true);
                        }, "duration-timer");
                        timer.setDaemon(true);
                        timer.start();
                    }
                    try { new ConsumerOrchestrator(config, pool, metrics).run(stopFlag); }
                    finally { pool.close(); }
                }

                case BOTH ->
                    new ProducerOrchestrator(config, metrics).run();
            }
        } finally {
            reporter.stop();
            reporterThread.join(2000);
            if (otelExporter != null) otelExporter.close();
        }

        long elapsedMs = System.currentTimeMillis() - startMs;
        reporter.printFinalReport(elapsedMs);

        if (outputFile != null) {
            reporter.writeCsvReport(outputFile, elapsedMs);
            log.info("CSV report written to {}", outputFile.getAbsolutePath());
        }

        return metrics.hasErrors() ? 1 : 0;
    }

    private StressConfig buildConfigFromCli() {
        StressConfig c = new StressConfig();
        c.setMode(mode);
        c.setBrokerUrl(brokerUrl);
        c.setQueue(queue);
        c.setUseTopic(useTopic);
        c.setUser(user);
        c.setPassword(password);
        c.setKeystorePath(keystorePath);
        c.setKeystorePassword(keystorePassword);
        c.setKeyPassword(keyPassword != null ? keyPassword : keystorePassword);
        c.setTruststorePath(truststorePath);
        c.setTruststorePassword(truststorePassword);
        c.setKeystoreType(keystoreType);
        c.setTlsVersion(tlsVersion);
        c.setThreads(threads);
        c.setTotalMessages(totalMessages);
        c.setDurationSeconds(durationSeconds);
        c.setRatePerThread(ratePerThread);
        c.setMessageSize(messageSize);
        c.setXmlTemplatePath(xmlTemplatePath);
        c.setConsumerThreads(consumerThreads);
        c.setConsumerReceiveTimeout(consumerReceiveTimeout);
        c.setPersistent(persistent);
        c.setBatchSize(batchSize);
        c.setConnectionPoolSize(connectionPoolSize);
        c.setSchemaValidation(schemaValidation);
        c.setSchemaPath(schemaPath);
        c.setIdStartValue(idStartValue);
        c.setWarmupMessages(warmupMessages);
        c.setReportIntervalSeconds(reportIntervalSeconds);
        return c;
    }

    private void printBanner() {
        System.out.println("""
            ╔══════════════════════════════════════════════════════════╗
            ║         ARTEMIS XML STRESS TOOL  v1.2.0                 ║
            ║  Produce • Consume • E2E Latency • OpenTelemetry        ║
            ╚══════════════════════════════════════════════════════════╝
            """);
    }
}
