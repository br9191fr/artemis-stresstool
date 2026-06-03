package com.artemis.stress;

import com.artemis.stress.config.StressConfig;
import com.artemis.stress.metrics.MetricsCollector;
import com.artemis.stress.metrics.ReportPrinter;
import com.artemis.stress.producer.ProducerOrchestrator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.util.concurrent.Callable;

/**
 * Entry point for the Artemis XML Stress Tool.
 *
 * Usage:
 *   java -jar artemis-stress-tool.jar --config config.json
 *   java -jar artemis-stress-tool.jar --broker-url ssl://localhost:61617 \
 *       --queue TEST.QUEUE --threads 10 --messages 10000 \
 *       --keystore client.jks --keystore-password changeit \
 *       --truststore truststore.jks --truststore-password changeit
 */
@Command(
    name = "artemis-stress",
    mixinStandardHelpOptions = true,
    version = "1.0.0",
    description = "Multi-threaded stress and performance tool for Apache Artemis (X.509 auth)"
)
public class StressToolMain implements Callable<Integer> {

    private static final Logger log = LoggerFactory.getLogger(StressToolMain.class);

    // ── Config file (overrides individual flags) ──────────────────────────────
    @Option(names = {"-c", "--config"}, description = "Path to JSON config file (overrides other options)")
    private File configFile;

    // ── Broker connection ─────────────────────────────────────────────────────
    @Option(names = {"-u", "--broker-url"},
            description = "Broker URL (default: ssl://localhost:61617)",
            defaultValue = "ssl://localhost:61617")
    private String brokerUrl;

    @Option(names = {"-q", "--queue"},
            description = "Destination queue/topic name (default: STRESS.TEST)",
            defaultValue = "STRESS.TEST")
    private String queue;

    @Option(names = {"--topic"}, description = "Use topic instead of queue")
    private boolean useTopic;

    // ── SSL / X.509 ───────────────────────────────────────────────────────────
    @Option(names = {"-k", "--keystore"},
            description = "Path to client keystore (JKS or PKCS12)")
    private String keystorePath;

    @Option(names = {"--keystore-password"},
            description = "Keystore password")
    private String keystorePassword;

    @Option(names = {"--key-password"},
            description = "Private key password (defaults to keystore password)")
    private String keyPassword;

    @Option(names = {"-t", "--truststore"},
            description = "Path to truststore (JKS or PKCS12)")
    private String truststorePath;

    @Option(names = {"--truststore-password"},
            description = "Truststore password")
    private String truststorePassword;

    @Option(names = {"--keystore-type"},
            description = "Keystore type: JKS or PKCS12 (default: JKS)",
            defaultValue = "JKS")
    private String keystoreType;

    @Option(names = {"--tls-version"},
            description = "TLS protocol version (default: TLSv1.3)",
            defaultValue = "TLSv1.3")
    private String tlsVersion;

    // ── Load parameters ───────────────────────────────────────────────────────
    @Option(names = {"-n", "--threads"},
            description = "Number of producer threads (default: 4)",
            defaultValue = "4")
    private int threads;

    @Option(names = {"-m", "--messages"},
            description = "Total messages to send, 0 = unlimited (default: 10000)",
            defaultValue = "10000")
    private long totalMessages;

    @Option(names = {"--duration"},
            description = "Max test duration in seconds, 0 = no limit (default: 0)",
            defaultValue = "0")
    private long durationSeconds;

    @Option(names = {"--rate"},
            description = "Target messages/sec per thread, 0 = max speed (default: 0)",
            defaultValue = "0")
    private int ratePerThread;

    @Option(names = {"--message-size"},
            description = "Approximate XML payload size in bytes (default: 1024)",
            defaultValue = "1024")
    private int messageSize;

    @Option(names = {"--xml-template"},
            description = "Path to XML template file (use ${NUMERIC_ID}, ${SEQ}, ${UUID}, ${TS} as placeholders)")
    private String xmlTemplatePath;

    // ── Schema validation ─────────────────────────────────────────────────────
    @Option(names = {"--schema-validation"},
            description = "Validate every XML document against XSD before sending (default: true)",
            defaultValue = "true")
    private boolean schemaValidation;

    @Option(names = {"--schema-path"},
            description = "Path to custom XSD file (default: bundled stress-message.xsd)")
    private String schemaPath;

    @Option(names = {"--id-start"},
            description = "First numeric ID value. Useful for multi-instance runs (default: 1)",
            defaultValue = "1")
    private long idStartValue;

    // ── JMS options ───────────────────────────────────────────────────────────
    @Option(names = {"--persistent"},
            description = "Send persistent messages (default: true)",
            defaultValue = "true")
    private boolean persistent;

    @Option(names = {"--batch-size"},
            description = "Commit batch size for transacted sessions (0 = auto-ack, default: 0)",
            defaultValue = "0")
    private int batchSize;

    @Option(names = {"--connection-pool"},
            description = "Number of JMS connections shared across threads (default: 1)",
            defaultValue = "1")
    private int connectionPoolSize;

    @Option(names = {"--warmup"},
            description = "Warmup messages to send before recording metrics (default: 100)",
            defaultValue = "100")
    private int warmupMessages;

    // ── Reporting ─────────────────────────────────────────────────────────────
    @Option(names = {"--report-interval"},
            description = "Live metrics print interval in seconds (default: 5)",
            defaultValue = "5")
    private int reportIntervalSeconds;

    @Option(names = {"--output"},
            description = "Write final report to file (CSV format)")
    private File outputFile;

    // ─────────────────────────────────────────────────────────────────────────

    public static void main(String[] args) {
        int exitCode = new CommandLine(new StressToolMain()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        printBanner();

        // Build config (file wins over CLI flags)
        StressConfig config;
        if (configFile != null) {
            log.info("Loading configuration from {}", configFile.getAbsolutePath());
            config = new ObjectMapper().readValue(configFile, StressConfig.class);
        } else {
            config = buildConfigFromCli();
        }

        config.validate();
        log.info("Configuration validated:\n{}", config.summary());

        // Metrics collector (shared across all threads)
        MetricsCollector metrics = new MetricsCollector(config.getWarmupMessages());

        // Live reporter
        ReportPrinter reporter = new ReportPrinter(metrics, config.getReportIntervalSeconds());
        Thread reporterThread = new Thread(reporter, "metrics-reporter");
        reporterThread.setDaemon(true);
        reporterThread.start();

        // Orchestrate producers
        ProducerOrchestrator orchestrator = new ProducerOrchestrator(config, metrics);

        long startMs = System.currentTimeMillis();
        try {
            orchestrator.run();
        } finally {
            reporter.stop();
            reporterThread.join(2000);
        }
        long elapsedMs = System.currentTimeMillis() - startMs;

        // Final report
        reporter.printFinalReport(elapsedMs);
        if (outputFile != null) {
            reporter.writeCsvReport(outputFile, elapsedMs);
            log.info("Report written to {}", outputFile.getAbsolutePath());
        }

        return metrics.hasErrors() ? 1 : 0;
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private StressConfig buildConfigFromCli() {
        StressConfig c = new StressConfig();
        c.setBrokerUrl(brokerUrl);
        c.setQueue(queue);
        c.setUseTopic(useTopic);
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
        c.setPersistent(persistent);
        c.setBatchSize(batchSize);
        c.setConnectionPoolSize(connectionPoolSize);
        c.setWarmupMessages(warmupMessages);
        c.setReportIntervalSeconds(reportIntervalSeconds);
        c.setSchemaValidation(schemaValidation);
        c.setSchemaPath(schemaPath);
        c.setIdStartValue(idStartValue);
        return c;
    }

    private void printBanner() {
        System.out.println("""
            ╔══════════════════════════════════════════════════════════╗
            ║         ARTEMIS XML STRESS TOOL  v1.0.0                 ║
            ║  Multi-threaded • X.509 Auth • Live Metrics             ║
            ╚══════════════════════════════════════════════════════════╝
            """);
    }
}
