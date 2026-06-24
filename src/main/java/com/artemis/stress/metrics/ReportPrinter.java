package com.artemis.stress.metrics;

import com.artemis.stress.config.StressConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Periodically prints live metrics and produces a final report covering both
 * producer (send latency) and consumer (end-to-end latency) sides.
 */
public class ReportPrinter implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ReportPrinter.class);
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static final String RESET  = "\u001B[0m";
    private static final String CYAN   = "\u001B[36m";
    private static final String GREEN  = "\u001B[32m";
    private static final String YELLOW = "\u001B[33m";
    private static final String RED    = "\u001B[31m";
    private static final String BOLD   = "\u001B[1m";

    private final MetricsCollector metrics;
    private final StressConfig     config;
    private final int              intervalSeconds;
    private final AtomicBoolean    running = new AtomicBoolean(true);

    public ReportPrinter(MetricsCollector metrics, StressConfig config) {
        this.metrics         = metrics;
        this.config          = config;
        this.intervalSeconds = config.getReportIntervalSeconds();
    }

    public void stop() { running.set(false); }

    @Override
    public void run() {
        printHeader();
        while (running.get()) {
            try { Thread.sleep(intervalSeconds * 1000L); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
            if (running.get()) printLiveLine();
        }
    }

    // ─── Live output ──────────────────────────────────────────────────────────

    private boolean showE2e() {
        return config.getMode() != StressConfig.Mode.PRODUCE;
    }

    private void printHeader() {
        System.out.println();
        if (showE2e()) {
            System.out.printf(BOLD +
                "%-8s │ %8s │ %8s │ %8s │ %8s │ %8s │ %8s │ %8s │ %8s%n" + RESET,
                "Time", "Sent", "Send/s", "sndP99ms",
                "Recv", "Recv/s", "e2eP99ms", "MB", "Errors");
            System.out.println("─".repeat(98));
        } else {
            System.out.printf(BOLD +
                "%-8s │ %10s │ %10s │ %8s │ %8s │ %8s │ %8s │ %8s │ %8s%n" + RESET,
                "Time", "Sent", "Rate msg/s", "Mean ms",
                "p50 ms", "p90 ms", "p99 ms", "Bytes MB", "Errors");
            System.out.println("─".repeat(96));
        }
    }

    private void printLiveLine() {
        if (!metrics.isWarmupDone()) {
            System.out.printf(YELLOW + "  [WARMUP in progress...]%n" + RESET);
            return;
        }

        String time    = LocalDateTime.now().format(FMT);
        long   sent    = metrics.getSendCount();
        double sndRate = metrics.currentSendRate();   // double msg/s
        double sndP99  = metrics.getSendPercentileNs(0.99) / 1_000_000.0;
        long   recv    = metrics.getE2eCount();
        double e2eRate = metrics.currentE2eRate();    // double msg/s — fixed
        double e2eP99  = metrics.getE2ePercentileNs(0.99) / 1_000_000.0;
        double bytesM  = metrics.getSendBytes() / (1024.0 * 1024.0);
        long   errors  = metrics.getSendErrors() + metrics.getE2eErrors();
        String errStr  = errors > 0 ? RED + errors + RESET : GREEN + "0" + RESET;

        if (showE2e()) {
            System.out.printf(CYAN + "%-8s" + RESET +
                " │ %8d │ %8.1f │ %8.2f │ %8d │ %8.1f │ %8.2f │ %8.2f │ %s%n",
                time, sent, sndRate, sndP99, recv, e2eRate, e2eP99, bytesM, errStr);
        } else {
            double meanMs = metrics.getSendLatMeanNs() / 1_000_000.0;
            double p50Ms  = metrics.getSendPercentileNs(0.50) / 1_000_000.0;
            double p90Ms  = metrics.getSendPercentileNs(0.90) / 1_000_000.0;
            System.out.printf(CYAN + "%-8s" + RESET +
                " │ %10d │ %10.1f │ %8.2f │ %8.2f │ %8.2f │ %8.2f │ %8.2f │ %s%n",
                time, sent, sndRate, meanMs, p50Ms, p90Ms, sndP99, bytesM, errStr);
        }
    }

    // ─── Final report ─────────────────────────────────────────────────────────

    public void printFinalReport(long elapsedMs) {
        System.out.println("\n" + "═".repeat(96));
        System.out.println(BOLD + "  FINAL REPORT" + RESET);
        System.out.println("═".repeat(96));

        double elapsed = elapsedMs / 1000.0;

        if (config.getMode() != StressConfig.Mode.CONSUME) {
            double totalMb = metrics.getSendBytes() / (1024.0 * 1024.0);
            System.out.println(BOLD + "  PRODUCER (send latency)" + RESET);
            System.out.printf("  %-32s : %d%n",       "Messages sent",    metrics.getSendCount());
            System.out.printf("  %-32s : %d%n",       "Send errors",      metrics.getSendErrors());
            System.out.printf("  %-32s : %.2f s%n",   "Elapsed time",     elapsed);
            System.out.printf("  %-32s : %.1f msg/s%n","Throughput",       metrics.overallSendRate());
            System.out.printf("  %-32s : %.2f MB%n",  "Total data",       totalMb);
            System.out.printf("  %-32s : %.2f MB/s%n","Data rate",        totalMb / Math.max(elapsed, 0.001));
            System.out.println("  Send latency:");
            printLatencyBlock(
                    metrics.getSendLatMinNs(),  metrics.getSendLatMeanNs(),
                    metrics.getSendPercentileNs(0.50), metrics.getSendPercentileNs(0.75),
                    metrics.getSendPercentileNs(0.90), metrics.getSendPercentileNs(0.95),
                    metrics.getSendPercentileNs(0.99), metrics.getSendPercentileNs(0.999),
                    metrics.getSendLatMaxNs());
            System.out.println();
        }

        if (config.getMode() != StressConfig.Mode.PRODUCE) {
            System.out.println(BOLD + "  CONSUMER (end-to-end latency: send → receive)" + RESET);
            System.out.printf("  %-32s : %d%n",       "Messages received", metrics.getE2eCount());
            System.out.printf("  %-32s : %d%n",       "Consumer errors",   metrics.getE2eErrors());
            System.out.printf("  %-32s : %.1f msg/s%n","Consume rate",      metrics.overallE2eRate());
            System.out.println("  End-to-end latency:");
            printLatencyBlock(
                    metrics.getE2eLatMinNs(),   metrics.getE2eLatMeanNs(),
                    metrics.getE2ePercentileNs(0.50), metrics.getE2ePercentileNs(0.75),
                    metrics.getE2ePercentileNs(0.90), metrics.getE2ePercentileNs(0.95),
                    metrics.getE2ePercentileNs(0.99), metrics.getE2ePercentileNs(0.999),
                    metrics.getE2eLatMaxNs());
            System.out.println();
        }

        long totalErrors = metrics.getSendErrors() + metrics.getE2eErrors();
        System.out.println(totalErrors > 0
                ? RED   + "  ⚠ Completed with " + totalErrors + " error(s)" + RESET
                : GREEN + "  ✔ Test completed successfully" + RESET);
        System.out.println("═".repeat(96));
    }

    private void printLatencyBlock(long minNs, long meanNs,
            long p50Ns, long p75Ns, long p90Ns, long p95Ns,
            long p99Ns, long p999Ns, long maxNs) {
        System.out.printf("    %-28s : %.3f ms%n", "Min",          minNs  / 1e6);
        System.out.printf("    %-28s : %.3f ms%n", "Mean",         meanNs / 1e6);
        System.out.printf("    %-28s : %.3f ms%n", "p50 (median)", p50Ns  / 1e6);
        System.out.printf("    %-28s : %.3f ms%n", "p75",          p75Ns  / 1e6);
        System.out.printf("    %-28s : %.3f ms%n", "p90",          p90Ns  / 1e6);
        System.out.printf("    %-28s : %.3f ms%n", "p95",          p95Ns  / 1e6);
        System.out.printf("    %-28s : %.3f ms%n", "p99",          p99Ns  / 1e6);
        System.out.printf("    %-28s : %.3f ms%n", "p99.9",        p999Ns / 1e6);
        System.out.printf("    %-28s : %.3f ms%n", "Max",          maxNs  / 1e6);
    }

    // ─── CSV ──────────────────────────────────────────────────────────────────

    public void writeCsvReport(File outputFile, long elapsedMs) {
        try (PrintWriter pw = new PrintWriter(new FileWriter(outputFile))) {
            pw.println("metric,value");
            pw.printf("messages_sent,%d%n",            metrics.getSendCount());
            pw.printf("send_errors,%d%n",              metrics.getSendErrors());
            pw.printf("elapsed_seconds,%.3f%n",        elapsedMs / 1000.0);
            pw.printf("send_throughput_msg_s,%.2f%n",  metrics.overallSendRate());
            pw.printf("total_bytes_mb,%.3f%n",         metrics.getSendBytes() / (1024.0 * 1024.0));
            pw.printf("send_lat_min_ms,%.3f%n",        metrics.getSendLatMinNs()          / 1e6);
            pw.printf("send_lat_mean_ms,%.3f%n",       metrics.getSendLatMeanNs()         / 1e6);
            pw.printf("send_lat_p50_ms,%.3f%n",        metrics.getSendPercentileNs(0.50)  / 1e6);
            pw.printf("send_lat_p90_ms,%.3f%n",        metrics.getSendPercentileNs(0.90)  / 1e6);
            pw.printf("send_lat_p99_ms,%.3f%n",        metrics.getSendPercentileNs(0.99)  / 1e6);
            pw.printf("send_lat_p999_ms,%.3f%n",       metrics.getSendPercentileNs(0.999) / 1e6);
            pw.printf("send_lat_max_ms,%.3f%n",        metrics.getSendLatMaxNs()          / 1e6);
            pw.printf("messages_received,%d%n",        metrics.getE2eCount());
            pw.printf("consumer_errors,%d%n",          metrics.getE2eErrors());
            pw.printf("e2e_throughput_msg_s,%.2f%n",   metrics.overallE2eRate());
            pw.printf("e2e_lat_min_ms,%.3f%n",         metrics.getE2eLatMinNs()           / 1e6);
            pw.printf("e2e_lat_mean_ms,%.3f%n",        metrics.getE2eLatMeanNs()          / 1e6);
            pw.printf("e2e_lat_p50_ms,%.3f%n",         metrics.getE2ePercentileNs(0.50)   / 1e6);
            pw.printf("e2e_lat_p90_ms,%.3f%n",         metrics.getE2ePercentileNs(0.90)   / 1e6);
            pw.printf("e2e_lat_p99_ms,%.3f%n",         metrics.getE2ePercentileNs(0.99)   / 1e6);
            pw.printf("e2e_lat_p999_ms,%.3f%n",        metrics.getE2ePercentileNs(0.999)  / 1e6);
            pw.printf("e2e_lat_max_ms,%.3f%n",         metrics.getE2eLatMaxNs()           / 1e6);
        } catch (IOException e) {
            log.error("Failed to write CSV report: {}", e.getMessage(), e);
        }
    }
}
