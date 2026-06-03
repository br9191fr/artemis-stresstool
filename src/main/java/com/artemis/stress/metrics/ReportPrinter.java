package com.artemis.stress.metrics;

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
 * Periodically prints live performance metrics to stdout and,
 * at the end of the test, prints a summary table and optionally
 * writes a CSV report.
 */
public class ReportPrinter implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ReportPrinter.class);
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final String ANSI_RESET  = "\u001B[0m";
    private static final String ANSI_CYAN   = "\u001B[36m";
    private static final String ANSI_GREEN  = "\u001B[32m";
    private static final String ANSI_YELLOW = "\u001B[33m";
    private static final String ANSI_RED    = "\u001B[31m";
    private static final String ANSI_BOLD   = "\u001B[1m";

    private final MetricsCollector metrics;
    private final int intervalSeconds;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public ReportPrinter(MetricsCollector metrics, int intervalSeconds) {
        this.metrics = metrics;
        this.intervalSeconds = intervalSeconds;
    }

    public void stop() {
        running.set(false);
    }

    @Override
    public void run() {
        printHeader();
        while (running.get()) {
            try {
                Thread.sleep(intervalSeconds * 1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (running.get()) {
                printLiveLine();
            }
        }
    }

    // ─── Live output ──────────────────────────────────────────────────────────

    private void printHeader() {
        System.out.println();
        System.out.printf(ANSI_BOLD +
                "%-8s │ %10s │ %10s │ %8s │ %8s │ %8s │ %8s │ %8s │ %8s%n" + ANSI_RESET,
                "Time", "Sent", "Rate msg/s", "Mean ms", "p50 ms", "p90 ms", "p99 ms",
                "Bytes MB", "Errors");
        System.out.println("─".repeat(96));
    }

    private void printLiveLine() {
        if (!metrics.isWarmupDone()) {
            System.out.printf(ANSI_YELLOW + "  [WARMUP in progress...]%n" + ANSI_RESET);
            return;
        }

        String time     = LocalDateTime.now().format(FMT);
        long   sent     = metrics.getMessageCount();
        double rate     = metrics.currentThroughput();
        double meanMs   = metrics.getLatencyMeanNs()  / 1_000_000.0;
        double p50Ms    = metrics.getPercentileNs(0.50) / 1_000_000.0;
        double p90Ms    = metrics.getPercentileNs(0.90) / 1_000_000.0;
        double p99Ms    = metrics.getPercentileNs(0.99) / 1_000_000.0;
        double bytesM   = metrics.getBytesCount() / (1024.0 * 1024.0);
        long   errors   = metrics.getErrorCount();

        String errStr   = errors > 0
                ? ANSI_RED + errors + ANSI_RESET
                : ANSI_GREEN + "0" + ANSI_RESET;

        System.out.printf(ANSI_CYAN + "%-8s" + ANSI_RESET +
                " │ %10d │ %10.1f │ %8.2f │ %8.2f │ %8.2f │ %8.2f │ %8.2f │ %s%n",
                time, sent, rate, meanMs, p50Ms, p90Ms, p99Ms, bytesM, errStr);
    }

    // ─── Final report ─────────────────────────────────────────────────────────

    public void printFinalReport(long elapsedMs) {
        System.out.println("\n" + "═".repeat(96));
        System.out.println(ANSI_BOLD + "  FINAL REPORT" + ANSI_RESET);
        System.out.println("═".repeat(96));

        long   sent      = metrics.getMessageCount();
        long   errors    = metrics.getErrorCount();
        double elapsed   = elapsedMs / 1000.0;
        double overall   = metrics.overallThroughput();
        double meanMs    = metrics.getLatencyMeanNs()    / 1_000_000.0;
        double minMs     = metrics.getLatencyMinNs()     / 1_000_000.0;
        double maxMs     = metrics.getLatencyMaxNs()     / 1_000_000.0;
        double p50Ms     = metrics.getPercentileNs(0.50) / 1_000_000.0;
        double p75Ms     = metrics.getPercentileNs(0.75) / 1_000_000.0;
        double p90Ms     = metrics.getPercentileNs(0.90) / 1_000_000.0;
        double p95Ms     = metrics.getPercentileNs(0.95) / 1_000_000.0;
        double p99Ms     = metrics.getPercentileNs(0.99) / 1_000_000.0;
        double p999Ms    = metrics.getPercentileNs(0.999)/ 1_000_000.0;
        double totalMb   = metrics.getBytesCount()       / (1024.0 * 1024.0);
        double throughMb = totalMb / Math.max(elapsed, 0.001);

        System.out.printf("  %-30s : %d%n", "Messages sent", sent);
        System.out.printf("  %-30s : %d%n", "Errors", errors);
        System.out.printf("  %-30s : %.2f s%n", "Elapsed time", elapsed);
        System.out.printf("  %-30s : %.1f msg/s%n", "Overall throughput", overall);
        System.out.printf("  %-30s : %.2f MB%n", "Total data sent", totalMb);
        System.out.printf("  %-30s : %.2f MB/s%n", "Data throughput", throughMb);
        System.out.println();
        System.out.println("  Latency distribution:");
        System.out.printf("  %-30s : %.3f ms%n", "Min", minMs);
        System.out.printf("  %-30s : %.3f ms%n", "Mean", meanMs);
        System.out.printf("  %-30s : %.3f ms%n", "p50 (median)", p50Ms);
        System.out.printf("  %-30s : %.3f ms%n", "p75", p75Ms);
        System.out.printf("  %-30s : %.3f ms%n", "p90", p90Ms);
        System.out.printf("  %-30s : %.3f ms%n", "p95", p95Ms);
        System.out.printf("  %-30s : %.3f ms%n", "p99", p99Ms);
        System.out.printf("  %-30s : %.3f ms%n", "p99.9", p999Ms);
        System.out.printf("  %-30s : %.3f ms%n", "Max", maxMs);
        System.out.println();

        if (errors > 0) {
            System.out.println(ANSI_RED + "  ⚠ Test completed with " + errors + " error(s)" + ANSI_RESET);
        } else {
            System.out.println(ANSI_GREEN + "  ✔ Test completed successfully" + ANSI_RESET);
        }
        System.out.println("═".repeat(96));
    }

    // ─── CSV export ───────────────────────────────────────────────────────────

    public void writeCsvReport(File outputFile, long elapsedMs) {
        try (PrintWriter pw = new PrintWriter(new FileWriter(outputFile))) {
            pw.println("metric,value");
            pw.printf("messages_sent,%d%n", metrics.getMessageCount());
            pw.printf("errors,%d%n", metrics.getErrorCount());
            pw.printf("elapsed_seconds,%.3f%n", elapsedMs / 1000.0);
            pw.printf("throughput_msg_per_sec,%.2f%n", metrics.overallThroughput());
            pw.printf("total_bytes_mb,%.3f%n", metrics.getBytesCount() / (1024.0 * 1024.0));
            pw.printf("latency_min_ms,%.3f%n", metrics.getLatencyMinNs() / 1_000_000.0);
            pw.printf("latency_mean_ms,%.3f%n", metrics.getLatencyMeanNs() / 1_000_000.0);
            pw.printf("latency_p50_ms,%.3f%n", metrics.getPercentileNs(0.50) / 1_000_000.0);
            pw.printf("latency_p75_ms,%.3f%n", metrics.getPercentileNs(0.75) / 1_000_000.0);
            pw.printf("latency_p90_ms,%.3f%n", metrics.getPercentileNs(0.90) / 1_000_000.0);
            pw.printf("latency_p95_ms,%.3f%n", metrics.getPercentileNs(0.95) / 1_000_000.0);
            pw.printf("latency_p99_ms,%.3f%n", metrics.getPercentileNs(0.99) / 1_000_000.0);
            pw.printf("latency_p999_ms,%.3f%n", metrics.getPercentileNs(0.999) / 1_000_000.0);
            pw.printf("latency_max_ms,%.3f%n", metrics.getLatencyMaxNs() / 1_000_000.0);
        } catch (IOException e) {
            log.error("Failed to write CSV report: {}", e.getMessage(), e);
        }
    }
}
