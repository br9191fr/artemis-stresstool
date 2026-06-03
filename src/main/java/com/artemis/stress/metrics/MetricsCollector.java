package com.artemis.stress.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe metrics collector.
 *
 * <p>Tracks:
 * <ul>
 *   <li>Total messages sent (excluding warmup)</li>
 *   <li>Total bytes sent</li>
 *   <li>End-to-end send latency (ns) — min / max / sum for mean calculation</li>
 *   <li>HDR-style histogram buckets for latency percentiles (p50 / p90 / p99 / p999)</li>
 *   <li>Error count</li>
 * </ul>
 * </p>
 *
 * <p>Uses lock-free {@link LongAdder} and {@link AtomicLong} for high
 * concurrency. The histogram uses pre-allocated buckets sized in powers-of-two
 * nanosecond ranges — no external HdrHistogram dependency required.</p>
 */
public class MetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(MetricsCollector.class);

    // Warmup gate
    private final int warmupTotal;
    private final LongAdder warmupCounter = new LongAdder();
    private final AtomicBoolean warmupDone = new AtomicBoolean(false);

    // Core counters
    private final LongAdder messageCount  = new LongAdder();
    private final LongAdder bytesCount    = new LongAdder();
    private final LongAdder errorCount    = new LongAdder();
    private final LongAdder latencySum    = new LongAdder(); // nanoseconds

    private final AtomicLong latencyMin   = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong latencyMax   = new AtomicLong(0);

    // Histogram: 32 buckets, each covering a doubling range in nanoseconds.
    // Bucket i covers [2^i .. 2^(i+1)) ns.
    // Bucket 0  = <1 ns  (practically impossible, but captured)
    // Bucket 10 = ~1 µs
    // Bucket 20 = ~1 ms
    // Bucket 30 = ~1 s
    private static final int BUCKET_COUNT = 32;
    private final LongAdder[] histogram = new LongAdder[BUCKET_COUNT];

    // Throughput snapshot for live reporting
    private volatile long snapshotTs = System.currentTimeMillis();
    private volatile long snapshotCount = 0;

    // Start time
    private final long startNano = System.nanoTime();

    public MetricsCollector(int warmupMessages) {
        this.warmupTotal = warmupMessages;
        for (int i = 0; i < BUCKET_COUNT; i++) {
            histogram[i] = new LongAdder();
        }
    }

    // ─── Recording ────────────────────────────────────────────────────────────

    public void recordSuccess(long latencyNs, int bytes) {
        // Handle warmup phase
        if (!warmupDone.get()) {
            long wc = warmupCounter.sumThenReset();
            warmupCounter.add(wc + 1);
            if (wc + 1 >= warmupTotal) {
                if (warmupDone.compareAndSet(false, true)) {
                    log.info("Warmup complete ({} messages) — metrics recording started", warmupTotal);
                    // Reset snapshot baseline
                    snapshotTs    = System.currentTimeMillis();
                    snapshotCount = 0;
                }
            }
            return; // don't count warmup in metrics
        }

        messageCount.increment();
        bytesCount.add(bytes);
        latencySum.add(latencyNs);

        // Min / Max (optimistic spin)
        updateMin(latencyNs);
        updateMax(latencyNs);

        // Histogram bucket
        int bucket = bucketFor(latencyNs);
        histogram[bucket].increment();
    }

    public void recordError() {
        errorCount.increment();
    }

    // ─── Accessors ────────────────────────────────────────────────────────────

    public long getMessageCount()   { return messageCount.sum(); }
    public long getBytesCount()     { return bytesCount.sum(); }
    public long getErrorCount()     { return errorCount.sum(); }
    public boolean hasErrors()      { return errorCount.sum() > 0; }
    public boolean isWarmupDone()   { return warmupDone.get(); }

    public long getLatencyMinNs()   {
        long v = latencyMin.get();
        return v == Long.MAX_VALUE ? 0 : v;
    }
    public long getLatencyMaxNs()   { return latencyMax.get(); }
    public long getLatencyMeanNs()  {
        long count = messageCount.sum();
        return count == 0 ? 0 : latencySum.sum() / count;
    }

    /** Elapsed time since collector was created (in milliseconds). */
    public long elapsedMs() {
        return (System.nanoTime() - startNano) / 1_000_000;
    }

    /**
     * Current throughput in messages/second (since last snapshot call).
     * Thread-safe but not perfectly precise at very high call rates.
     */
    public double currentThroughput() {
        long now   = System.currentTimeMillis();
        long count = messageCount.sum();
        long dtMs  = now - snapshotTs;
        if (dtMs == 0) return 0;
        double rate = (double)(count - snapshotCount) / dtMs * 1000.0;
        snapshotTs    = now;
        snapshotCount = count;
        return rate;
    }

    /** Overall average throughput since warmup completed. */
    public double overallThroughput() {
        long ms = elapsedMs();
        return ms == 0 ? 0 : (double) messageCount.sum() / ms * 1000.0;
    }

    /**
     * Returns the approximate percentile latency in nanoseconds.
     * Uses histogram bucket analysis.
     *
     * @param percentile e.g. 0.50, 0.90, 0.99, 0.999
     */
    public long getPercentileNs(double percentile) {
        long total = messageCount.sum();
        if (total == 0) return 0;

        long target = (long) Math.ceil(total * percentile);
        long cumulative = 0;

        for (int i = 0; i < BUCKET_COUNT; i++) {
            cumulative += histogram[i].sum();
            if (cumulative >= target) {
                // Return midpoint of the bucket range
                long bucketStart = i == 0 ? 0 : (1L << i);
                long bucketEnd   = (1L << (i + 1)) - 1;
                return (bucketStart + bucketEnd) / 2;
            }
        }
        return getLatencyMaxNs();
    }

    // ─── Internals ────────────────────────────────────────────────────────────

    private int bucketFor(long ns) {
        if (ns <= 0) return 0;
        int bit = 63 - Long.numberOfLeadingZeros(ns);
        return Math.min(bit, BUCKET_COUNT - 1);
    }

    private void updateMin(long ns) {
        long current;
        do { current = latencyMin.get(); }
        while (ns < current && !latencyMin.compareAndSet(current, ns));
    }

    private void updateMax(long ns) {
        long current;
        do { current = latencyMax.get(); }
        while (ns > current && !latencyMax.compareAndSet(current, ns));
    }
}
