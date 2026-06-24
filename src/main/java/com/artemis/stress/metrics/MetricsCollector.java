package com.artemis.stress.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe metrics collector for producer (send latency) and
 * consumer (end-to-end latency) sides.
 */
public class MetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(MetricsCollector.class);
    private static final int BUCKETS = 32;

    // Warmup
    private final int           warmupTotal;
    private final LongAdder     warmupCounter = new LongAdder();
    private final AtomicBoolean warmupDone    = new AtomicBoolean(false);

    // Producer counters
    private final LongAdder  sendCount  = new LongAdder();
    private final LongAdder  sendBytes  = new LongAdder();
    private final LongAdder  sendErrors = new LongAdder();
    private final LongAdder  sendLatSum = new LongAdder();
    private final AtomicLong sendLatMin = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong sendLatMax = new AtomicLong(0);
    private final LongAdder[] sendHisto = initBuckets();

    // Consumer counters
    private final LongAdder  e2eCount   = new LongAdder();
    private final LongAdder  e2eErrors  = new LongAdder();
    private final LongAdder  e2eLatSum  = new LongAdder();
    private final AtomicLong e2eLatMin  = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong e2eLatMax  = new AtomicLong(0);
    private final LongAdder[] e2eHisto  = initBuckets();

    // Throughput snapshots — shared timestamp for both rates
    private volatile long snapTs      = System.currentTimeMillis();
    private volatile long snapSend    = 0;
    private volatile long snapE2e     = 0;

    private final long startNano = System.nanoTime();

    public MetricsCollector(int warmupMessages) {
        this.warmupTotal = warmupMessages;
    }

    // ─── Recording ────────────────────────────────────────────────────────────

    public void recordSend(long latencyNs, int bytes) {
        if (!isWarmupDone()) { tickWarmup(); return; }
        sendCount.increment();
        sendBytes.add(bytes);
        sendLatSum.add(latencyNs);
        updateMin(sendLatMin, latencyNs);
        updateMax(sendLatMax, latencyNs);
        sendHisto[bucket(latencyNs)].increment();
    }

    public void recordSendError() { sendErrors.increment(); }

    public void recordE2e(long latencyNs) {
        e2eCount.increment();
        e2eLatSum.add(latencyNs);
        updateMin(e2eLatMin, latencyNs);
        updateMax(e2eLatMax, latencyNs);
        e2eHisto[bucket(latencyNs)].increment();
    }

    public void recordE2eError() { e2eErrors.increment(); }

    // ─── Accessors ────────────────────────────────────────────────────────────

    public long    getSendCount()    { return sendCount.sum(); }
    public long    getSendBytes()    { return sendBytes.sum(); }
    public long    getSendErrors()   { return sendErrors.sum(); }
    public long    getE2eCount()     { return e2eCount.sum(); }
    public long    getE2eErrors()    { return e2eErrors.sum(); }
    public boolean hasErrors()       { return sendErrors.sum() > 0 || e2eErrors.sum() > 0; }
    public boolean isWarmupDone()    { return warmupDone.get(); }
    public long    elapsedMs()       { return (System.nanoTime() - startNano) / 1_000_000; }

    public long getSendLatMinNs()  { long v = sendLatMin.get(); return v == Long.MAX_VALUE ? 0 : v; }
    public long getSendLatMaxNs()  { return sendLatMax.get(); }
    public long getSendLatMeanNs() { long c = sendCount.sum(); return c == 0 ? 0 : sendLatSum.sum() / c; }
    public long getSendPercentileNs(double p) {
        return percentile(sendHisto, sendCount.sum(), p, getSendLatMaxNs());
    }

    public long getE2eLatMinNs()   { long v = e2eLatMin.get(); return v == Long.MAX_VALUE ? 0 : v; }
    public long getE2eLatMaxNs()   { return e2eLatMax.get(); }
    public long getE2eLatMeanNs()  { long c = e2eCount.sum(); return c == 0 ? 0 : e2eLatSum.sum() / c; }
    public long getE2ePercentileNs(double p) {
        return percentile(e2eHisto, e2eCount.sum(), p, getE2eLatMaxNs());
    }

    /** Send throughput msg/s since last call. */
    public double currentSendRate() {
        long now   = System.currentTimeMillis();
        long count = sendCount.sum();
        long dt    = now - snapTs;
        long delta = count - snapSend;
        snapTs   = now;
        snapSend = count;
        return dt == 0 ? 0.0 : (double) delta / dt * 1000.0;
    }

    /** Consume throughput msg/s since last call. */
    public double currentE2eRate() {
        long now   = System.currentTimeMillis();
        long count = e2eCount.sum();
        long dt    = now - snapTs;       // same window as send rate
        long delta = count - snapE2e;
        snapE2e = count;
        return dt == 0 ? 0.0 : (double) delta / dt * 1000.0;
    }

    public double overallSendRate() {
        long ms = elapsedMs(); return ms == 0 ? 0.0 : (double) sendCount.sum() / ms * 1000.0;
    }
    public double overallE2eRate() {
        long ms = elapsedMs(); return ms == 0 ? 0.0 : (double) e2eCount.sum() / ms * 1000.0;
    }

    // ─── Internals ────────────────────────────────────────────────────────────

    private void tickWarmup() {
        warmupCounter.increment();
        if (warmupCounter.sum() >= warmupTotal && warmupDone.compareAndSet(false, true)) {
            log.info("Warmup complete ({} messages) — recording started", warmupTotal);
            snapTs = System.currentTimeMillis();
        }
    }

    private long percentile(LongAdder[] histo, long total, double p, long max) {
        if (total == 0) return 0;
        long target = (long) Math.ceil(total * p), cum = 0;
        for (int i = 0; i < BUCKETS; i++) {
            cum += histo[i].sum();
            if (cum >= target) return ((i == 0 ? 0L : 1L << i) + (1L << (i + 1)) - 1) / 2;
        }
        return max;
    }

    private int bucket(long ns) {
        if (ns <= 0) return 0;
        return Math.min(63 - Long.numberOfLeadingZeros(ns), BUCKETS - 1);
    }

    private void updateMin(AtomicLong ref, long val) {
        long cur; do { cur = ref.get(); } while (val < cur && !ref.compareAndSet(cur, val));
    }
    private void updateMax(AtomicLong ref, long val) {
        long cur; do { cur = ref.get(); } while (val > cur && !ref.compareAndSet(cur, val));
    }
    private static LongAdder[] initBuckets() {
        LongAdder[] b = new LongAdder[BUCKETS];
        for (int i = 0; i < BUCKETS; i++) b[i] = new LongAdder();
        return b;
    }

    // Legacy aliases so unchanged producer code compiles
    public void   recordSuccess(long ns, int bytes) { recordSend(ns, bytes); }
    public void   recordError()                     { recordSendError(); }
    public long   getMessageCount()                 { return getSendCount(); }
    public long   getErrorCount()                   { return getSendErrors(); }
    public long   getBytesCount()                   { return getSendBytes(); }
    public long   getLatencyMinNs()                 { return getSendLatMinNs(); }
    public long   getLatencyMaxNs()                 { return getSendLatMaxNs(); }
    public long   getLatencyMeanNs()                { return getSendLatMeanNs(); }
    public long   getPercentileNs(double p)         { return getSendPercentileNs(p); }
    public double currentThroughput()               { return currentSendRate(); }
    public double overallThroughput()               { return overallSendRate(); }
}
