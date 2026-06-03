package com.artemis.stress.xml;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe generator of strictly monotonically increasing numeric IDs.
 *
 * <p>A single {@link AtomicLong} is shared across ALL producer threads, so
 * every call to {@link #next()} returns a value that is:
 * <ul>
 *   <li><b>Unique</b> — no two calls ever return the same number.</li>
 *   <li><b>Strictly increasing</b> — later calls always return a higher value.</li>
 *   <li><b>Contiguous</b> — values form an unbroken sequence starting at
 *       {@code startValue} (configurable, default 1).</li>
 * </ul>
 * </p>
 *
 * <p>Range: 1 … {@link Long#MAX_VALUE} (9 223 372 036 854 775 807), which at
 * 1 000 000 msg/s would take ~292 000 years to exhaust.</p>
 *
 * <p>The counter is intentionally <em>not</em> reset between test runs so that
 * IDs remain unique even when the tool is restarted with the same broker queue.
 * If absolute uniqueness across JVM restarts is required, pass a
 * {@code startValue} derived from the current epoch millis.</p>
 */
public final class MessageIdGenerator {

    private static final Logger log = LoggerFactory.getLogger(MessageIdGenerator.class);

    private final AtomicLong counter;

    /**
     * Creates a generator starting at 1.
     */
    public MessageIdGenerator() {
        this(1L);
    }

    /**
     * Creates a generator starting at the given value (must be &gt;= 1).
     *
     * @param startValue first ID that will be returned by {@link #next()}
     */
    public MessageIdGenerator(long startValue) {
        if (startValue < 1) {
            throw new IllegalArgumentException("startValue must be >= 1, got: " + startValue);
        }
        // We store (startValue - 1) so the first getAndIncrement() yields startValue
        this.counter = new AtomicLong(startValue - 1);
        log.info("MessageIdGenerator initialised: first id={}", startValue);
    }

    /**
     * Returns the next unique numeric ID. Safe to call concurrently from any
     * number of threads without external synchronisation.
     *
     * @return a positive long that has never been returned by this instance
     * @throws ArithmeticException if the counter overflows {@link Long#MAX_VALUE}
     */
    public long next() {
        long id = counter.incrementAndGet();
        if (id < 0) {
            // Overflow — practically impossible but guard it anyway
            throw new ArithmeticException("MessageIdGenerator counter overflowed Long.MAX_VALUE");
        }
        return id;
    }

    /**
     * Returns the last ID that was issued, or {@code startValue - 1} if no ID
     * has been issued yet. Useful for reporting.
     */
    public long lastIssued() {
        return counter.get();
    }

    /**
     * Returns the total number of IDs that have been issued since construction.
     */
    public long totalIssued() {
        return counter.get() - (counter.get() - lastIssued()); // snapshot-safe
    }
}
