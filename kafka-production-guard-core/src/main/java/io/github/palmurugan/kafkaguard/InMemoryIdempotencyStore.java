package io.github.palmurugan.kafkaguard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single-JVM store. Fine for tests and local dev; NOT safe across multiple instances.
 */
public class InMemoryIdempotencyStore implements IdempotencyStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryIdempotencyStore.class);

    private record Entry(boolean completed, long expiresAtNanos) {
    }

    private static final int PURGE_EVERY = 1024;

    private final Map<String, Entry> entries = new ConcurrentHashMap<>();
    private final AtomicLong calls = new AtomicLong();

    public InMemoryIdempotencyStore() {
        log.warn("kafka-production-guard: using IN-MEMORY idempotency store. This is NOT safe with multiple instances. " +
                "Add kafka-production-guard-jdbc or provide your own IdempotencyStore bean for production use.");
    }

    @Override
    public ClaimResult claim(String key, Duration inProgressTtl) {
        maybePurge();
        ClaimResult[] result = new ClaimResult[1];
        entries.compute(key, (k, existing) -> {
            long now = System.nanoTime();
            if (existing == null || existing.expiresAtNanos() - now <= 0) {
                result[0] = ClaimResult.ACQUIRED;
                return new Entry(false, now + inProgressTtl.toNanos());
            }
            result[0] = existing.completed() ? ClaimResult.DUPLICATE : ClaimResult.IN_PROGRESS;
            return existing;
        });
        if (log.isDebugEnabled()) {
            log.debug("Claim result for key '{}': {}", key, result[0]);
        }
        return result[0];
    }

    @Override
    public void complete(String key, Duration retention) {
        entries.put(key, new Entry(true, System.nanoTime() + retention.toNanos()));
        if (log.isDebugEnabled()) {
            log.debug("Marked key '{}' as completed with retention {}", key, retention);
        }
    }

    @Override
    public void release(String key) {
        entries.computeIfPresent(key, (k, e) -> e.completed() ? e : null);
        if (log.isDebugEnabled()) {
            log.debug("Released claim for key '{}'", key);
        }
    }

    private void maybePurge() {
        if (calls.incrementAndGet() % PURGE_EVERY == 0) {
            long now = System.nanoTime();
            int removed = 0;
            for (var it = entries.entrySet().iterator(); it.hasNext(); ) {
                if (it.next().getValue().expiresAtNanos() - now <= 0) {
                    it.remove();
                    removed++;
                }
            }
            if (log.isTraceEnabled() && removed > 0) {
                log.trace("Purged {} expired entries from in-memory store", removed);
            }
        }
    }
}
